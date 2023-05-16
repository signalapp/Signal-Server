/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import com.codahale.metrics.annotation.Timed;
import com.google.api.client.googleapis.auth.oauth2.GoogleIdToken;
import com.google.api.client.googleapis.auth.oauth2.GoogleIdTokenVerifier;
import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.auth.Auth;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.signal.event.AdminEventLogger;
import org.signal.event.RemoteConfigDeleteEvent;
import org.signal.event.RemoteConfigSetEvent;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.entities.UserRemoteConfig;
import org.whispersystems.textsecuregcm.entities.UserRemoteConfigList;
import org.whispersystems.textsecuregcm.storage.RemoteConfig;
import org.whispersystems.textsecuregcm.storage.RemoteConfigsManager;
import org.whispersystems.textsecuregcm.util.Conversions;
import org.whispersystems.textsecuregcm.util.Util;

@Path("/v1/config")
@Tag(name = "Remote Config")
public class RemoteConfigController {

  private final RemoteConfigsManager remoteConfigsManager;
  private final AdminEventLogger adminEventLogger;
  private final List<String> configAuthTokens;
  private final Set<String> configAuthUsers;
  private final Map<String, String> globalConfig;

  private final String requiredHostedDomain;

  private final GoogleIdTokenVerifier googleIdTokenVerifier;

  private static final String GLOBAL_CONFIG_PREFIX = "global.";

  public RemoteConfigController(RemoteConfigsManager remoteConfigsManager, AdminEventLogger adminEventLogger,
      List<String> configAuthTokens, Set<String> configAuthUsers, String requiredHostedDomain, List<String> audience,
      final GoogleIdTokenVerifier.Builder googleIdTokenVerifierBuilder, Map<String, String> globalConfig) {
    this.remoteConfigsManager = remoteConfigsManager;
    this.adminEventLogger = Objects.requireNonNull(adminEventLogger);
    this.configAuthTokens = configAuthTokens;
    this.configAuthUsers = configAuthUsers;
    this.globalConfig = globalConfig;

    this.requiredHostedDomain = requiredHostedDomain;
    this.googleIdTokenVerifier = googleIdTokenVerifierBuilder.setAudience(audience).build();
  }

  @Timed
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public UserRemoteConfigList getAll(@Auth AuthenticatedAccount auth) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA1");

      final Stream<UserRemoteConfig> globalConfigStream = globalConfig.entrySet().stream()
          .map(entry -> new UserRemoteConfig(GLOBAL_CONFIG_PREFIX + entry.getKey(), true, entry.getValue()));
      return new UserRemoteConfigList(Stream.concat(remoteConfigsManager.getAll().stream().map(config -> {
        final byte[] hashKey = config.getHashKey() != null ? config.getHashKey().getBytes(StandardCharsets.UTF_8)
            : config.getName().getBytes(StandardCharsets.UTF_8);
        boolean inBucket = isInBucket(digest, auth.getAccount().getUuid(), hashKey, config.getPercentage(),
            config.getUuids());
        return new UserRemoteConfig(config.getName(), inBucket,
            inBucket ? config.getValue() : config.getDefaultValue());
      }), globalConfigStream).collect(Collectors.toList()));
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    }
  }

  @Timed
  @PUT
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public void set(@HeaderParam("Config-Token") String configToken, @NotNull @Valid RemoteConfig config) {

    final String authIdentity = getAuthIdentity(configToken)
        .orElseThrow(() -> new WebApplicationException(Response.Status.UNAUTHORIZED));

    if (config.getName().startsWith(GLOBAL_CONFIG_PREFIX)) {
      throw new WebApplicationException(Response.Status.FORBIDDEN);
    }

    adminEventLogger.logEvent(
        new RemoteConfigSetEvent(
            authIdentity,
            config.getName(),
            config.getPercentage(),
            config.getDefaultValue(),
            config.getValue(),
            config.getHashKey(),
            config.getUuids().stream().map(UUID::toString).collect(Collectors.toList())));
    remoteConfigsManager.set(config);
  }

  @Timed
  @DELETE
  @Path("/{name}")
  public void delete(@HeaderParam("Config-Token") String configToken, @PathParam("name") String name) {
    final String authIdentity = getAuthIdentity(configToken)
        .orElseThrow(() -> new WebApplicationException(Response.Status.UNAUTHORIZED));

    if (name.startsWith(GLOBAL_CONFIG_PREFIX)) {
      throw new WebApplicationException(Response.Status.FORBIDDEN);
    }

    adminEventLogger.logEvent(new RemoteConfigDeleteEvent(authIdentity, name));
    remoteConfigsManager.delete(name);
  }

  private Optional<String> getAuthIdentity(String token) {
    return getAuthorizedGoogleIdentity(token)
        .map(googleIdToken -> googleIdToken.getPayload().getEmail())
        .or(() -> Optional.ofNullable(isAuthorized(token) ? token : null));
  }

  private Optional<GoogleIdToken> getAuthorizedGoogleIdentity(String token) {
    try {
      final @Nullable GoogleIdToken googleIdToken = googleIdTokenVerifier.verify(token);

      if (googleIdToken != null
          && googleIdToken.getPayload().getHostedDomain().equals(requiredHostedDomain)
          && googleIdToken.getPayload().getEmailVerified()
          && configAuthUsers.contains(googleIdToken.getPayload().getEmail())) {

        return Optional.of(googleIdToken);
      }

      return Optional.empty();

    } catch (final Exception ignored) {
      return Optional.empty();
    }
  }

  @VisibleForTesting
  public static boolean isInBucket(MessageDigest digest, UUID uid, byte[] hashKey, int configPercentage,
      Set<UUID> uuidsInBucket) {
    if (uuidsInBucket.contains(uid)) {
      return true;
    }

    ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
    bb.putLong(uid.getMostSignificantBits());
    bb.putLong(uid.getLeastSignificantBits());

    digest.update(bb.array());

    byte[] hash = digest.digest(hashKey);
    int bucket = (int) (Util.ensureNonNegativeLong(Conversions.byteArrayToLong(hash)) % 100);

    return bucket < configPercentage;
  }

  @SuppressWarnings("BooleanMethodIsAlwaysInverted")
  private boolean isAuthorized(String configToken) {
    return configToken != null && configAuthTokens.stream().anyMatch(authorized -> MessageDigest.isEqual(authorized.getBytes(), configToken.getBytes()));
  }
}
