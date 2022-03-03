/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import com.codahale.metrics.annotation.Timed;
import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.auth.Auth;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.entities.UserRemoteConfig;
import org.whispersystems.textsecuregcm.entities.UserRemoteConfigList;
import org.whispersystems.textsecuregcm.storage.RemoteConfig;
import org.whispersystems.textsecuregcm.storage.RemoteConfigsManager;
import org.whispersystems.textsecuregcm.util.Conversions;

@Path("/v1/config")
public class RemoteConfigController {

  private final RemoteConfigsManager remoteConfigsManager;
  private final List<String>         configAuthTokens;
  private final Map<String, String>  globalConfig;

  private static final String GLOBAL_CONFIG_PREFIX = "global.";

  public RemoteConfigController(RemoteConfigsManager remoteConfigsManager, List<String> configAuthTokens, Map<String, String> globalConfig) {
    this.remoteConfigsManager = remoteConfigsManager;
    this.configAuthTokens = configAuthTokens;
    this.globalConfig = globalConfig;
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
    if (!isAuthorized(configToken)) {
      throw new WebApplicationException(Response.Status.UNAUTHORIZED);
    }

    if (config.getName().startsWith(GLOBAL_CONFIG_PREFIX)) {
      throw new WebApplicationException(Response.Status.FORBIDDEN);
    }

    remoteConfigsManager.set(config);
  }

  @Timed
  @DELETE
  @Path("/{name}")
  public void delete(@HeaderParam("Config-Token") String configToken, @PathParam("name") String name) {
    if (!isAuthorized(configToken)) {
      throw new WebApplicationException(Response.Status.UNAUTHORIZED);
    }

    if (name.startsWith(GLOBAL_CONFIG_PREFIX)) {
      throw new WebApplicationException(Response.Status.FORBIDDEN);
    }

    remoteConfigsManager.delete(name);
  }

  @VisibleForTesting
  public static boolean isInBucket(MessageDigest digest, UUID uid, byte[] hashKey, int configPercentage, Set<UUID> uuidsInBucket) {
    if (uuidsInBucket.contains(uid)) return true;

    ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
    bb.putLong(uid.getMostSignificantBits());
    bb.putLong(uid.getLeastSignificantBits());

    digest.update(bb.array());

    byte[] hash   = digest.digest(hashKey);
    int    bucket = (int)(Math.abs(Conversions.byteArrayToLong(hash)) % 100);

    return bucket < configPercentage;
  }

  @SuppressWarnings("BooleanMethodIsAlwaysInverted")
  private boolean isAuthorized(String configToken) {
    return configToken != null && configAuthTokens.stream().anyMatch(authorized -> MessageDigest.isEqual(authorized.getBytes(), configToken.getBytes()));
  }
}
