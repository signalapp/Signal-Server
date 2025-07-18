/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.auth.Auth;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.headers.Header;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.EntityTag;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.util.Arrays;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.entities.RemoteConfigurationResponse;
import org.whispersystems.textsecuregcm.storage.RemoteConfig;
import org.whispersystems.textsecuregcm.storage.RemoteConfigsManager;
import org.whispersystems.textsecuregcm.util.Conversions;
import org.whispersystems.textsecuregcm.util.Util;
import org.whispersystems.textsecuregcm.util.ua.ClientPlatform;
import org.whispersystems.textsecuregcm.util.ua.UnrecognizedUserAgentException;
import org.whispersystems.textsecuregcm.util.ua.UserAgentUtil;

@Path("/v2/config")
@Tag(name = "Remote Config")
public class RemoteConfigController {

  private final RemoteConfigsManager remoteConfigsManager;
  private final Map<String, String> globalConfig;

  private static final String GLOBAL_CONFIG_PREFIX = "global.";
  private static final Set<String> PLATFORM_PREFIXES = Arrays.stream(ClientPlatform.values())
    .map(p -> p.name().toLowerCase())
    .collect(Collectors.toSet());

  public RemoteConfigController(RemoteConfigsManager remoteConfigsManager,
      Map<String, String> globalConfig,
      final Clock clock) {
    this.remoteConfigsManager = remoteConfigsManager;
    this.globalConfig = globalConfig;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Fetch remote configuration",
      description = "Remote configuration is a list of namespaced keys that clients may use for consistent configuration or behavior. Configuration values change over time, and the list should be refreshed periodically, typically at client launch and every few hours thereafter. Some values depend on the authenticated user, so the list should be refreshed immediately if the user changes."
  )
  @ApiResponse(
      responseCode = "200",
      description = "Remote configuration values for the authenticated user",
      content = @Content(schema = @Schema(implementation = RemoteConfigurationResponse.class)),
      headers = @Header(name = "ETag", description = "A hash of the configuration content which can be supplied in an If-None-Match header on future requests"))
  @ApiResponse(responseCode = "304", description = "There is no change since the last fetch", content = {})
  @ApiResponse(responseCode = "401", description = "This request requires authentication", content = {})

  public Response getAll(
      @Auth AuthenticatedDevice auth,

      @Parameter(description = "The ETag header supplied with a previous response from this endpoint. Optional.")
      @HeaderParam(HttpHeaders.IF_NONE_MATCH)
      @Nullable EntityTag eTag,

      @Parameter(description = "The user agent in standard form.")
      @HeaderParam(HttpHeaders.USER_AGENT)
      String userAgent
  ) {
    final String platformPrefix = platformPrefix(userAgent);
    final List<RemoteConfig> remoteConfigs = remoteConfigsManager.getAll();

    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");

      final Map<String, String> configs = Stream.concat(
          remoteConfigs.stream()
              .filter(config -> {
                  final String firstNameComponent = config.getName().split("\\.", 2)[0];
                  return firstNameComponent.equals(platformPrefix) || !PLATFORM_PREFIXES.contains(firstNameComponent);
              })
              .map(
                  config -> {
                          final byte[] hashKey = config.getHashKey() != null
                              ? config.getHashKey().getBytes(StandardCharsets.UTF_8)
                              : config.getName().getBytes(StandardCharsets.UTF_8);
                          boolean inBucket = isInBucket(digest, auth.accountIdentifier(), hashKey, config.getPercentage(), config.getUuids());
                          final String value = inBucket ? config.getValue() : config.getDefaultValue();
                          return Pair.of(config.getName(), value == null ? String.valueOf(inBucket) : value);
                      }),
                  globalConfig.entrySet().stream()
                    .map(e -> Pair.of(GLOBAL_CONFIG_PREFIX + e.getKey(), e.getValue())))
        .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));

      final EntityTag newETag = new EntityTag(HexFormat.of().toHexDigits(configs.hashCode()));
      if (newETag.equals(eTag)) {
        return Response.notModified(eTag).build();
      }

      return Response.ok(new RemoteConfigurationResponse(configs))
          .tag(newETag)
          .build();
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    }
  }

  private static String platformPrefix(final String userAgent) {
    try {
      return UserAgentUtil.parseUserAgentString(userAgent).platform().name().toLowerCase();
    } catch (UnrecognizedUserAgentException e) {
      return null;
    }
  }

  @VisibleForTesting
  public static boolean isInBucket(MessageDigest digest, UUID uid, byte[] hashKey, int configPercentage,
      Set<UUID> uuidsInBucket) {
    if (uuidsInBucket.contains(uid)) {
      return true;
    }

    ByteBuffer bb = ByteBuffer.allocate(16);
    bb.putLong(uid.getMostSignificantBits());
    bb.putLong(uid.getLeastSignificantBits());

    digest.update(bb.array());

    byte[] hash = digest.digest(hashKey);
    int bucket = (int) (Util.ensureNonNegativeLong(Conversions.byteArrayToLong(hash)) % 100);

    return bucket < configPercentage;
  }

}
