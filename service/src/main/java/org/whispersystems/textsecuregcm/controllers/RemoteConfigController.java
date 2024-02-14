/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.auth.Auth;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.entities.UserRemoteConfig;
import org.whispersystems.textsecuregcm.entities.UserRemoteConfigList;
import org.whispersystems.textsecuregcm.storage.RemoteConfigsManager;
import org.whispersystems.textsecuregcm.util.Conversions;
import org.whispersystems.textsecuregcm.util.Util;
import org.whispersystems.websocket.auth.ReadOnly;

@Path("/v1/config")
@Tag(name = "Remote Config")
public class RemoteConfigController {

  private final RemoteConfigsManager remoteConfigsManager;
  private final Map<String, String> globalConfig;

  private final Clock clock;

  private static final String GLOBAL_CONFIG_PREFIX = "global.";

  public RemoteConfigController(RemoteConfigsManager remoteConfigsManager,
      Map<String, String> globalConfig,
      final Clock clock) {
    this.remoteConfigsManager = remoteConfigsManager;
    this.globalConfig = globalConfig;

    this.clock = clock;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public UserRemoteConfigList getAll(@ReadOnly @Auth AuthenticatedAccount auth) {
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
      }), globalConfigStream).collect(Collectors.toList()), clock.instant());
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError(e);
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

}
