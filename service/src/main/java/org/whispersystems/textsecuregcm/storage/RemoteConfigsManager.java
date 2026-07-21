/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.whispersystems.textsecuregcm.util.Conversions;
import org.whispersystems.textsecuregcm.util.Util;
import org.whispersystems.textsecuregcm.util.ua.ClientPlatform;
import org.whispersystems.textsecuregcm.util.ua.UnrecognizedUserAgentException;
import org.whispersystems.textsecuregcm.util.ua.UserAgentUtil;

public class RemoteConfigsManager {

  private final Supplier<List<RemoteConfig>> remoteConfigSupplier;
  private final Map<String, String> globalConfig;

  private static final String GLOBAL_CONFIG_PREFIX = "global.";
  private static final Set<String> PLATFORM_PREFIXES = Arrays.stream(ClientPlatform.values())
      .map(p -> p.name().toLowerCase())
      .collect(Collectors.toSet());

  public RemoteConfigsManager(final RemoteConfigs remoteConfigs, final Map<String, String> globalConfig) {
    this.remoteConfigSupplier =
        Suppliers.memoizeWithExpiration(remoteConfigs::getAll, 10, TimeUnit.SECONDS);
    this.globalConfig = globalConfig;
  }

  /**
   * Returns all remote configuration (per-platform and global) for the specified account and client.
   *
   * @param accountIdentifier the identifier of the authenticated account
   * @param userAgent         the requester's user agent, used to select the client platform
   * @return a map of namespaced configuration keys to their resolved values
   */
  public Map<String, String> getConfigForAccount(final UUID accountIdentifier, @Nullable final String userAgent) {
    final String platformPrefix = platformPrefix(userAgent);

    final MessageDigest digest;
    try {
      digest = MessageDigest.getInstance("SHA-256");
    } catch (final NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    }

    final List<RemoteConfig> perPlatformConfig = remoteConfigSupplier.get();

    return Stream.concat(
            perPlatformConfig.stream()
                .filter(config -> {
                  final String firstNameComponent = config.getName().split("\\.", 2)[0];
                  return firstNameComponent.equals(platformPrefix) || !PLATFORM_PREFIXES.contains(firstNameComponent);
                })
                .map(config -> {
                  final byte[] hashKey = config.getHashKey() != null
                      ? config.getHashKey().getBytes(StandardCharsets.UTF_8)
                      : config.getName().getBytes(StandardCharsets.UTF_8);
                  boolean inBucket = isInBucket(digest, accountIdentifier, hashKey, config.getPercentage(),
                      config.getUuids());
                  final String value = inBucket ? config.getValue() : config.getDefaultValue();
                  return Pair.of(config.getName(), value == null ? String.valueOf(inBucket) : value);
                }),

            globalConfig.entrySet().stream()
                .map(e -> Pair.of(GLOBAL_CONFIG_PREFIX + e.getKey(), e.getValue())))
        .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
  }

  @Nullable
  private static String platformPrefix(@Nullable final String userAgent) {
    try {
      return UserAgentUtil.parseUserAgentString(userAgent).platform().name().toLowerCase();
    } catch (UnrecognizedUserAgentException e) {
      return null;
    }
  }

  @VisibleForTesting
  static boolean isInBucket(MessageDigest digest, UUID uid, byte[] hashKey, int configPercentage,
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
