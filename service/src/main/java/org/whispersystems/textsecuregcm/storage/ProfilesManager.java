/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.util.SystemMapper;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

public class ProfilesManager {

  private final Logger logger = LoggerFactory.getLogger(PendingAccountsManager.class);

  private static final String CACHE_PREFIX = "profiles::";

  private final Profiles                  profiles;
  private final FaultTolerantRedisCluster cacheCluster;
  private final ObjectMapper              mapper;

  public ProfilesManager(Profiles profiles, FaultTolerantRedisCluster cacheCluster) {
    this.profiles               = profiles;
    this.cacheCluster           = cacheCluster;
    this.mapper                 = SystemMapper.getMapper();
  }

  public void set(UUID uuid, VersionedProfile versionedProfile) {
    memcacheSet(uuid, versionedProfile);
    profiles.set(uuid, versionedProfile);
  }

  public void deleteAll(UUID uuid) {
    memcacheDelete(uuid);
    profiles.deleteAll(uuid);
  }

  public Optional<VersionedProfile> get(UUID uuid, String version) {
    Optional<VersionedProfile> profile = memcacheGet(uuid, version);

    if (!profile.isPresent()) {
      profile = profiles.get(uuid, version);
      profile.ifPresent(versionedProfile -> memcacheSet(uuid, versionedProfile));
    }

    return profile;
  }

  private void memcacheSet(UUID uuid, VersionedProfile profile) {
    try {
      final String profileJson = mapper.writeValueAsString(profile);

      cacheCluster.useCluster(connection -> connection.sync().hset(CACHE_PREFIX + uuid.toString(), profile.getVersion(), profileJson));
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private Optional<VersionedProfile> memcacheGet(UUID uuid, String version) {
    try {
      final String json = cacheCluster.withCluster(connection -> connection.sync().hget(CACHE_PREFIX + uuid.toString(), version));

      if (json == null) return Optional.empty();
      else              return Optional.of(mapper.readValue(json, VersionedProfile.class));
    } catch (IOException e) {
      logger.warn("Error deserializing value...", e);
      return Optional.empty();
    } catch (RedisException e) {
      logger.warn("Redis exception", e);
      return Optional.empty();
    }
  }

  private void memcacheDelete(UUID uuid) {
    cacheCluster.useCluster(connection -> connection.sync().del(CACHE_PREFIX + uuid.toString()));
  }
}
