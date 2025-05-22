/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.lettuce.core.RedisException;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import javax.annotation.Nullable;
import io.micrometer.core.instrument.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.Util;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;

public class ProfilesManager {

  private final Logger logger = LoggerFactory.getLogger(ProfilesManager.class);

  private static final String CACHE_PREFIX = "profiles::";

  private final Profiles profiles;
  private final FaultTolerantRedisClusterClient cacheCluster;
  private final S3AsyncClient s3Client;
  private final String bucket;
  private final ObjectMapper mapper;

  private static final String DELETE_AVATAR_COUNTER_NAME = name(ProfilesManager.class, "deleteAvatar");

  public ProfilesManager(final Profiles profiles, final FaultTolerantRedisClusterClient cacheCluster, final S3AsyncClient s3Client,
      final String bucket) {
    this.profiles = profiles;
    this.cacheCluster = cacheCluster;
    this.s3Client = s3Client;
    this.bucket = bucket;
    this.mapper = SystemMapper.jsonMapper();
  }

  public void set(UUID uuid, VersionedProfile versionedProfile) {
    redisSet(uuid, versionedProfile);
    profiles.set(uuid, versionedProfile);
  }

  public CompletableFuture<Void> setAsync(UUID uuid, VersionedProfile versionedProfile) {
    return profiles.setAsync(uuid, versionedProfile)
        .thenCompose(ignored -> redisSetAsync(uuid, versionedProfile));
  }

  public CompletableFuture<Void> deleteAll(UUID uuid) {

    final CompletableFuture<Void> profilesAndAvatars = Mono.fromFuture(profiles.deleteAll(uuid))
        .flatMapIterable(Function.identity())
        .flatMap(avatar ->
          Mono.fromFuture(deleteAvatar(avatar))
              // this is best-effort
              .retry(3)
              .onErrorComplete())
        .then().toFuture();

    return CompletableFuture.allOf(redisDelete(uuid), profilesAndAvatars);
  }

  public CompletableFuture<Void> deleteAvatar(String avatar) {
    return s3Client.deleteObject(DeleteObjectRequest.builder()
        .bucket(bucket)
        .key(avatar)
        .build())
        .handle((ignored, throwable) -> {
          final String outcome;
          if (throwable != null) {
            logger.warn("Error deleting avatar", throwable);
            outcome = "error";
          } else {
            outcome = "success";
          }

          Metrics.counter(DELETE_AVATAR_COUNTER_NAME, "outcome", outcome).increment();
          return null;
        })
        .thenRun(Util.NOOP);
  }

  public Optional<VersionedProfile> get(UUID uuid, String version) {
    Optional<VersionedProfile> profile = redisGet(uuid, version);

    if (profile.isEmpty()) {
      profile = profiles.get(uuid, version);
      profile.ifPresent(versionedProfile -> redisSet(uuid, versionedProfile));
    }

    return profile;
  }

  public CompletableFuture<Optional<VersionedProfile>> getAsync(UUID uuid, String version) {
    return redisGetAsync(uuid, version)
        .thenCompose(maybeVersionedProfile -> maybeVersionedProfile
            .map(versionedProfile -> CompletableFuture.completedFuture(maybeVersionedProfile))
            .orElseGet(() -> profiles.getAsync(uuid, version)
                .thenCompose(maybeVersionedProfileFromDynamo -> maybeVersionedProfileFromDynamo
                    .map(profile -> redisSetAsync(uuid, profile).thenApply(ignored -> maybeVersionedProfileFromDynamo))
                    .orElseGet(() -> CompletableFuture.completedFuture(maybeVersionedProfileFromDynamo)))));
  }

  private void redisSet(UUID uuid, VersionedProfile profile) {
    try {
      final String profileJson = mapper.writeValueAsString(profile);

      cacheCluster.useCluster(connection -> connection.sync().hset(getCacheKey(uuid), profile.version(), profileJson));
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private CompletableFuture<Void> redisSetAsync(UUID uuid, VersionedProfile profile) {
    final String profileJson;

    try {
      profileJson = mapper.writeValueAsString(profile);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(e);
    }

    return cacheCluster.withCluster(connection ->
        connection.async().hset(getCacheKey(uuid), profile.version(), profileJson))
            .thenRun(Util.NOOP)
            .toCompletableFuture();
  }

  private Optional<VersionedProfile> redisGet(UUID uuid, String version) {
    try {
      @Nullable final String json = cacheCluster.withCluster(connection -> connection.sync().hget(getCacheKey(uuid), version));

      return parseProfileJson(json);
    } catch (RedisException e) {
      logger.warn("Redis exception", e);
      return Optional.empty();
    }
  }

  private CompletableFuture<Optional<VersionedProfile>> redisGetAsync(UUID uuid, String version) {
    return cacheCluster.withCluster(connection ->
        connection.async().hget(getCacheKey(uuid), version))
        .thenApply(this::parseProfileJson)
        .exceptionally(throwable -> {
          logger.warn("Failed to read versioned profile from Redis", throwable);
          return Optional.empty();
        })
        .toCompletableFuture();
  }

  private Optional<VersionedProfile> parseProfileJson(@Nullable final String maybeJson) {
    try {
      if (maybeJson != null) {
        return Optional.of(mapper.readValue(maybeJson, VersionedProfile.class));
      }
      return Optional.empty();
    } catch (final IOException e) {
      logger.warn("Error deserializing value...", e);
      return Optional.empty();
    }
  }

  private CompletableFuture<Void> redisDelete(UUID uuid) {
    return cacheCluster.withCluster(connection -> connection.async().del(getCacheKey(uuid)))
        .toCompletableFuture()
        .thenRun(Util.NOOP);
  }

  @VisibleForTesting
  static String getCacheKey(UUID uuid) {
    return CACHE_PREFIX + uuid.toString();
  }
}
