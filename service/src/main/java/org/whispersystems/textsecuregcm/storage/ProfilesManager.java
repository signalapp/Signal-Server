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
import io.micrometer.core.instrument.Metrics;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import org.whispersystems.textsecuregcm.util.ResilienceUtil;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.Util;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;

public class ProfilesManager {

  private final Logger logger = LoggerFactory.getLogger(ProfilesManager.class);

  private static final String CACHE_PREFIX_V1 = "profiles_v1::";

  private final Profiles profiles;
  private final FaultTolerantRedisClusterClient cacheCluster;
  private final ScheduledExecutorService retryExecutor;
  private final S3AsyncClient s3Client;
  private final String bucket;
  private final ObjectMapper mapper;

  private static final String RETRY_NAME = ResilienceUtil.name(ProfilesManager.class);

  private static final String DELETE_AVATAR_COUNTER_NAME = name(ProfilesManager.class, "deleteAvatar");

  public ProfilesManager(final Profiles profiles,
      final FaultTolerantRedisClusterClient cacheCluster,
      final ScheduledExecutorService retryExecutor,
      final S3AsyncClient s3Client,
      final String bucket) {
    this.profiles = profiles;
    this.cacheCluster = cacheCluster;
    this.retryExecutor = retryExecutor;
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

  /**
   * Delete all profiles for the given uuid.
   * <p>
   * Avatars should be included for explicit delete actions, such as API calls and expired accounts. Implicit
   * deletions, such as registration, should preserve them, so that PIN recovery includes the avatar.
   */
  public CompletableFuture<Void> deleteAll(UUID uuid, final boolean includeAvatar) {

    final CompletableFuture<Void> profilesAndAvatars = Mono.fromFuture(profiles.deleteAll(uuid))
        .flatMapIterable(Function.identity())
        .flatMap(avatar ->
          Mono.fromFuture(includeAvatar ? deleteAvatar(avatar) : CompletableFuture.completedFuture(null))
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
      try {
        profile.ifPresent(versionedProfile -> redisSet(uuid, versionedProfile));
      } catch (RedisException e) {
        logger.warn("Failed to cache retrieved profile", e);
      }
    }

    return profile;
  }

  public CompletableFuture<Optional<VersionedProfile>> getAsync(UUID uuid, String version) {
    return redisGetAsync(uuid, version)
        .thenCompose(maybeVersionedProfile -> maybeVersionedProfile
            .map(versionedProfile -> CompletableFuture.completedFuture(maybeVersionedProfile))
            .orElseGet(() -> profiles.getAsync(uuid, version)
                .thenCompose(maybeVersionedProfileFromDynamo -> maybeVersionedProfileFromDynamo
                    .map(profile -> redisSetAsync(uuid, profile)
                        .exceptionally(ExceptionUtils.exceptionallyHandler(RedisException.class, e -> {
                          logger.warn("Failed to cache retrieved profile", e);
                          return null;
                        }))
                        .thenApply(ignored -> maybeVersionedProfileFromDynamo))
                    .orElseGet(() -> CompletableFuture.completedFuture(maybeVersionedProfileFromDynamo)))));
  }

  private void redisSet(UUID uuid, VersionedProfile profile) {
    try {
      final byte[] profileJson = mapper.writeValueAsBytes(profile);

      cacheCluster.useBinaryCluster(connection -> connection.sync().hset(getCacheKeyV1(uuid), profile.version().getBytes(), profileJson));
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private CompletableFuture<Void> redisSetAsync(UUID uuid, VersionedProfile profile) {
    final byte[] profileJson;

    try {
      profileJson = mapper.writeValueAsBytes(profile);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(e);
    }

    return cacheCluster.withBinaryCluster(connection ->
        connection.async().hset(getCacheKeyV1(uuid), profile.version().getBytes(), profileJson)).toCompletableFuture()
            .thenRun(Util.NOOP)
            .toCompletableFuture();
  }

  private Optional<VersionedProfile> redisGet(UUID uuid, String version) {
    try {
      @Nullable final byte[] json = cacheCluster.withBinaryCluster(connection -> connection.sync().hget(getCacheKeyV1(uuid), version.getBytes()));

      return parseProfileJson(json);
    } catch (RedisException e) {
      logger.warn("Failed to retrieve profile from cache", e);
      return Optional.empty();
    }
  }

  private CompletableFuture<Optional<VersionedProfile>> redisGetAsync(UUID uuid, String version) {
    return cacheCluster.withBinaryCluster(connection ->
        connection.async().hget(getCacheKeyV1(uuid), version.getBytes()))
        .thenApply(this::parseProfileJson)
        .exceptionally(throwable -> {
          logger.warn("Failed to read versioned profile from Redis", throwable);
          return Optional.empty();
        })
        .toCompletableFuture();
  }

  private Optional<VersionedProfile> parseProfileJson(@Nullable final byte[] maybeJson) {
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

    return ResilienceUtil.getGeneralRedisRetry(RETRY_NAME)
        .executeCompletionStage(retryExecutor, () -> cacheCluster.withBinaryCluster(connection -> connection.async().del(getCacheKeyV1(uuid))))
        .toCompletableFuture()
        .thenRun(Util.NOOP);
  }

  @VisibleForTesting
  static byte[] getCacheKeyV1(UUID uuid) {
    return (CACHE_PREFIX_V1 + '{' + uuid.toString() + '}').getBytes();
  }
}
