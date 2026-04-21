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
import io.lettuce.core.ScriptOutputType;
import io.micrometer.core.instrument.Metrics;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.redis.ClusterLuaScript;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;
import org.whispersystems.textsecuregcm.util.ResilienceUtil;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.Util;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.MetadataDirective;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

public class ProfilesManager {

  private final Logger logger = LoggerFactory.getLogger(ProfilesManager.class);

  private static final String CACHE_PREFIX_V1 = "profiles_v1::";
  private static final String CACHE_PREFIX_V2 = "profiles_v2::";

  private static final byte[] EMPTY = new byte[0];

  private final Profiles profilesV1;
  private final ProfilesV2 profilesV2;
  private final ProfileAvatars profileAvatars;
  private final FaultTolerantRedisClusterClient cacheCluster;
  private final ScheduledExecutorService retryExecutor;
  private final S3AsyncClient s3Client;
  private final String bucket;
  private final ClusterLuaScript setLuaScript;
  private final ObjectMapper mapper;

  private static final String RETRY_NAME = ResilienceUtil.name(ProfilesManager.class);

  private static final String DELETE_AVATAR_COUNTER_NAME = name(ProfilesManager.class, "deleteAvatar");

  public ProfilesManager(final Profiles profilesV1,
      final ProfilesV2 profilesV2,
      final ProfileAvatars profileAvatars,
      final FaultTolerantRedisClusterClient cacheCluster,
      final ScheduledExecutorService retryExecutor,
      final S3AsyncClient s3Client,
      final String bucket) throws IOException {
    this(profilesV1, profilesV2, profileAvatars, cacheCluster, retryExecutor, s3Client, bucket,
        ClusterLuaScript.fromResource(cacheCluster, "lua/profile_set.lua", ScriptOutputType.STATUS));
  }

  @VisibleForTesting
  ProfilesManager(final Profiles profilesV1,
      final ProfilesV2 profilesV2,
      final ProfileAvatars profileAvatars,
      final FaultTolerantRedisClusterClient cacheCluster,
      final ScheduledExecutorService retryExecutor,
      final S3AsyncClient s3Client,
      final String bucket,
      final ClusterLuaScript setLuaScript) {
    this.profilesV1 = profilesV1;
    this.profilesV2 = profilesV2;
    this.profileAvatars = profileAvatars;
    this.cacheCluster = cacheCluster;
    this.retryExecutor = retryExecutor;
    this.s3Client = s3Client;
    this.bucket = bucket;
    this.setLuaScript = setLuaScript;
    this.mapper = SystemMapper.jsonMapper();
  }

  public void setV1(UUID uuid, VersionedProfileV1 versionedProfile) {
    redisDelete(uuid);

    profilesV1.set(uuid, versionedProfile);

    redisSet(uuid, null, versionedProfile);

    try {
      // until the PROFILES_V2 capability prevents downgrades, if a client sets a v2 profile, and then the account
      // downgrades to v1, all v2 profiles are stale
      assert !DeviceCapability.PROFILES_V2.preventDowngrade();

      profilesV2.deleteAll(uuid);
    } catch (final Exception e) {
      logger.warn("Failed to delete v2 profile data: {}", uuid, e);
    }
  }

  /// Transactionally sets v1 and v2 Profile data in DynamoDB.
  ///
  /// Note: writes to the Redis cache are not transactional, and a failed DynamoDB write may leave incorrect data in Redis
  /// @throws WriteConflictException if the expected data hash does not match the stored data hash
  public void set(UUID uuid, VersionedProfileV1 versionedProfileV1, VersionedProfile versionedProfile, @Nullable byte[] expectedCurrentDataHash) throws WriteConflictException {

    final TransactWriteItem v1TransactWriteItem = profilesV1.getTransactWriteItem(uuid, versionedProfileV1);

    redisDelete(uuid);

    profilesV2.set(uuid,
        versionedProfile.version(),
        versionedProfile.data(),
        versionedProfile.dataHash(),
        versionedProfile.commitment(),
        versionedProfile.paymentAddress(),
        versionedProfile.paymentAddressHash(),
        expectedCurrentDataHash,
        v1TransactWriteItem);

    redisSet(uuid, versionedProfile, versionedProfileV1);
  }

  /// Delete all profiles for the given uuid.
  ///
  /// Avatars should be included for explicit delete actions, such as API calls and expired accounts. Implicit
  /// deletions, such as registration, should preserve them, so that PIN recovery includes the avatar.
  public CompletableFuture<Void> deleteAll(UUID uuid, final boolean includeAvatar) {

    final CompletableFuture<Void> profilesV1AndAvatars = Mono.fromFuture(profilesV1.deleteAll(uuid))
        .flatMapIterable(Function.identity())
        .flatMap(avatar ->
          Mono.fromFuture(includeAvatar ? deleteAvatar(avatar) : CompletableFuture.completedFuture(null))
              // this is best-effort
              .retry(3)
              .onErrorComplete())
        .then().toFuture();

    return CompletableFuture.allOf(redisDeleteAsync(uuid), profilesV1AndAvatars, profilesV2.deleteAll(uuid));
  }

  public CompletableFuture<Void> deleteAvatar(String avatar) {
    return s3Client.deleteObject(DeleteObjectRequest.builder()
        .bucket(bucket)
        .key(avatar)
        .build())
        .whenComplete((_, throwable) -> {
          final String outcome;
          if (throwable != null) {
            logger.warn("Error deleting avatar", throwable);
            outcome = "error";
          } else {
            outcome = "success";
          }

          Metrics.counter(DELETE_AVATAR_COUNTER_NAME, "outcome", outcome).increment();
        })
        .thenRun(Util.NOOP);
  }

  public Optional<VersionedProfileV1> getV1(UUID uuid, String version) {
    return redisGetV1(uuid, version).or(() -> {
      final Optional<VersionedProfileV1> profile = profilesV1.get(uuid, version);
      try {
        profile.ifPresent(versionedProfile -> redisSet(uuid, null, versionedProfile));
      } catch (RedisException e) {
        logger.warn("Failed to cache retrieved profile", e);
      }

      return profile;
    });
  }

  public Optional<VersionedProfile> get(UUID uuid, byte[] version) {
    return redisGet(uuid, version).or(() -> {
      final Optional<VersionedProfile> profile = profilesV2.get(uuid, version);
      try {
        profile.ifPresent(versionedProfile -> redisSet(uuid, versionedProfile, null));
      } catch (RedisException e) {
        logger.warn("Failed to cache retrieved profile", e);
      }

      return profile;
    });
  }

  @VisibleForTesting
  void redisSet(UUID uuid, @Nullable VersionedProfile v2Profile, @Nullable VersionedProfileV1 v1Profile) {
    try {
      if (v1Profile == null && v2Profile == null) {
        logger.error("Expected at least one profile");
        return;
      }

      final byte[] v1ProfileVersion;
      final byte[] v1ProfileBytes;
      if (v1Profile != null) {
        v1ProfileVersion = v1Profile.version().getBytes();
        v1ProfileBytes = mapper.writeValueAsBytes(v1Profile);
      } else {
        v1ProfileVersion = EMPTY;
        v1ProfileBytes = EMPTY;
      }

      final byte[] v2ProfileVersion;
      final byte[] v2ProfileBytes;
      if (v2Profile != null) {
        v2ProfileVersion = v2Profile.version();
        v2ProfileBytes = mapper.writeValueAsBytes(v2Profile);
      } else {
        v2ProfileVersion = EMPTY;
        v2ProfileBytes = EMPTY;
      }

      setLuaScript.executeBinary(List.of(getCacheKeyV1(uuid), getCacheKeyV2(uuid)),
          List.of(v1ProfileVersion, v1ProfileBytes, v2ProfileVersion, v2ProfileBytes));

    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private Optional<VersionedProfile> redisGet(UUID uuid, byte[] version) {
    try {
      @Nullable final byte[] json = cacheCluster.withBinaryCluster(connection -> connection.sync().hget(getCacheKeyV2(uuid), version));

      return parseProfileJson(json, VersionedProfile.class);
    } catch (RedisException e) {
      logger.warn("Redis exception", e);
      return Optional.empty();
    }
  }

  @VisibleForTesting
  static byte[] getCacheKeyV2(UUID uuid) {
    return (CACHE_PREFIX_V2 + '{' + uuid.toString() + '}').getBytes();
  }

  private Optional<VersionedProfileV1> redisGetV1(UUID uuid, String version) {
    try {
      @Nullable final byte[] json = cacheCluster.withBinaryCluster(connection -> connection.sync().hget(getCacheKeyV1(uuid), version.getBytes()));

      return parseProfileJson(json, VersionedProfileV1.class);
    } catch (RedisException e) {
      logger.warn("Failed to retrieve profile from cache", e);
      return Optional.empty();
    }
  }

  private <T> Optional<T> parseProfileJson(@Nullable final byte[] maybeJson, Class<T> klass) {
    try {
      if (maybeJson != null) {
        return Optional.of(mapper.readValue(maybeJson, klass));
      }
      return Optional.empty();
    } catch (final IOException e) {
      logger.warn("Error deserializing value...", e);
      return Optional.empty();
    }
  }

  private void redisDelete(UUID uuid) {
    ResilienceUtil.getGeneralRedisRetry(RETRY_NAME)
        .executeRunnable(() -> cacheCluster.withBinaryCluster(
            connection -> connection.sync().del(getCacheKeyV1(uuid), getCacheKeyV2(uuid))));
  }

  private CompletableFuture<Void> redisDeleteAsync(UUID uuid) {

    return ResilienceUtil.getGeneralRedisRetry(RETRY_NAME)
        .executeCompletionStage(retryExecutor,
            () -> cacheCluster.withBinaryCluster(connection -> connection.async().del(getCacheKeyV1(uuid), getCacheKeyV2(uuid))))
        .toCompletableFuture()
        .thenRun(Util.NOOP);
  }

  @VisibleForTesting
  static byte[] getCacheKeyV1(UUID uuid) {
    return (CACHE_PREFIX_V1 + '{' + uuid.toString() + '}').getBytes();
  }

  public void deleteAvatarForIdentity(final byte[] identity) {
    profileAvatars.deleteAvatarUrl(identity).ifPresent(avatar -> Mono.fromFuture(() ->
            deleteAvatar(avatar))
        .retry(3)
        .onErrorComplete()
        .block());
  }

  /// Sets an avatar for the identity. If there was already an avatar, it will be deleted from storage.
  public void setAvatarForIdentity(byte[] identity, String avatar) {
    profileAvatars.setAvatarUrl(identity, avatar).map(previousAvatar -> Mono.fromFuture(() ->
            deleteAvatar(previousAvatar))
        .retry(3)
        .onErrorComplete()
        .block());
  }

  public Optional<String> extendAvatarTtlForIdentity(final byte[] identity) {

    final Optional<String> maybePath = profileAvatars.updateAvatarTtl(identity);

    return maybePath.map(key -> {
      try {
        // copying the object to itself extends the expiration...
        Mono.fromFuture(() -> s3Client.copyObject(CopyObjectRequest.builder()
                .sourceBucket(bucket)
                .sourceKey(key)
                .destinationBucket(bucket)
                .destinationKey(key)
                .metadataDirective(MetadataDirective.REPLACE)
                // ...but there needs to be a trivial change, otherwise it is rejected
                .metadata(Map.of("t", String.valueOf(Instant.now().getEpochSecond())))
                .build()))
            .retryWhen(Retry.max(3).filter(e -> !(e instanceof NoSuchKeyException)))
            .block();

        return key;
      } catch (NoSuchKeyException _) {
        logger.warn("avatar expected to be present is gone");

        profileAvatars.deleteAvatarUrl(identity);

        return null;
      }
    });
  }
}
