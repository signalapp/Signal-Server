/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalMatchers.aryEq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.lettuce.core.RedisException;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.signal.libsignal.protocol.ServiceId;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.profiles.ProfileKey;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;
import org.whispersystems.textsecuregcm.tests.util.MockRedisFuture;
import org.whispersystems.textsecuregcm.tests.util.ProfileTestHelper;
import org.whispersystems.textsecuregcm.tests.util.RedisClusterHelper;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;

@Timeout(value = 10, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
public class ProfilesManagerTest {

  private Profiles profiles;
  private RedisAdvancedClusterCommands<byte[], byte[]> binaryCommands;
  private RedisAdvancedClusterAsyncCommands<byte[], byte[]> binaryAsyncCommands;
  private S3AsyncClient s3Client;

  private ProfilesManager profilesManager;

  private static final String BUCKET = "bucket";

  @BeforeEach
  void setUp() {
    //noinspection unchecked
    binaryCommands = mock(RedisAdvancedClusterCommands.class);
    //noinspection unchecked
    binaryAsyncCommands = mock(RedisAdvancedClusterAsyncCommands.class);
    final FaultTolerantRedisClusterClient cacheCluster = RedisClusterHelper.builder()
        .binaryCommands(binaryCommands)
        .binaryAsyncCommands(binaryAsyncCommands)
        .build();

    profiles = mock(Profiles.class);
    s3Client = mock(S3AsyncClient.class);

    profilesManager = new ProfilesManager(profiles, cacheCluster, mock(ScheduledExecutorService.class), s3Client, BUCKET);
  }

  @Test
  public void testGetProfileInCache() throws InvalidInputException {
    final UUID uuid = UUID.randomUUID();
    final byte[] name = TestRandomUtil.nextBytes(81);
    final byte[] commitment = new ProfileKey(new byte[32]).getCommitment(new ServiceId.Aci(uuid)).serialize();
    when(binaryCommands.hget(eq(ProfilesManager.getCacheKeyV1(uuid)), aryEq("someversion".getBytes())))
        .thenReturn(String.format(
            "{\"version\": \"someversion\", \"name\": \"%s\", \"avatar\": \"someavatar\", \"commitment\":\"%s\"}",
            ProfileTestHelper.encodeToBase64(name),
            ProfileTestHelper.encodeToBase64(commitment)).getBytes());

    Optional<VersionedProfile> profile = profilesManager.get(uuid, "someversion");

    assertTrue(profile.isPresent());
    assertArrayEquals(profile.get().name(), name);
    assertEquals("someavatar", profile.get().avatar());
    assertArrayEquals(profile.get().commitment(), commitment);

    verify(binaryCommands, times(1)).hget(eq(ProfilesManager.getCacheKeyV1(uuid)), aryEq("someversion".getBytes()));
    verifyNoMoreInteractions(binaryCommands);
    verifyNoMoreInteractions(profiles);
  }

  @Test
  public void testGetProfileAsyncInCache() throws InvalidInputException {
    final UUID uuid = UUID.randomUUID();
    final byte[] name = TestRandomUtil.nextBytes(81);
    final byte[] commitment = new ProfileKey(new byte[32]).getCommitment(new ServiceId.Aci(uuid)).serialize();

    when(binaryAsyncCommands.hget(eq(ProfilesManager.getCacheKeyV1(uuid)), aryEq("someversion".getBytes())))
        .thenReturn(MockRedisFuture.completedFuture(String.format("{\"version\": \"someversion\", \"name\": \"%s\", \"avatar\": \"someavatar\", \"commitment\":\"%s\"}",
            ProfileTestHelper.encodeToBase64(name),
            ProfileTestHelper.encodeToBase64(commitment)).getBytes()));

    Optional<VersionedProfile> profile = profilesManager.getAsync(uuid, "someversion").join();

    assertTrue(profile.isPresent());
    assertArrayEquals(profile.get().name(), name);
    assertEquals("someavatar", profile.get().avatar());
    assertArrayEquals(profile.get().commitment(), commitment);

    verify(binaryAsyncCommands, times(1)).hget(eq(ProfilesManager.getCacheKeyV1(uuid)), aryEq("someversion".getBytes()));
    verifyNoMoreInteractions(binaryAsyncCommands);
    verifyNoMoreInteractions(profiles);
  }

  @Test
  public void testGetProfileNotInCache() {
    final UUID uuid = UUID.randomUUID();
    final byte[] name = TestRandomUtil.nextBytes(81);
    final VersionedProfile profile = new VersionedProfile("someversion", name, "someavatar", null, null,
        null, null, "somecommitment".getBytes());

    when(binaryCommands.hget(eq(ProfilesManager.getCacheKeyV1(uuid)), aryEq("someversion".getBytes()))).thenReturn(null);
    when(profiles.get(eq(uuid), eq("someversion"))).thenReturn(Optional.of(profile));

    Optional<VersionedProfile> retrieved = profilesManager.get(uuid, "someversion");

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), profile);

    verify(binaryCommands, times(1)).hget(eq(ProfilesManager.getCacheKeyV1(uuid)), aryEq("someversion".getBytes()));
    verify(binaryCommands, times(1)).hset(eq(ProfilesManager.getCacheKeyV1(uuid)), aryEq("someversion".getBytes()), any(byte[].class));
    verifyNoMoreInteractions(binaryCommands);

    verify(profiles, times(1)).get(eq(uuid), eq("someversion"));
    verifyNoMoreInteractions(profiles);
  }

  @Test
  public void testGetProfileAsyncNotInCache() {
    final UUID uuid = UUID.randomUUID();
    final byte[] name = TestRandomUtil.nextBytes(81);
    final VersionedProfile profile = new VersionedProfile("someversion", name, "someavatar", null, null,
        null, null, "somecommitment".getBytes());

    when(binaryAsyncCommands.hget(eq(ProfilesManager.getCacheKeyV1(uuid)), eq("someversion".getBytes()))).thenReturn(MockRedisFuture.completedFuture(null));
    when(binaryAsyncCommands.hset(eq(ProfilesManager.getCacheKeyV1(uuid)), eq("someversion".getBytes()), any(byte[].class))).thenReturn(MockRedisFuture.completedFuture(null));
    when(profiles.getAsync(eq(uuid), eq("someversion"))).thenReturn(CompletableFuture.completedFuture(Optional.of(profile)));

    Optional<VersionedProfile> retrieved = profilesManager.getAsync(uuid, "someversion").join();

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), profile);

    verify(binaryAsyncCommands, times(1)).hget(eq(ProfilesManager.getCacheKeyV1(uuid)), eq("someversion".getBytes()));
    verify(binaryAsyncCommands, times(1)).hset(eq(ProfilesManager.getCacheKeyV1(uuid)), eq("someversion".getBytes()), any(byte[].class));
    verifyNoMoreInteractions(binaryAsyncCommands);

    verify(profiles, times(1)).getAsync(eq(uuid), eq("someversion"));
    verifyNoMoreInteractions(profiles);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testGetProfileBrokenCache(final boolean failUpdateCache) {
    final UUID uuid = UUID.randomUUID();
    final byte[] name = TestRandomUtil.nextBytes(81);
    final VersionedProfile profile = new VersionedProfile("someversion", name, "someavatar", null, null,
        null, null, "somecommitment".getBytes());

    when(binaryCommands.hget(eq(ProfilesManager.getCacheKeyV1(uuid)), aryEq("someversion".getBytes()))).thenThrow(new RedisException("Connection lost"));
    if (failUpdateCache) {
      when(binaryCommands.hset(eq(ProfilesManager.getCacheKeyV1(uuid)), aryEq("someversion".getBytes()), any(byte[].class)))
        .thenThrow(new RedisException("Connection lost"));
    }
    when(profiles.get(eq(uuid), eq("someversion"))).thenReturn(Optional.of(profile));

    Optional<VersionedProfile> retrieved = profilesManager.get(uuid, "someversion");

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), profile);

    verify(binaryCommands, times(1)).hget(eq(ProfilesManager.getCacheKeyV1(uuid)), aryEq("someversion".getBytes()));
    verify(binaryCommands, times(1)).hset(eq(ProfilesManager.getCacheKeyV1(uuid)), aryEq("someversion".getBytes()), any(byte[].class));
    verifyNoMoreInteractions(binaryCommands);

    verify(profiles, times(1)).get(eq(uuid), eq("someversion"));
    verifyNoMoreInteractions(profiles);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testGetProfileAsyncBrokenCache(final boolean failUpdateCache) {
    final UUID uuid = UUID.randomUUID();
    final byte[] name = TestRandomUtil.nextBytes(81);
    final VersionedProfile profile = new VersionedProfile("someversion", name, "someavatar", null, null,
        null, null, "somecommitment".getBytes());

    when(binaryAsyncCommands.hget(eq(ProfilesManager.getCacheKeyV1(uuid)), eq("someversion".getBytes()))).thenReturn(MockRedisFuture.failedFuture(new RedisException("Connection lost")));
    when(binaryAsyncCommands.hset(eq(ProfilesManager.getCacheKeyV1(uuid)), eq("someversion".getBytes()), any(byte[].class)))
        .thenReturn(failUpdateCache
            ? MockRedisFuture.failedFuture(new RedisException("Connection lost"))
            : MockRedisFuture.completedFuture(null));
    when(profiles.getAsync(eq(uuid), eq("someversion"))).thenReturn(CompletableFuture.completedFuture(Optional.of(profile)));

    Optional<VersionedProfile> retrieved = profilesManager.getAsync(uuid, "someversion").join();

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), profile);

    verify(binaryAsyncCommands, times(1)).hget(eq(ProfilesManager.getCacheKeyV1(uuid)), eq("someversion".getBytes()));
    verify(binaryAsyncCommands, times(1)).hset(eq(ProfilesManager.getCacheKeyV1(uuid)), eq("someversion".getBytes()), any(byte[].class));
    verifyNoMoreInteractions(binaryAsyncCommands);

    verify(profiles, times(1)).getAsync(eq(uuid), eq("someversion"));
    verifyNoMoreInteractions(profiles);
  }

  @Test
  public void testSet() {
    final UUID uuid = UUID.randomUUID();
    final byte[] name = TestRandomUtil.nextBytes(81);
    final VersionedProfile profile = new VersionedProfile("someversion", name, "someavatar", null, null,
        null, null, "somecommitment".getBytes());

    profilesManager.set(uuid, profile);

    verify(binaryCommands, times(1)).hset(eq(ProfilesManager.getCacheKeyV1(uuid)), aryEq("someversion".getBytes()), any(byte[].class));
    verifyNoMoreInteractions(binaryCommands);

    verify(profiles, times(1)).set(eq(uuid), eq(profile));
    verifyNoMoreInteractions(profiles);
  }

  @Test
  public void testSetAsync() {
    final UUID uuid = UUID.randomUUID();
    final byte[] name = TestRandomUtil.nextBytes(81);
    final VersionedProfile profile = new VersionedProfile("someversion", name, "someavatar", null, null,
        null, null, "somecommitment".getBytes());

    when(binaryAsyncCommands.hset(eq(ProfilesManager.getCacheKeyV1(uuid)), eq("someversion".getBytes()), any(byte[].class))).thenReturn(MockRedisFuture.completedFuture(null));
    when(profiles.setAsync(eq(uuid), eq(profile))).thenReturn(CompletableFuture.completedFuture(null));

    profilesManager.setAsync(uuid, profile).join();

    verify(binaryAsyncCommands, times(1)).hset(eq(ProfilesManager.getCacheKeyV1(uuid)), eq("someversion".getBytes()), any());
    verifyNoMoreInteractions(binaryAsyncCommands);

    verify(profiles, times(1)).setAsync(eq(uuid), eq(profile));
    verifyNoMoreInteractions(profiles);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testDeleteAll(final boolean includeAvatar) {
    final UUID uuid = UUID.randomUUID();

    final String avatarOne = "avatar1";
    final String avatarTwo = "avatar2";
    when(profiles.deleteAll(uuid)).thenReturn(CompletableFuture.completedFuture(List.of(avatarOne, avatarTwo)));
    when(binaryAsyncCommands.del(ProfilesManager.getCacheKeyV1(uuid))).thenReturn(MockRedisFuture.completedFuture(null));
    when(s3Client.deleteObject(any(DeleteObjectRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(null))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException("some error")));

    profilesManager.deleteAll(uuid, includeAvatar).join();

    verify(profiles).deleteAll(uuid);
    verify(binaryAsyncCommands).del(ProfilesManager.getCacheKeyV1(uuid));
    if (includeAvatar) {
      verify(s3Client).deleteObject(DeleteObjectRequest.builder()
          .bucket(BUCKET)
          .key(avatarOne)
          .build());
      verify(s3Client).deleteObject(DeleteObjectRequest.builder()
          .bucket(BUCKET)
          .key(avatarTwo)
          .build());
    } else {
      verifyNoInteractions(s3Client);
    }
  }
}
