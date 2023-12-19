/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.lettuce.core.RedisException;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.signal.libsignal.protocol.ServiceId;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.profiles.ProfileKey;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.tests.util.MockRedisFuture;
import org.whispersystems.textsecuregcm.tests.util.ProfileTestHelper;
import org.whispersystems.textsecuregcm.tests.util.RedisClusterHelper;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

@Timeout(value = 10, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
public class ProfilesManagerTest {

  private Profiles profiles;
  private RedisAdvancedClusterCommands<String, String> commands;
  private RedisAdvancedClusterAsyncCommands<String, String> asyncCommands;

  private ProfilesManager profilesManager;

  @BeforeEach
  void setUp() {
    //noinspection unchecked
    commands = mock(RedisAdvancedClusterCommands.class);
    asyncCommands = mock(RedisAdvancedClusterAsyncCommands.class);
    final FaultTolerantRedisCluster cacheCluster = RedisClusterHelper.builder()
        .stringCommands(commands)
        .stringAsyncCommands(asyncCommands)
        .build();

    profiles = mock(Profiles.class);

    profilesManager = new ProfilesManager(profiles, cacheCluster);
  }

  @Test
  public void testGetProfileInCache() throws InvalidInputException {
    final UUID uuid = UUID.randomUUID();
    final byte[] name = TestRandomUtil.nextBytes(81);
    final byte[] commitment = new ProfileKey(new byte[32]).getCommitment(new ServiceId.Aci(uuid)).serialize();
    when(commands.hget(eq("profiles::" + uuid), eq("someversion"))).thenReturn(String.format(
        "{\"version\": \"someversion\", \"name\": \"%s\", \"avatar\": \"someavatar\", \"commitment\":\"%s\"}",
        ProfileTestHelper.encodeToBase64(name),
        ProfileTestHelper.encodeToBase64(commitment)));

    Optional<VersionedProfile> profile = profilesManager.get(uuid, "someversion");

    assertTrue(profile.isPresent());
    assertArrayEquals(profile.get().name(), name);
    assertEquals(profile.get().avatar(), "someavatar");
    assertArrayEquals(profile.get().commitment(), commitment);

    verify(commands, times(1)).hget(eq("profiles::" + uuid), eq("someversion"));
    verifyNoMoreInteractions(commands);
    verifyNoMoreInteractions(profiles);
  }

  @Test
  public void testGetProfileAsyncInCache() throws InvalidInputException {
    final UUID uuid = UUID.randomUUID();
    final byte[] name = TestRandomUtil.nextBytes(81);
    final byte[] commitment = new ProfileKey(new byte[32]).getCommitment(new ServiceId.Aci(uuid)).serialize();

    when(asyncCommands.hget(eq("profiles::" + uuid), eq("someversion"))).thenReturn(
        MockRedisFuture.completedFuture(String.format("{\"version\": \"someversion\", \"name\": \"%s\", \"avatar\": \"someavatar\", \"commitment\":\"%s\"}",
            ProfileTestHelper.encodeToBase64(name),
            ProfileTestHelper.encodeToBase64(commitment))));

    Optional<VersionedProfile> profile = profilesManager.getAsync(uuid, "someversion").join();

    assertTrue(profile.isPresent());
    assertArrayEquals(profile.get().name(), name);
    assertEquals(profile.get().avatar(), "someavatar");
    assertArrayEquals(profile.get().commitment(), commitment);

    verify(asyncCommands, times(1)).hget(eq("profiles::" + uuid), eq("someversion"));
    verifyNoMoreInteractions(asyncCommands);
    verifyNoMoreInteractions(profiles);
  }

  @Test
  public void testGetProfileNotInCache() {
    final UUID uuid = UUID.randomUUID();
    final byte[] name = TestRandomUtil.nextBytes(81);
    final VersionedProfile profile = new VersionedProfile("someversion", name, "someavatar", null, null,
        null, null, "somecommitment".getBytes());

    when(commands.hget(eq("profiles::" + uuid), eq("someversion"))).thenReturn(null);
    when(profiles.get(eq(uuid), eq("someversion"))).thenReturn(Optional.of(profile));

    Optional<VersionedProfile> retrieved = profilesManager.get(uuid, "someversion");

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), profile);

    verify(commands, times(1)).hget(eq("profiles::" + uuid), eq("someversion"));
    verify(commands, times(1)).hset(eq("profiles::" + uuid), eq("someversion"), anyString());
    verifyNoMoreInteractions(commands);

    verify(profiles, times(1)).get(eq(uuid), eq("someversion"));
    verifyNoMoreInteractions(profiles);
  }

  @Test
  public void testGetProfileAsyncNotInCache() {
    final UUID uuid = UUID.randomUUID();
    final byte[] name = TestRandomUtil.nextBytes(81);
    final VersionedProfile profile = new VersionedProfile("someversion", name, "someavatar", null, null,
        null, null, "somecommitment".getBytes());

    when(asyncCommands.hget(eq("profiles::" + uuid), eq("someversion"))).thenReturn(MockRedisFuture.completedFuture(null));
    when(asyncCommands.hset(eq("profiles::" + uuid), eq("someversion"), anyString())).thenReturn(MockRedisFuture.completedFuture(null));
    when(profiles.getAsync(eq(uuid), eq("someversion"))).thenReturn(CompletableFuture.completedFuture(Optional.of(profile)));

    Optional<VersionedProfile> retrieved = profilesManager.getAsync(uuid, "someversion").join();

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), profile);

    verify(asyncCommands, times(1)).hget(eq("profiles::" + uuid), eq("someversion"));
    verify(asyncCommands, times(1)).hset(eq("profiles::" + uuid), eq("someversion"), anyString());
    verifyNoMoreInteractions(asyncCommands);

    verify(profiles, times(1)).getAsync(eq(uuid), eq("someversion"));
    verifyNoMoreInteractions(profiles);
  }

  @Test
  public void testGetProfileBrokenCache() {
    final UUID uuid = UUID.randomUUID();
    final byte[] name = TestRandomUtil.nextBytes(81);
    final VersionedProfile profile = new VersionedProfile("someversion", name, "someavatar", null, null,
        null, null, "somecommitment".getBytes());

    when(commands.hget(eq("profiles::" + uuid), eq("someversion"))).thenThrow(new RedisException("Connection lost"));
    when(profiles.get(eq(uuid), eq("someversion"))).thenReturn(Optional.of(profile));

    Optional<VersionedProfile> retrieved = profilesManager.get(uuid, "someversion");

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), profile);

    verify(commands, times(1)).hget(eq("profiles::" + uuid), eq("someversion"));
    verify(commands, times(1)).hset(eq("profiles::" + uuid), eq("someversion"), anyString());
    verifyNoMoreInteractions(commands);

    verify(profiles, times(1)).get(eq(uuid), eq("someversion"));
    verifyNoMoreInteractions(profiles);
  }

  @Test
  public void testGetProfileAsyncBrokenCache() {
    final UUID uuid = UUID.randomUUID();
    final byte[] name = TestRandomUtil.nextBytes(81);
    final VersionedProfile profile = new VersionedProfile("someversion", name, "someavatar", null, null,
        null, null, "somecommitment".getBytes());

    when(asyncCommands.hget(eq("profiles::" + uuid), eq("someversion"))).thenReturn(MockRedisFuture.failedFuture(new RedisException("Connection lost")));
    when(asyncCommands.hset(eq("profiles::" + uuid), eq("someversion"), anyString())).thenReturn(MockRedisFuture.completedFuture(null));
    when(profiles.getAsync(eq(uuid), eq("someversion"))).thenReturn(CompletableFuture.completedFuture(Optional.of(profile)));

    Optional<VersionedProfile> retrieved = profilesManager.getAsync(uuid, "someversion").join();

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), profile);

    verify(asyncCommands, times(1)).hget(eq("profiles::" + uuid), eq("someversion"));
    verify(asyncCommands, times(1)).hset(eq("profiles::" + uuid), eq("someversion"), anyString());
    verifyNoMoreInteractions(asyncCommands);

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

    verify(commands, times(1)).hset(eq("profiles::" + uuid), eq("someversion"), any());
    verifyNoMoreInteractions(commands);

    verify(profiles, times(1)).set(eq(uuid), eq(profile));
    verifyNoMoreInteractions(profiles);
  }

  @Test
  public void testSetAsync() {
    final UUID uuid = UUID.randomUUID();
    final byte[] name = TestRandomUtil.nextBytes(81);
    final VersionedProfile profile = new VersionedProfile("someversion", name, "someavatar", null, null,
        null, null, "somecommitment".getBytes());

    when(asyncCommands.hset(eq("profiles::" + uuid), eq("someversion"), anyString())).thenReturn(MockRedisFuture.completedFuture(null));
    when(profiles.setAsync(eq(uuid), eq(profile))).thenReturn(CompletableFuture.completedFuture(null));

    profilesManager.setAsync(uuid, profile).join();

    verify(asyncCommands, times(1)).hset(eq("profiles::" + uuid), eq("someversion"), any());
    verifyNoMoreInteractions(asyncCommands);

    verify(profiles, times(1)).setAsync(eq(uuid), eq(profile));
    verifyNoMoreInteractions(profiles);
  }
}
