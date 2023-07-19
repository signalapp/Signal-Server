/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.assertj.core.api.Assertions.assertThat;
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
import java.util.Base64;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.tests.util.MockRedisFuture;
import org.whispersystems.textsecuregcm.tests.util.RedisClusterHelper;

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
  public void testGetProfileInCache() {
    UUID uuid = UUID.randomUUID();

    when(commands.hget(eq("profiles::" + uuid), eq("someversion"))).thenReturn("{\"version\": \"someversion\", \"name\": \"somename\", \"avatar\": \"someavatar\", \"commitment\":\"" + Base64.getEncoder().encodeToString("somecommitment".getBytes()) + "\"}");

    Optional<VersionedProfile> profile = profilesManager.get(uuid, "someversion");

    assertTrue(profile.isPresent());
    assertEquals(profile.get().getName(), "somename");
    assertEquals(profile.get().getAvatar(), "someavatar");
    assertThat(profile.get().getCommitment()).isEqualTo("somecommitment".getBytes());

    verify(commands, times(1)).hget(eq("profiles::" + uuid), eq("someversion"));
    verifyNoMoreInteractions(commands);
    verifyNoMoreInteractions(profiles);
  }

  @Test
  public void testGetProfileAsyncInCache() {
    UUID uuid = UUID.randomUUID();

    when(asyncCommands.hget(eq("profiles::" + uuid), eq("someversion"))).thenReturn(
        MockRedisFuture.completedFuture("{\"version\": \"someversion\", \"name\": \"somename\", \"avatar\": \"someavatar\", \"commitment\":\"" + Base64.getEncoder().encodeToString("somecommitment".getBytes()) + "\"}"));

    Optional<VersionedProfile> profile = profilesManager.getAsync(uuid, "someversion").join();

    assertTrue(profile.isPresent());
    assertEquals(profile.get().getName(), "somename");
    assertEquals(profile.get().getAvatar(), "someavatar");
    assertThat(profile.get().getCommitment()).isEqualTo("somecommitment".getBytes());

    verify(asyncCommands, times(1)).hget(eq("profiles::" + uuid), eq("someversion"));
    verifyNoMoreInteractions(asyncCommands);
    verifyNoMoreInteractions(profiles);
  }

  @Test
  public void testGetProfileNotInCache() {
    UUID             uuid    = UUID.randomUUID();
    VersionedProfile profile = new VersionedProfile("someversion", "somename", "someavatar", null, null,
        null, "somecommitment".getBytes());

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
    UUID uuid = UUID.randomUUID();
    VersionedProfile profile = new VersionedProfile("someversion", "somename", "someavatar", null, null,
        null, "somecommitment".getBytes());

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
    UUID             uuid    = UUID.randomUUID();
    VersionedProfile profile = new VersionedProfile("someversion", "somename", "someavatar", null, null,
        null, "somecommitment".getBytes());

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
    UUID uuid = UUID.randomUUID();
    VersionedProfile profile = new VersionedProfile("someversion", "somename", "someavatar", null, null,
        null, "somecommitment".getBytes());

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
    UUID uuid = UUID.randomUUID();
    VersionedProfile profile = new VersionedProfile("someversion", "somename", "someavatar", null, null,
        null, "somecommitment".getBytes());

    profilesManager.set(uuid, profile);

    verify(commands, times(1)).hset(eq("profiles::" + uuid), eq("someversion"), any());
    verifyNoMoreInteractions(commands);

    verify(profiles, times(1)).set(eq(uuid), eq(profile));
    verifyNoMoreInteractions(profiles);
  }

  @Test
  public void testSetAsync() {
    UUID uuid = UUID.randomUUID();
    VersionedProfile profile = new VersionedProfile("someversion", "somename", "someavatar", null, null,
        null, "somecommitment".getBytes());

    when(asyncCommands.hset(eq("profiles::" + uuid), eq("someversion"), anyString())).thenReturn(MockRedisFuture.completedFuture(null));
    when(profiles.setAsync(eq(uuid), eq(profile))).thenReturn(CompletableFuture.completedFuture(null));

    profilesManager.setAsync(uuid, profile).join();

    verify(asyncCommands, times(1)).hset(eq("profiles::" + uuid), eq("someversion"), any());
    verifyNoMoreInteractions(asyncCommands);

    verify(profiles, times(1)).setAsync(eq(uuid), eq(profile));
    verifyNoMoreInteractions(profiles);
  }
}
