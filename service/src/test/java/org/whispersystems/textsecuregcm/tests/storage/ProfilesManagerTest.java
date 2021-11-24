/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.lettuce.core.RedisException;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import java.util.Base64;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicProfileMigrationConfiguration;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.storage.Profiles;
import org.whispersystems.textsecuregcm.storage.ProfilesDynamoDb;
import org.whispersystems.textsecuregcm.storage.ProfilesManager;
import org.whispersystems.textsecuregcm.storage.VersionedProfile;
import org.whispersystems.textsecuregcm.tests.util.RedisClusterHelper;

public class ProfilesManagerTest {

  private Profiles profiles;
  private RedisAdvancedClusterCommands<String, String> commands;

  private ProfilesManager profilesManager;

  @BeforeEach
  void setUp() {
    //noinspection unchecked
    commands = mock(RedisAdvancedClusterCommands.class);
    final FaultTolerantRedisCluster cacheCluster = RedisClusterHelper.buildMockRedisCluster(commands);

    profiles = mock(Profiles.class);

    @SuppressWarnings("unchecked") final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager =
        mock(DynamicConfigurationManager.class);

    final DynamicConfiguration dynamicConfiguration = mock(DynamicConfiguration.class);
    final DynamicProfileMigrationConfiguration profileMigrationConfiguration =
        mock(DynamicProfileMigrationConfiguration.class);

    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);
    when(dynamicConfiguration.getProfileMigrationConfiguration()).thenReturn(profileMigrationConfiguration);

    profilesManager = new ProfilesManager(profiles,
        mock(ProfilesDynamoDb.class),
        cacheCluster,
        dynamicConfigurationManager,
        mock(Executor.class));
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
}
