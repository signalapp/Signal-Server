/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junitpioneer.jupiter.params.IntRangeSource;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.ua.ClientPlatform;

class RemoteConfigsManagerTest {
  private static final UUID NORMAL_ACI = UUID.randomUUID();
  private static final UUID SPECIAL_ACI = UUID.randomUUID();

  private RemoteConfigs remoteConfigs;

  private RemoteConfigsManager remoteConfigsManager;

  private static final Map<String, String> GLOBAL = Map.of("maxGroupSize", "42");
  private static final List<RemoteConfig> CLIENT = List.of(
      new RemoteConfig("android.stickers", 100, Set.of(), null, null, null),
      new RemoteConfig("ios.stickers", 100, Set.of(), null, null, null),
      new RemoteConfig("desktop.stickers", 100, Set.of(), null, null, null),
      new RemoteConfig("always.true", 100, Set.of(), null, null, null),
      new RemoteConfig("only.special", 0, Set.of(SPECIAL_ACI), null, null, null),
      new RemoteConfig("value.always.true", 100, Set.of(), "foo", "bar", null),
      new RemoteConfig("value.only.special", 0, Set.of(SPECIAL_ACI), "abc", "xyz", null),
      new RemoteConfig("value.always.false", 0, Set.of(), "red", "green", null),
      new RemoteConfig("linked.config.0", 50, Set.of(), null, null, null),
      new RemoteConfig("linked.config.1", 50, Set.of(), null, null, "linked.config.0"),
      new RemoteConfig("unlinked.config", 50, Set.of(), null, null, null));

  @BeforeEach
  void setup() {
    this.remoteConfigs = mock(RemoteConfigs.class);
    when(remoteConfigs.getAll()).thenReturn(CLIENT);
    this.remoteConfigsManager = new RemoteConfigsManager(remoteConfigs, GLOBAL);
  }

  @Test
  void testGetConfigForAccount() {
    remoteConfigsManager.getConfigForAccount(UUID.randomUUID(), null);
    remoteConfigsManager.getConfigForAccount(UUID.randomUUID(), null);

    // A memoized supplier should prevent multiple calls to the underlying data source
    verify(remoteConfigs, times(1)).getAll();
  }

  @ParameterizedTest
  @EnumSource
  void testRetrieveConfig(ClientPlatform platform) {
    final Map<String, String> configs = remoteConfigsManager.getConfigForAccount(
        SPECIAL_ACI,
        String.format("Signal-%s/7.6.2", platform.name()));

    verify(remoteConfigs, times(1)).getAll();

    assertThat(configs).hasSize(10);
    assertThat(configs).containsKeys(platform.name().toLowerCase() + ".stickers", "linked.config.0", "linked.config.1", "unlinked.config");
    assertThat(configs).contains(
        entry("always.true", "true"),
        entry("only.special", "true"),
        entry("value.always.true", "bar"),
        entry("value.only.special", "xyz"),
        entry("value.always.false", "red"),
        entry("global.maxGroupSize", "42"));
  }

  @ParameterizedTest
  @EnumSource
  void testRetrieveConfigNotSpecial(ClientPlatform platform) {
    final Map<String, String> configs = remoteConfigsManager.getConfigForAccount(
        NORMAL_ACI,
        String.format("Signal-%s/7.6.2", platform.name()));

    verify(remoteConfigs, times(1)).getAll();

    assertThat(configs).hasSize(10);
    assertThat(configs).containsKeys(platform.name().toLowerCase() + ".stickers", "linked.config.0", "linked.config.1", "unlinked.config");
    assertThat(configs).contains(
        entry("always.true", "true"),
        entry("only.special", "false"),
        entry("value.always.true", "bar"),
        entry("value.only.special", "abc"),
        entry("value.always.false", "red"),
        entry("global.maxGroupSize", "42"));
  }

  @Test
  void testRetrieveConfigUnrecognizedPlatform() {
    final Map<String, String> configs = remoteConfigsManager.getConfigForAccount(
        NORMAL_ACI,
        "Third-Party-Signal-Client/1.0.0");

    verify(remoteConfigs, times(1)).getAll();

    assertThat(configs).hasSize(9);
    assertThat(configs).containsKeys("linked.config.0", "linked.config.1", "unlinked.config");
    assertThat(configs).contains(
        entry("always.true", "true"),
        entry("only.special", "false"),
        entry("value.always.true", "bar"),
        entry("value.only.special", "abc"),
        entry("value.always.false", "red"),
        entry("global.maxGroupSize", "42"));
  }

  @Test
  void testHashKeyLinkedConfigs() {
    boolean allUnlinkedConfigsMatched = true;
    for (AuthHelper.TestAccount testAccount : AuthHelper.TEST_ACCOUNTS) {
      final Map<String, String> configs = remoteConfigsManager.getConfigForAccount(testAccount.uuid, null);
      assertThat(configs.get("linked.config.0")).isEqualTo(configs.get("linked.config.1"));
      allUnlinkedConfigsMatched &= (configs.get("linked.config.0").equals(configs.get("unlinked.config")));
    }

    // with 20 test accounts, 1 in 2^20 chance that this fails when it shouldn't, but
    // AuthHelper#generateTestAccounts uses a constant random seed that doesn't fail as of the time
    // of this writing; if this starts failing for no apparent reason, it's likely that we've
    // changed the order of the sequence of random numbers used during test initialization in such
    // a way that we've accidentally picked an unlucky set of accounts here
    assertThat(allUnlinkedConfigsMatched).isFalse();
  }

  @ParameterizedTest
  @IntRangeSource(from = 1, to = 99)
  void testMath(int percentage) throws NoSuchAlgorithmException {
    final MessageDigest digest = MessageDigest.getInstance("SHA-256");
    final Random random = new Random(9424242L);  // the seed value doesn't matter so much as it's constant to make the test not flaky
    final int iterations = 10000;
    int enabledCount = 0;

    for (int i = 0; i < iterations; i++) {
      if (RemoteConfigsManager.isInBucket(digest, AuthHelper.getRandomUUID(random), "test".getBytes(), percentage, Set.of())) {
        enabledCount++;
      }
    }


    // https://en.wikipedia.org/wiki/Binomial_distribution#Expected_value_and_variance
    final double expectedCount = iterations * percentage / 100.0;
    final double stdev = Math.sqrt(expectedCount * (1 - percentage / 100.0));

    // 3 standard deviations = 99.73% chance of success for one bucket, 23.5%
    // chance of any failure in 99 buckets; if this starts failing after a
    // change, run it again with a few different random seeds to make sure it
    // fails only about on about one seed in four
    assertThat((double) enabledCount).isCloseTo(expectedCount, Offset.offset(3 * stdev));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 100})
  void testMathExactForZeroOrOneHundred(int percentage) throws NoSuchAlgorithmException {
    final MessageDigest digest = MessageDigest.getInstance("SHA-256");
    final Random random = new Random();
    final int iterations = 10000;
    int enabledCount = 0;

    for (int i = 0; i < iterations; i++) {
      if (RemoteConfigsManager.isInBucket(digest, AuthHelper.getRandomUUID(random), "test".getBytes(), percentage, Set.of())) {
        enabledCount++;
      }
    }

    assertThat(enabledCount).isEqualTo(iterations * percentage / 100);
  }

}
