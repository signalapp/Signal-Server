/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import jakarta.ws.rs.core.EntityTag;
import jakarta.ws.rs.core.Response;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.assertj.core.data.Offset;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junitpioneer.jupiter.params.IntRangeSource;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.entities.RemoteConfigurationResponse;
import org.whispersystems.textsecuregcm.mappers.DeviceLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.storage.RemoteConfig;
import org.whispersystems.textsecuregcm.storage.RemoteConfigsManager;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.TestClock;
import org.whispersystems.textsecuregcm.util.ua.ClientPlatform;

@ExtendWith(DropwizardExtensionsSupport.class)
class RemoteConfigControllerTest {

  private static final RemoteConfigsManager remoteConfigsManager = mock(RemoteConfigsManager.class);

  private static final long PINNED_EPOCH_SECONDS = 1701287216L;
  private static final TestClock TEST_CLOCK = TestClock.pinned(Instant.ofEpochSecond(PINNED_EPOCH_SECONDS));


  private static final ResourceExtension resources = ResourceExtension.builder()
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(new AuthValueFactoryProvider.Binder<>(AuthenticatedDevice.class))
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addProvider(new DeviceLimitExceededExceptionMapper())
      .addResource(new RemoteConfigController(remoteConfigsManager, Map.of("maxGroupSize", "42"), TEST_CLOCK))
      .build();


  @BeforeEach
  void setup() throws Exception {
    when(remoteConfigsManager.getAll()).thenReturn(
      List.of(
          new RemoteConfig("android.stickers", 100, Set.of(), null, null, null),
          new RemoteConfig("ios.stickers", 100, Set.of(), null, null, null),
          new RemoteConfig("desktop.stickers", 100, Set.of(), null, null, null),
          new RemoteConfig("always.true", 100, Set.of(), null, null, null),
          new RemoteConfig("only.special", 0, Set.of(AuthHelper.VALID_UUID), null, null, null),
          new RemoteConfig("value.always.true", 100, Set.of(), "foo", "bar", null),
          new RemoteConfig("value.only.special", 0, Set.of(AuthHelper.VALID_UUID), "abc", "xyz", null),
          new RemoteConfig("value.always.false", 0, Set.of(), "red", "green", null),
          new RemoteConfig("linked.config.0", 50, Set.of(), null, null, null),
          new RemoteConfig("linked.config.1", 50, Set.of(), null, null, "linked.config.0"),
          new RemoteConfig("unlinked.config", 50, Set.of(), null, null, null)));
  }

  @AfterEach
  void teardown() {
    reset(remoteConfigsManager);
  }

  @ParameterizedTest
  @EnumSource
  void testRetrieveConfig(ClientPlatform platform) {
    RemoteConfigurationResponse configuration = resources.getJerseyTest()
        .target("/v2/config/")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .header("User-Agent", String.format("Signal-%s/7.6.2", platform.name()))
        .get(RemoteConfigurationResponse.class);

    verify(remoteConfigsManager, times(1)).getAll();

    assertThat(configuration.config()).hasSize(10);
    assertThat(configuration.config()).containsKeys(platform.name().toLowerCase() + ".stickers", "linked.config.0", "linked.config.1", "unlinked.config");
    assertThat(configuration.config()).contains(
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
    RemoteConfigurationResponse configuration = resources.getJerseyTest()
        .target("/v2/config/")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_TWO, AuthHelper.VALID_PASSWORD_TWO))
        .header("User-Agent", String.format("Signal-%s/7.6.2", platform.name()))
        .get(RemoteConfigurationResponse.class);

    verify(remoteConfigsManager, times(1)).getAll();

    assertThat(configuration.config()).hasSize(10);
    assertThat(configuration.config()).containsKeys(platform.name().toLowerCase() + ".stickers", "linked.config.0", "linked.config.1", "unlinked.config");
    assertThat(configuration.config()).contains(
        entry("always.true", "true"),
        entry("only.special", "false"),
        entry("value.always.true", "bar"),
        entry("value.only.special", "abc"),
        entry("value.always.false", "red"),
        entry("global.maxGroupSize", "42"));
  }

  @Test
  void testRetrieveConfigUnrecognizedPlatform() {
    RemoteConfigurationResponse configuration = resources.getJerseyTest()
        .target("/v2/config/")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_TWO, AuthHelper.VALID_PASSWORD_TWO))
        .header("User-Agent", "Third-Party-Signal-Client/1.0.0")
        .get(RemoteConfigurationResponse.class);

    verify(remoteConfigsManager, times(1)).getAll();

    assertThat(configuration.config()).hasSize(9);
    assertThat(configuration.config()).containsKeys("linked.config.0", "linked.config.1", "unlinked.config");
    assertThat(configuration.config()).contains(
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
      RemoteConfigurationResponse configuration = resources.getJerseyTest()
          .target("/v2/config/")
          .request()
          .header("Authorization", testAccount.getAuthHeader())
          .get(RemoteConfigurationResponse.class);

      assertThat(configuration.config().get("linked.config.0")).isEqualTo(configuration.config().get("linked.config.1"));
      allUnlinkedConfigsMatched &= (configuration.config().get("linked.config.0") == configuration.config().get("unlinked.config"));
    }

    // with 20 test accounts, 1 in 2^20 chance that this fails when it shouldn't, but
    // AuthHelper#generateTestAccounts uses a constant random seed that doesn't fail as of the time
    // of this writing; if this starts failing for no apparent reason, it's likely that we've
    // changed the order of the sequence of random numbers used during test initialization in such
    // a way that we've accidentally picked an unlucky set of accounts here
    assertThat(allUnlinkedConfigsMatched).isFalse();
  }

  @Test
  void testRetrieveConfigUnauthorized() {
    Response response = resources.getJerseyTest()
        .target("/v2/config/")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.INVALID_PASSWORD))
        .get();

    assertThat(response.getStatus()).isEqualTo(401);

    verifyNoMoreInteractions(remoteConfigsManager);
  }

  @Test
  void testRetrieveConfigUnchanged() {
    Response response = resources.getJerseyTest()
        .target("/v2/config/")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .header("User-Agent", "Signal-Android/7.6.2 Android/34 libsignal/0.46.0")
        .get();

    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.getLength()).isPositive();
    final EntityTag etag = response.getEntityTag();
    assertThat(etag).isNotNull();

    response = resources.getJerseyTest()
        .target("/v2/config/")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .header("User-Agent", "Signal-Android/7.6.2 Android/34 libsignal/0.46.0")
        .header("If-None-Match", etag)
        .get();

    assertThat(response.getStatus()).isEqualTo(304);
    assertThat(response.getLength()).isNotPositive();
  }

  @Test
  void testRetrieveConfigChanged() {
    Response response = resources.getJerseyTest()
        .target("/v2/config/")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .header("User-Agent", "Signal-Android/7.6.2 Android/34 libsignal/0.46.0")
        .get();

    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.getLength()).isPositive();
    final EntityTag etag = response.getEntityTag();
    assertThat(etag).isNotNull();

    final List<RemoteConfig> configs = new ArrayList<>(remoteConfigsManager.getAll());
    configs.add(new RemoteConfig("android.new.config", 100, Set.of(), null, null, null));
    when(remoteConfigsManager.getAll()).thenReturn(configs);

    response = resources.getJerseyTest()
        .target("/v2/config/")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .header("User-Agent", "Signal-Android/7.6.2 Android/34 libsignal/0.46.0")
        .header("If-None-Match", etag)
        .get();

    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.getLength()).isPositive();
  }

  @ParameterizedTest
  @MethodSource
  void testEtag(boolean expect304, String userAgent1, String authHeader1, String userAgent2, String authHeader2) {
    // Use a deterministic config; account 1 is special, 2 and 3 are identical
    List<RemoteConfig> configs = remoteConfigsManager.getAll().stream().filter(config -> config.getPercentage() == 0 || config.getPercentage() == 100).toList();
    when(remoteConfigsManager.getAll()).thenReturn(configs);

    Response response = resources.getJerseyTest()
        .target("/v2/config/")
        .request()
        .header("Authorization", authHeader1)
        .header("User-Agent", userAgent1)
        .get();

    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.getLength()).isPositive();
    final EntityTag etag = response.getEntityTag();
    assertThat(etag).isNotNull();

    response = resources.getJerseyTest()
        .target("/v2/config/")
        .request()
        .header("Authorization", authHeader2)
        .header("User-Agent", userAgent2)
        .header("If-None-Match", etag)
        .get();

    if (expect304) {
      assertThat(response.getStatus()).isEqualTo(304);
      assertThat(response.getLength()).isNotPositive();
    } else {
      assertThat(response.getStatus()).isEqualTo(200);
      assertThat(response.getLength()).isPositive();
    }
  }

  static List<Arguments> testEtag() {
    final String uuid1AuthHeader = AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD);
    final String uuid2AuthHeader = AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_TWO, AuthHelper.VALID_PASSWORD_TWO);
    final String uuid3AuthHeader = AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_3, AuthHelper.VALID_PASSWORD_3_PRIMARY);

    final String ios762 = "Signal-iOS/7.6.2 iOS/18.5 libsignal/0.46.0";
    final String android762 = "Signal-Android/7.6.2 Android/34 libsignal/0.46.0";
    final String android763 = "Signal-Android/7.6.3 Android/34 libsignal/0.46.0";

    // boolean is expect304
    return List.of(
        Arguments.argumentSet("User change", false, android762, uuid1AuthHeader, android762, uuid2AuthHeader),
        Arguments.argumentSet("Irrelevant user change", true, android762, uuid2AuthHeader, android762, uuid3AuthHeader),
        Arguments.argumentSet("User agent change", false, android762, uuid1AuthHeader, ios762, uuid1AuthHeader),
        Arguments.argumentSet("Irrelevant user agent change", true, android762, uuid1AuthHeader, android763, uuid1AuthHeader)
    );
  }

  @ParameterizedTest
  @IntRangeSource(from = 1, to = 99)
  void testMath(int percentage) throws NoSuchAlgorithmException {
    final MessageDigest digest = MessageDigest.getInstance("SHA-256");
    final Random random = new Random(9424242L);  // the seed value doesn't matter so much as it's constant to make the test not flaky
    final int iterations = 10000;
    int enabledCount = 0;

    for (int i = 0; i < iterations; i++) {
      if (RemoteConfigController.isInBucket(digest, AuthHelper.getRandomUUID(random), "test".getBytes(), percentage, Set.of())) {
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
      if (RemoteConfigController.isInBucket(digest, AuthHelper.getRandomUUID(random), "test".getBytes(), percentage, Set.of())) {
          enabledCount++;
      }
    }

    assertThat(enabledCount).isEqualTo(iterations * percentage / 100);
  }
}
