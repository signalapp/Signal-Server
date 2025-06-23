/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import jakarta.ws.rs.core.Response;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.entities.UserRemoteConfig;
import org.whispersystems.textsecuregcm.entities.UserRemoteConfigList;
import org.whispersystems.textsecuregcm.mappers.DeviceLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.storage.RemoteConfig;
import org.whispersystems.textsecuregcm.storage.RemoteConfigsManager;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.TestClock;

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
    when(remoteConfigsManager.getAll()).thenReturn(new LinkedList<>() {{
      add(new RemoteConfig("android.stickers", 25, Set.of(AuthHelper.VALID_UUID_3, AuthHelper.INVALID_UUID), null,
          null, null));
      add(new RemoteConfig("ios.stickers", 50, Set.of(), null, null, null));
      add(new RemoteConfig("always.true", 100, Set.of(), null, null, null));
      add(new RemoteConfig("only.special", 0, Set.of(AuthHelper.VALID_UUID), null, null, null));
      add(new RemoteConfig("value.always.true", 100, Set.of(), "foo", "bar", null));
      add(new RemoteConfig("value.only.special", 0, Set.of(AuthHelper.VALID_UUID), "abc", "xyz", null));
      add(new RemoteConfig("value.always.false", 0, Set.of(), "red", "green", null));
      add(new RemoteConfig("linked.config.0", 50, Set.of(), null, null, null));
      add(new RemoteConfig("linked.config.1", 50, Set.of(), null, null, "linked.config.0"));
      add(new RemoteConfig("unlinked.config", 50, Set.of(), null, null, null));
    }});

  }

  @AfterEach
  void teardown() {
    reset(remoteConfigsManager);
  }

  @Test
  void testRetrieveConfig() {
    UserRemoteConfigList configuration = resources.getJerseyTest()
                                                  .target("/v1/config/")
                                                  .request()
                                                  .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                                                  .get(UserRemoteConfigList.class);

    verify(remoteConfigsManager, times(1)).getAll();

    assertThat(configuration.getConfig()).hasSize(11);
    assertThat(configuration.getConfig().get(0).getName()).isEqualTo("android.stickers");
    assertThat(configuration.getConfig().get(1).getName()).isEqualTo("ios.stickers");
    assertThat(configuration.getConfig().get(2).getName()).isEqualTo("always.true");
    assertThat(configuration.getConfig().get(2).isEnabled()).isEqualTo(true);
    assertThat(configuration.getConfig().get(2).getValue()).isNull();
    assertThat(configuration.getConfig().get(3).getName()).isEqualTo("only.special");
    assertThat(configuration.getConfig().get(3).isEnabled()).isEqualTo(true);
    assertThat(configuration.getConfig().get(2).getValue()).isNull();
    assertThat(configuration.getConfig().get(4).getName()).isEqualTo("value.always.true");
    assertThat(configuration.getConfig().get(4).isEnabled()).isEqualTo(true);
    assertThat(configuration.getConfig().get(4).getValue()).isEqualTo("bar");
    assertThat(configuration.getConfig().get(5).getName()).isEqualTo("value.only.special");
    assertThat(configuration.getConfig().get(5).isEnabled()).isEqualTo(true);
    assertThat(configuration.getConfig().get(5).getValue()).isEqualTo("xyz");
    assertThat(configuration.getConfig().get(6).getName()).isEqualTo("value.always.false");
    assertThat(configuration.getConfig().get(6).isEnabled()).isEqualTo(false);
    assertThat(configuration.getConfig().get(6).getValue()).isEqualTo("red");
    assertThat(configuration.getConfig().get(7).getName()).isEqualTo("linked.config.0");
    assertThat(configuration.getConfig().get(8).getName()).isEqualTo("linked.config.1");
    assertThat(configuration.getConfig().get(9).getName()).isEqualTo("unlinked.config");
    assertThat(configuration.getConfig().get(10).getName()).isEqualTo("global.maxGroupSize");
  }

  @Test
  void testServerEpochTime() {
    Object serverEpochTime = resources.getJerseyTest()
        .target("/v1/config/")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(Map.class)
        .get("serverEpochTime");

    assertThat(serverEpochTime).asInstanceOf(new InstanceOfAssertFactory<>(Number.class, Assertions::assertThat))
        .extracting(Number::longValue)
        .isEqualTo(PINNED_EPOCH_SECONDS);
  }

  @Test
  void testRetrieveConfigNotSpecial() {
    UserRemoteConfigList configuration = resources.getJerseyTest()
                                                  .target("/v1/config/")
                                                  .request()
                                                  .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_TWO, AuthHelper.VALID_PASSWORD_TWO))
                                                  .get(UserRemoteConfigList.class);

    verify(remoteConfigsManager, times(1)).getAll();

    assertThat(configuration.getConfig()).hasSize(11);
    assertThat(configuration.getConfig().get(0).getName()).isEqualTo("android.stickers");
    assertThat(configuration.getConfig().get(1).getName()).isEqualTo("ios.stickers");
    assertThat(configuration.getConfig().get(2).getName()).isEqualTo("always.true");
    assertThat(configuration.getConfig().get(2).isEnabled()).isEqualTo(true);
    assertThat(configuration.getConfig().get(2).getValue()).isNull();
    assertThat(configuration.getConfig().get(3).getName()).isEqualTo("only.special");
    assertThat(configuration.getConfig().get(3).isEnabled()).isEqualTo(false);
    assertThat(configuration.getConfig().get(2).getValue()).isNull();
    assertThat(configuration.getConfig().get(4).getName()).isEqualTo("value.always.true");
    assertThat(configuration.getConfig().get(4).isEnabled()).isEqualTo(true);
    assertThat(configuration.getConfig().get(4).getValue()).isEqualTo("bar");
    assertThat(configuration.getConfig().get(5).getName()).isEqualTo("value.only.special");
    assertThat(configuration.getConfig().get(5).isEnabled()).isEqualTo(false);
    assertThat(configuration.getConfig().get(5).getValue()).isEqualTo("abc");
    assertThat(configuration.getConfig().get(6).getName()).isEqualTo("value.always.false");
    assertThat(configuration.getConfig().get(6).isEnabled()).isEqualTo(false);
    assertThat(configuration.getConfig().get(6).getValue()).isEqualTo("red");
    assertThat(configuration.getConfig().get(7).getName()).isEqualTo("linked.config.0");
    assertThat(configuration.getConfig().get(8).getName()).isEqualTo("linked.config.1");
    assertThat(configuration.getConfig().get(9).getName()).isEqualTo("unlinked.config");
    assertThat(configuration.getConfig().get(10).getName()).isEqualTo("global.maxGroupSize");
  }

  @Test
  void testHashKeyLinkedConfigs() {
    boolean allUnlinkedConfigsMatched = true;
    for (AuthHelper.TestAccount testAccount : AuthHelper.TEST_ACCOUNTS) {
      UserRemoteConfigList configuration = resources.getJerseyTest()
          .target("/v1/config/")
          .request()
          .header("Authorization", testAccount.getAuthHeader())
          .get(UserRemoteConfigList.class);

      assertThat(configuration.getConfig()).hasSize(11);

      final UserRemoteConfig linkedConfig0  = configuration.getConfig().get(7);
      assertThat(linkedConfig0.getName()).isEqualTo("linked.config.0");

      final UserRemoteConfig linkedConfig1  = configuration.getConfig().get(8);
      assertThat(linkedConfig1.getName()).isEqualTo("linked.config.1");

      final UserRemoteConfig unlinkedConfig = configuration.getConfig().get(9);
      assertThat(unlinkedConfig.getName()).isEqualTo("unlinked.config");

      assertThat(linkedConfig0.isEnabled() == linkedConfig1.isEnabled()).isTrue();
      allUnlinkedConfigsMatched &= (linkedConfig0.isEnabled() == unlinkedConfig.isEnabled());
    }
    assertThat(allUnlinkedConfigsMatched).isFalse();
  }

  @Test
  void testRetrieveConfigUnauthorized() {
    Response response = resources.getJerseyTest()
        .target("/v1/config/")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.INVALID_PASSWORD))
        .get();

    assertThat(response.getStatus()).isEqualTo(401);

    verifyNoMoreInteractions(remoteConfigsManager);
  }

  @Test
  void testMath() throws NoSuchAlgorithmException {
    List<RemoteConfig>   remoteConfigList = remoteConfigsManager.getAll();
    Map<String, Integer> enabledMap       = new HashMap<>();
    MessageDigest        digest           = MessageDigest.getInstance("SHA1");
    int                  iterations       = 100000;
    Random               random           = new Random(9424242L);  // the seed value doesn't matter so much as it's constant to make the test not flaky

    for (int i=0;i<iterations;i++) {
      for (RemoteConfig config : remoteConfigList) {
        int count  = enabledMap.getOrDefault(config.getName(), 0);

        if (RemoteConfigController.isInBucket(digest, AuthHelper.getRandomUUID(random), config.getName().getBytes(), config.getPercentage(), new HashSet<>())) {
          count++;
        }

        enabledMap.put(config.getName(), count);
      }
    }

    for (RemoteConfig config : remoteConfigList) {
      double targetNumber = iterations * (config.getPercentage() / 100.0);
      double variance = targetNumber * 0.01;

      assertThat(enabledMap.get(config.getName())).isBetween((int) (targetNumber - variance),
          (int) (targetNumber + variance));
    }
  }

}
