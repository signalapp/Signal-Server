/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import jakarta.ws.rs.core.EntityTag;
import jakarta.ws.rs.core.Response;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.mappers.DeviceLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.storage.RemoteConfigsManager;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;

@ExtendWith(DropwizardExtensionsSupport.class)
class RemoteConfigControllerTest {

  private static final RemoteConfigsManager remoteConfigsManager = mock(RemoteConfigsManager.class);

  private static final ResourceExtension resources = ResourceExtension.builder()
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(new AuthValueFactoryProvider.Binder<>(AuthenticatedDevice.class))
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addProvider(new DeviceLimitExceededExceptionMapper())
      .addResource(new RemoteConfigController(remoteConfigsManager))
      .build();

  @BeforeEach
  void setUp() {
    reset(remoteConfigsManager);
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
    final String userAgent = "user-agent";
    when(remoteConfigsManager.getConfigForAccount(AuthHelper.VALID_UUID, userAgent))
        .thenReturn(Map.of("test.test", "bar", "global.test", "false"));

    Response response = resources.getJerseyTest()
        .target("/v2/config/")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .header("User-Agent", userAgent)
        .get();

    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.getLength()).isPositive();
    final EntityTag etag = response.getEntityTag();
    assertThat(etag).isNotNull();

    response = resources.getJerseyTest()
        .target("/v2/config/")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .header("User-Agent", userAgent)
        .header("If-None-Match", etag)
        .get();

    assertThat(response.getStatus()).isEqualTo(304);
    assertThat(response.getLength()).isNotPositive();
  }

  @Test
  void testRetrieveConfigChanged() {
    final String userAgent = "Signal-Android/7.6.2 Android/34 libsignal/0.46.0";

    final Map<String, String> config1 = Map.of(
        "android.stickers", "foo",
        "test.test", "bar",
        "global.test", "false");

    when(remoteConfigsManager.getConfigForAccount(AuthHelper.VALID_UUID, userAgent)).thenReturn(config1);

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


    final Map<String, String> config2 = new HashMap<>(config1);
    config2.put("android.new.config", "true");
    when(remoteConfigsManager.getConfigForAccount(AuthHelper.VALID_UUID, userAgent)).thenReturn(config2);

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
  void testEtag(boolean expect304, final Map<String, String> config1, Map<String, String> config2) {
    final String ua1 = "user-agent-1";
    final String ua2 = "user-agent-2";

    when(remoteConfigsManager.getConfigForAccount(AuthHelper.VALID_UUID, ua1)).thenReturn(config1);
    when(remoteConfigsManager.getConfigForAccount(AuthHelper.VALID_UUID_TWO, ua2)).thenReturn(config2);

    Response response = resources.getJerseyTest()
        .target("/v2/config/")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .header("User-Agent", ua1)
        .get();

    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.getLength()).isPositive();
    final EntityTag etag = response.getEntityTag();
    assertThat(etag).isNotNull();

    response = resources.getJerseyTest()
        .target("/v2/config/")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_TWO, AuthHelper.VALID_PASSWORD_TWO))
        .header("User-Agent", ua2)
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
    final Map<String, String> config1 = Map.of(
        "android.stickers", "foo",
        "test.test", "bar",
        "global.test", "false");

    final Map<String, String> config2 = Map.of(
        "android.stickers", "foo",
        "test.test", "bar",
        "global.test", "true");


    return List.of(
        Arguments.argumentSet("same", true, config1, config1),
        Arguments.argumentSet("change", false, config1, config2),
        Arguments.argumentSet("different sort", true,
            new TreeMap<>(config1),
            config1.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a,_) -> a, () -> new TreeMap<String, String>(Comparator.reverseOrder()))))
    );
  }
}
