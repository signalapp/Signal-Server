/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.util.List;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.CloudflareTurnCredentialsManager;
import org.whispersystems.textsecuregcm.auth.TurnToken;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.mappers.RateLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.TestRemoteAddressFilterProvider;

@ExtendWith(DropwizardExtensionsSupport.class)
class CallRoutingControllerV2Test {

  private static final String GET_CALL_RELAYS_PATH = "v2/calling/relays";
  private static final String REMOTE_ADDRESS = "123.123.123.1";
  private static final TurnToken CLOUDFLARE_TURN_TOKEN = new TurnToken(
      "ABC",
      "XYZ",
      43_200,
      List.of("turn:cloudflare.example.com:3478?transport=udp"),
      null,
      "cf.example.com");

  private static final RateLimiters rateLimiters = mock(RateLimiters.class);
  private static final RateLimiter getCallEndpointLimiter = mock(RateLimiter.class);
  private static final CloudflareTurnCredentialsManager cloudflareTurnCredentialsManager =
      mock(CloudflareTurnCredentialsManager.class);

  private static final ResourceExtension resources = ResourceExtension.builder()
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(new AuthValueFactoryProvider.Binder<>(AuthenticatedDevice.class))
      .addProvider(new RateLimitExceededExceptionMapper())
      .addProvider(new TestRemoteAddressFilterProvider(REMOTE_ADDRESS))
      .setMapper(SystemMapper.jsonMapper())
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(new CallRoutingControllerV2(rateLimiters, cloudflareTurnCredentialsManager))
      .build();

  @BeforeEach
  void setup() {
    when(rateLimiters.getCallEndpointLimiter()).thenReturn(getCallEndpointLimiter);
  }

  @AfterEach
  void tearDown() {
    reset(rateLimiters, getCallEndpointLimiter);
  }

  @Test
  void testGetRelaysBothRouting() throws IOException {
    when(cloudflareTurnCredentialsManager.retrieveFromCloudflare()).thenReturn(CLOUDFLARE_TURN_TOKEN);

    try (final Response rawResponse = resources.getJerseyTest()
        .target(GET_CALL_RELAYS_PATH)
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get()) {

      assertThat(rawResponse.getStatus()).isEqualTo(200);

      assertThat(rawResponse.readEntity(GetCallingRelaysResponse.class).relays())
          .isEqualTo(List.of(CLOUDFLARE_TURN_TOKEN));
    }
  }

  @Test
  void testGetRelaysRateLimited() throws RateLimitExceededException {
    doThrow(new RateLimitExceededException(null))
        .when(getCallEndpointLimiter).validate(AuthHelper.VALID_UUID);

    try (final Response response = resources.getJerseyTest()
        .target(GET_CALL_RELAYS_PATH)
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get()) {

      assertThat(response.getStatus()).isEqualTo(429);
    }
  }
}
