/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.CloudflareTurnCredentialsManager;
import org.whispersystems.textsecuregcm.auth.TurnToken;
import org.whispersystems.textsecuregcm.auth.TurnTokenGenerator;
import org.whispersystems.textsecuregcm.calls.routing.TurnCallRouter;
import org.whispersystems.textsecuregcm.calls.routing.TurnServerOptions;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.mappers.RateLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.TestRemoteAddressFilterProvider;

@ExtendWith(DropwizardExtensionsSupport.class)
class CallRoutingControllerV2Test {

  private static final String GET_CALL_RELAYS_PATH = "v2/calling/relays";
  private static final String REMOTE_ADDRESS = "123.123.123.1";
  private static final TurnServerOptions TURN_SERVER_OPTIONS = new TurnServerOptions(
      "example.domain.org",
      List.of("stun:12.34.56.78"),
      List.of("stun:example.domain.org")
  );
  private static final TurnToken CLOUDFLARE_TURN_TOKEN = new TurnToken(
      "ABC",
      "XYZ",
      List.of("turn:cloudflare.example.com:3478?transport=udp"),
      null,
      "cf.example.com");

  private static final RateLimiters rateLimiters = mock(RateLimiters.class);
  private static final RateLimiter getCallEndpointLimiter = mock(RateLimiter.class);
  private static final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager = mock(
      DynamicConfigurationManager.class);
  private static final ExperimentEnrollmentManager experimentEnrollmentManager = mock(
      ExperimentEnrollmentManager.class);
  private static final TurnTokenGenerator turnTokenGenerator = new TurnTokenGenerator(dynamicConfigurationManager,
      "bloop".getBytes(StandardCharsets.UTF_8));
  private static final CloudflareTurnCredentialsManager cloudflareTurnCredentialsManager = mock(
      CloudflareTurnCredentialsManager.class);
  private static final TurnCallRouter turnCallRouter = mock(TurnCallRouter.class);

  private static final ResourceExtension resources = ResourceExtension.builder()
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(new AuthValueFactoryProvider.Binder<>(AuthenticatedDevice.class))
      .addProvider(new RateLimitExceededExceptionMapper())
      .addProvider(new TestRemoteAddressFilterProvider(REMOTE_ADDRESS))
      .setMapper(SystemMapper.jsonMapper())
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(new CallRoutingControllerV2(rateLimiters, turnCallRouter, turnTokenGenerator,
          experimentEnrollmentManager, cloudflareTurnCredentialsManager))
      .build();

  @BeforeEach
  void setup() {
    when(rateLimiters.getCallEndpointLimiter()).thenReturn(getCallEndpointLimiter);
  }

  @AfterEach
  void tearDown() {
    reset(experimentEnrollmentManager, dynamicConfigurationManager, rateLimiters, getCallEndpointLimiter,
        turnCallRouter);
  }

  void initializeMocksWith(Optional<TurnServerOptions> signalTurn, Optional<TurnToken> cloudflare) {
    signalTurn.ifPresent(options -> {
      try {
        when(turnCallRouter.getRoutingFor(
            eq(AuthHelper.VALID_UUID),
            eq(Optional.of(InetAddress.getByName(REMOTE_ADDRESS))),
            anyInt())
        ).thenReturn(options);
      } catch (UnknownHostException ignored) {
      }
    });
    cloudflare.ifPresent(token -> {
      when(experimentEnrollmentManager.isEnrolled(AuthHelper.VALID_NUMBER, AuthHelper.VALID_UUID, "cloudflareTurn"))
          .thenReturn(true);
      try {
        when(cloudflareTurnCredentialsManager.retrieveFromCloudflare()).thenReturn(token);
      } catch (IOException ignored) {
      }
    });
  }

  @Test
  void testGetRelaysSignalRoutingOnly() {
    TurnServerOptions options = TURN_SERVER_OPTIONS;
    initializeMocksWith(Optional.of(options), Optional.empty());

    try (Response rawResponse = resources.getJerseyTest()
        .target(GET_CALL_RELAYS_PATH)
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get()) {

      assertThat(rawResponse.getStatus()).isEqualTo(200);

      CallRoutingControllerV2.GetCallingRelaysResponse response = rawResponse.readEntity(
          CallRoutingControllerV2.GetCallingRelaysResponse.class);
      List<TurnToken> relays = response.relays();
      assertThat(relays).hasSize(1);
      assertThat(relays.getFirst().username()).isNotEmpty();
      assertThat(relays.getFirst().password()).isNotEmpty();
      assertThat(relays.getFirst().hostname()).isEqualTo(options.hostname());
      assertThat(relays.getFirst().urlsWithIps()).isEqualTo(options.urlsWithIps());
      assertThat(relays.getFirst().urls()).isEqualTo(options.urlsWithHostname());
    }
  }

  @Test
  void testGetRelaysBothRouting() {
    TurnServerOptions options = TURN_SERVER_OPTIONS;
    initializeMocksWith(Optional.of(options), Optional.of(CLOUDFLARE_TURN_TOKEN));

    try (Response rawResponse = resources.getJerseyTest()
        .target(GET_CALL_RELAYS_PATH)
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get()) {

      assertThat(rawResponse.getStatus()).isEqualTo(200);

      CallRoutingControllerV2.GetCallingRelaysResponse response = rawResponse.readEntity(
          CallRoutingControllerV2.GetCallingRelaysResponse.class);

      List<TurnToken> relays = response.relays();
      assertThat(relays).hasSize(2);

      assertThat(relays.getFirst()).isEqualTo(CLOUDFLARE_TURN_TOKEN);

      TurnToken token = relays.get(1);
      assertThat(token.username()).isNotEmpty();
      assertThat(token.password()).isNotEmpty();
      assertThat(token.hostname()).isEqualTo(options.hostname());
      assertThat(token.urlsWithIps()).isEqualTo(options.urlsWithIps());
      assertThat(token.urls()).isEqualTo(options.urlsWithHostname());
    }
  }

  @Test
  void testGetRelaysInvalidIpSuccess() throws UnknownHostException {
    TurnServerOptions options = new TurnServerOptions(
        "example.domain.org",
        List.of(),
        List.of("stun:example.domain.org")
    );

    when(turnCallRouter.getRoutingFor(
        eq(AuthHelper.VALID_UUID),
        eq(Optional.of(InetAddress.getByName(REMOTE_ADDRESS))),
        anyInt())
    ).thenReturn(options);
    try (Response rawResponse = resources.getJerseyTest()
        .target(GET_CALL_RELAYS_PATH)
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get()) {

      assertThat(rawResponse.getStatus()).isEqualTo(200);
      CallRoutingControllerV2.GetCallingRelaysResponse response = rawResponse.readEntity(
          CallRoutingControllerV2.GetCallingRelaysResponse.class
      );

      assertThat(response.relays()).hasSize(1);
      TurnToken token = response.relays().getFirst();
      assertThat(token.username()).isNotEmpty();
      assertThat(token.password()).isNotEmpty();
      assertThat(token.hostname()).isEqualTo(options.hostname());
      assertThat(token.urlsWithIps()).isEqualTo(options.urlsWithIps());
      assertThat(token.urls()).isEqualTo(options.urlsWithHostname());
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
