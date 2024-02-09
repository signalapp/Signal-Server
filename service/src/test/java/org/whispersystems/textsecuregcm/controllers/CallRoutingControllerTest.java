/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.net.HttpHeaders;
import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.TurnToken;
import org.whispersystems.textsecuregcm.auth.TurnTokenGenerator;
import org.whispersystems.textsecuregcm.calls.routing.TurnCallRouter;
import org.whispersystems.textsecuregcm.calls.routing.TurnServerOptions;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.mappers.RateLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.TestRemoteAddressFilterProvider;

@ExtendWith(DropwizardExtensionsSupport.class)
class CallRoutingControllerTest {
  private static final RateLimiters rateLimiters = mock(RateLimiters.class);
  private static final RateLimiter getCallEndpointLimiter = mock(RateLimiter.class);
  private static final DynamicConfigurationManager<DynamicConfiguration> configManager =  mock(DynamicConfigurationManager.class);
  private static final TurnTokenGenerator turnTokenGenerator = new TurnTokenGenerator(configManager, "bloop".getBytes(
      StandardCharsets.UTF_8));
  private static final TurnCallRouter turnCallRouter = mock(TurnCallRouter.class);
  private static final String GET_CALL_ENDPOINTS_PATH = "v1/calling/relays";
  private static final String REMOTE_ADDRESS = "123.123.123.1";

  private static final ResourceExtension resources  = ResourceExtension.builder()
        .addProvider(AuthHelper.getAuthFilter())
        .addProvider(new AuthValueFactoryProvider.Binder<>(AuthenticatedAccount.class))
        .addProvider(new RateLimitExceededExceptionMapper())
        .addProvider(new TestRemoteAddressFilterProvider(REMOTE_ADDRESS))
        .setMapper(SystemMapper.jsonMapper())
        .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
        .addResource(new CallRoutingController(rateLimiters, turnCallRouter, turnTokenGenerator))
        .build();

  @BeforeEach
  void setup() {
    when(rateLimiters.getCallEndpointLimiter()).thenReturn(getCallEndpointLimiter);
  }

  @Test
  void testGetTurnEndpointsSuccess() throws UnknownHostException {
    TurnServerOptions options = new TurnServerOptions(
        "example.domain.org",
        List.of("stun:12.34.56.78"),
        List.of("stun:example.domain.org")
    );

    when(turnCallRouter.getRoutingFor(
        eq(AuthHelper.VALID_UUID),
        eq(Optional.of(InetAddress.getByName(REMOTE_ADDRESS))),
        anyInt())
    ).thenReturn(options);
    try(Response response = resources.getJerseyTest()
        .target(GET_CALL_ENDPOINTS_PATH)
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get()) {

      assertThat(response.getStatus()).isEqualTo(200);
      TurnToken token = response.readEntity(TurnToken.class);
      assertThat(token.username()).isNotEmpty();
      assertThat(token.password()).isNotEmpty();
      assertThat(token.hostname()).isEqualTo(options.hostname());
      assertThat(token.urlsWithIps()).isEqualTo(options.urlsWithIps());
      assertThat(token.urls()).isEqualTo(options.urlsWithHostname());
    }
  }

  @Test
  void testGetTurnEndpointsInvalidIpSuccess() throws UnknownHostException {
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
    try(Response response = resources.getJerseyTest()
        .target(GET_CALL_ENDPOINTS_PATH)
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get()) {

      assertThat(response.getStatus()).isEqualTo(200);
      TurnToken token = response.readEntity(TurnToken.class);
      assertThat(token.username()).isNotEmpty();
      assertThat(token.password()).isNotEmpty();
      assertThat(token.hostname()).isEqualTo(options.hostname());
      assertThat(token.urlsWithIps()).isEqualTo(options.urlsWithIps());
      assertThat(token.urls()).isEqualTo(options.urlsWithHostname());
    }
  }

  @Test
  void testGetTurnEndpointRateLimited() throws RateLimitExceededException {
    doThrow(new RateLimitExceededException(null, false))
        .when(getCallEndpointLimiter).validate(AuthHelper.VALID_UUID);

    try(final Response response = resources.getJerseyTest()
        .target(GET_CALL_ENDPOINTS_PATH)
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get()) {

      assertThat(response.getStatus()).isEqualTo(429);
    }
  }
}
