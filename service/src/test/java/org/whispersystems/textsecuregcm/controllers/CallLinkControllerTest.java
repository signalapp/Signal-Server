/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.signal.libsignal.protocol.util.Hex;
import org.signal.libsignal.zkgroup.GenericServerSecretParams;
import org.signal.libsignal.zkgroup.calllinks.CreateCallLinkCredentialRequestContext;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.entities.GetCreateCallLinkCredentialsRequest;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.mappers.RateLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.SystemMapper;

@ExtendWith(DropwizardExtensionsSupport.class)
public class CallLinkControllerTest {
  private static final GenericServerSecretParams genericServerSecretParams = GenericServerSecretParams.generate();
  private static final RateLimiters rateLimiters = mock(RateLimiters.class);
  private static final RateLimiter createCallLinkLimiter = mock(RateLimiter.class);
  private static final byte[] roomId = Hex.fromStringCondensedAssert("c8c9cacbcccdcecfd0d1d2d3d4d5d6d7d8d9dadbdcdddedfe0e1e2e3e4e5e6e7");
  private static final CreateCallLinkCredentialRequestContext createCallLinkRequestContext = CreateCallLinkCredentialRequestContext.forRoom(roomId);
  private static final byte[] createCallLinkRequestSerialized = createCallLinkRequestContext.getRequest().serialize();

  private static final ResourceExtension resources = ResourceExtension.builder()
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(new AuthValueFactoryProvider.Binder<>(AuthenticatedDevice.class))
      .addProvider(new RateLimitExceededExceptionMapper())
      .setMapper(SystemMapper.jsonMapper())
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(new CallLinkController(rateLimiters, genericServerSecretParams))
      .build();

  @BeforeEach
  void setup() {
    when(rateLimiters.getCreateCallLinkLimiter()).thenReturn(createCallLinkLimiter);
  }

  @Test
  void testGetCreateAuth() {
    try (Response response = resources.getJerseyTest()
        .target("/v1/call-link/create-auth")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .post(Entity.json(new GetCreateCallLinkCredentialsRequest(createCallLinkRequestSerialized)))) {
      assertThat(response.getStatus()).isEqualTo(200);
    }
  }

  @Test
  void testGetCreateAuthInvalidInput() {
    try (Response response = resources.getJerseyTest()
        .target("/v1/call-link/create-auth")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .post(Entity.json(new GetCreateCallLinkCredentialsRequest(new byte[10])))) {
      assertThat(response.getStatus()).isEqualTo(400);
    }
  }

  @Test
  void testGetCreateAuthInvalidAuth() {
    try (Response response = resources.getJerseyTest()
        .target("/v1/call-link/create-auth")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.INVALID_UUID, AuthHelper.VALID_PASSWORD))
        .post(Entity.json(new GetCreateCallLinkCredentialsRequest(createCallLinkRequestSerialized)))) {
      assertThat(response.getStatus()).isEqualTo(401);
    }
  }

  @Test
  void testGetCreateAuthInvalidRequest() {
    try (Response response = resources.getJerseyTest()
        .target("/v1/call-link/create-auth")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .post(Entity.json(""))) {

      assertThat(response.getStatus()).isEqualTo(422);
    }
  }

  @Test
  void testGetCreateAuthInvalidInputEmptyRequestBody() {
    try (Response response = resources.getJerseyTest()
        .target("/v1/call-link/create-auth")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .post(Entity.json("{}"))) {
      assertThat(response.getStatus()).isEqualTo(422);
    }
  }

  @Test
  void testGetCreateAuthInvalidInputEmptyField() {
    try (Response response = resources.getJerseyTest()
        .target("/v1/call-link/create-auth")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .post(Entity.json("{\"createCallLinkCredentialRequest\": \"\"}"))) {
      assertThat(response.getStatus()).isEqualTo(422);
    }
  }

  @Test
  void testGetCreateAuthRatelimited() throws RateLimitExceededException{
    doThrow(new RateLimitExceededException(null))
        .when(createCallLinkLimiter).validate(AuthHelper.VALID_UUID);

    try (Response response = resources.getJerseyTest()
        .target("/v1/call-link/create-auth")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .post(Entity.json(new GetCreateCallLinkCredentialsRequest(createCallLinkRequestSerialized)))) {

      assertThat(response.getStatus()).isEqualTo(429);
    }
  }
}
