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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.signal.libsignal.protocol.ServiceId;
import org.signal.libsignal.protocol.util.Hex;
import org.signal.libsignal.zkgroup.GenericServerSecretParams;
import org.signal.libsignal.zkgroup.calllinks.CallLinkSecretParams;
import org.signal.libsignal.zkgroup.calllinks.CreateCallLinkCredential;
import org.signal.libsignal.zkgroup.calllinks.CreateCallLinkCredentialPresentation;
import org.signal.libsignal.zkgroup.calllinks.CreateCallLinkCredentialRequestContext;
import org.signal.libsignal.zkgroup.calllinks.CreateCallLinkCredentialResponse;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.entities.GetCreateCallLinkCredentialsRequest;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.mappers.RateLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

@ExtendWith(DropwizardExtensionsSupport.class)
public class CallLinkControllerTest {
  private static final GenericServerSecretParams genericServerSecretParams = GenericServerSecretParams.generate();
  private static final GenericServerSecretParams genericServerSecretParamsPreV101 = GenericServerSecretParams.generate();
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
      .addResource(new CallLinkController(rateLimiters, genericServerSecretParams, genericServerSecretParamsPreV101))
      .build();

  @BeforeEach
  void setup() {
    when(rateLimiters.getCreateCallLinkLimiter()).thenReturn(createCallLinkLimiter);
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(booleans = {true, false})
  void testGetCreateAuth(final Boolean v101) throws Exception {
    try (Response response = resources.getJerseyTest()
        .target("/v1/call-link/create-auth")
        // a null value will exclude the parameter altogether, rather than sending `?v101=`
        .queryParam("v101", v101)
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .post(Entity.json(new GetCreateCallLinkCredentialsRequest(createCallLinkRequestSerialized)))) {
      assertThat(response.getStatus()).isEqualTo(200);

      final GenericServerSecretParams verificationParams = v101 != null && v101
          ? genericServerSecretParams
          : genericServerSecretParamsPreV101;

      final byte[] serializedCredential = response.readEntity(
          org.whispersystems.textsecuregcm.entities.CreateCallLinkCredential.class).credential();
      final CreateCallLinkCredentialResponse credentialResponse = new CreateCallLinkCredentialResponse(serializedCredential);

      final ServiceId.Aci aci = new ServiceId.Aci(AuthHelper.VALID_UUID);
      final CreateCallLinkCredential credential = createCallLinkRequestContext.receiveResponse(credentialResponse, aci,
          verificationParams.getPublicParams());
      final CallLinkSecretParams callLinkSecretParams = CallLinkSecretParams.deriveFromRootKey(TestRandomUtil.nextBytes(16));

      final CreateCallLinkCredentialPresentation presentation = credential.present(roomId,
          aci, verificationParams.getPublicParams(), callLinkSecretParams);

      presentation.verify(roomId, verificationParams, callLinkSecretParams.getPublicParams());
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
