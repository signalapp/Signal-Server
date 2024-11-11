/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.entities.ProvisioningMessage;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.mappers.RateLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.push.ProvisioningManager;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.websocket.ProvisioningConnectListener;

@ExtendWith(DropwizardExtensionsSupport.class)
class ProvisioningControllerTest {

  private RateLimiter messagesRateLimiter;

  private static final RateLimiters rateLimiters = mock(RateLimiters.class);
  private static final ProvisioningManager provisioningManager = mock(ProvisioningManager.class);

  private static final ResourceExtension RESOURCE_EXTENSION = ResourceExtension.builder()
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(new AuthValueFactoryProvider.Binder<>(AuthenticatedDevice.class))
      .addProvider(new RateLimitExceededExceptionMapper())
      .setMapper(SystemMapper.jsonMapper())
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(new ProvisioningController(rateLimiters, provisioningManager))
      .build();

  @BeforeEach
  void setUp() {
    reset(rateLimiters, provisioningManager);

    messagesRateLimiter = mock(RateLimiter.class);
    when(rateLimiters.getMessagesLimiter()).thenReturn(messagesRateLimiter);
  }

  @Test
  void sendProvisioningMessage() {
    final String provisioningAddress = ProvisioningConnectListener.generateProvisioningAddress();
    final byte[] messageBody = "test".getBytes(StandardCharsets.UTF_8);

    when(provisioningManager.sendProvisioningMessage(any(), any())).thenReturn(true);

    try (final Response response = RESOURCE_EXTENSION.getJerseyTest()
        .target("/v1/provisioning/" + provisioningAddress)
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .put(Entity.entity(new ProvisioningMessage(Base64.getMimeEncoder().encodeToString(messageBody)),
            MediaType.APPLICATION_JSON))) {

      assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());

      verify(provisioningManager).sendProvisioningMessage(provisioningAddress, messageBody);
    }
  }

  @Test
  void sendProvisioningMessageRateLimited() throws RateLimitExceededException {
    final String provisioningAddress = ProvisioningConnectListener.generateProvisioningAddress();
    final byte[] messageBody = "test".getBytes(StandardCharsets.UTF_8);

    doThrow(new RateLimitExceededException(Duration.ZERO))
        .when(messagesRateLimiter).validate(AuthHelper.VALID_UUID);

    try (final Response response = RESOURCE_EXTENSION.getJerseyTest()
        .target("/v1/provisioning/" + provisioningAddress)
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .put(Entity.entity(new ProvisioningMessage(Base64.getMimeEncoder().encodeToString(messageBody)),
            MediaType.APPLICATION_JSON))) {

      assertEquals(429, response.getStatus());

      verify(provisioningManager, never()).sendProvisioningMessage(any(), any());
    }
  }
}
