package org.whispersystems.textsecuregcm.controllers;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import io.dropwizard.auth.PolymorphicAuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.UUID;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.DisabledPermittedAuthenticatedAccount;
import org.whispersystems.textsecuregcm.entities.ProvisioningMessage;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.mappers.RateLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.push.ProvisioningManager;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.websocket.ProvisioningAddress;

@ExtendWith(DropwizardExtensionsSupport.class)
class ProvisioningControllerTest {

  private RateLimiter messagesRateLimiter;

  private static final RateLimiters rateLimiters = mock(RateLimiters.class);
  private static final ProvisioningManager provisioningManager = mock(ProvisioningManager.class);

  private static final ResourceExtension RESOURCE_EXTENSION = ResourceExtension.builder()
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(new PolymorphicAuthValueFactoryProvider.Binder<>(
          ImmutableSet.of(AuthenticatedAccount.class, DisabledPermittedAuthenticatedAccount.class)))
      .addProvider(new RateLimitExceededExceptionMapper())
      .setMapper(SystemMapper.getMapper())
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
    final String destination = UUID.randomUUID().toString();
    final byte[] messageBody = "test".getBytes(StandardCharsets.UTF_8);

    when(provisioningManager.sendProvisioningMessage(any(), any())).thenReturn(true);

    try (final Response response = RESOURCE_EXTENSION.getJerseyTest()
        .target("/v1/provisioning/" + destination)
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .put(Entity.entity(new ProvisioningMessage(Base64.getMimeEncoder().encodeToString(messageBody)),
            MediaType.APPLICATION_JSON))) {

      assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());

      final ArgumentCaptor<ProvisioningAddress> provisioningAddressCaptor =
          ArgumentCaptor.forClass(ProvisioningAddress.class);

      final ArgumentCaptor<byte[]> provisioningMessageCaptor = ArgumentCaptor.forClass(byte[].class);

      verify(provisioningManager).sendProvisioningMessage(provisioningAddressCaptor.capture(),
          provisioningMessageCaptor.capture());

      assertEquals(destination, provisioningAddressCaptor.getValue().getAddress());
      assertEquals(0, provisioningAddressCaptor.getValue().getDeviceId());

      assertArrayEquals(messageBody, provisioningMessageCaptor.getValue());
    }
  }

  @Test
  void sendProvisioningMessageRateLimited() throws RateLimitExceededException {
    final String destination = UUID.randomUUID().toString();
    final byte[] messageBody = "test".getBytes(StandardCharsets.UTF_8);

    doThrow(new RateLimitExceededException(Duration.ZERO, true))
        .when(messagesRateLimiter).validate(AuthHelper.VALID_UUID);

    try (final Response response = RESOURCE_EXTENSION.getJerseyTest()
        .target("/v1/provisioning/" + destination)
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .put(Entity.entity(new ProvisioningMessage(Base64.getMimeEncoder().encodeToString(messageBody)),
            MediaType.APPLICATION_JSON))) {

      assertEquals(413, response.getStatus());

      verify(provisioningManager, never()).sendProvisioningMessage(any(), any());
    }
  }
}
