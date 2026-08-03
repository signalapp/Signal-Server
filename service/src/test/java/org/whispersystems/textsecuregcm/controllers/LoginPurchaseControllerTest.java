/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;
import java.util.stream.Stream;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.ServerSecretParams;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.receipts.ClientZkReceiptOperations;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialRequest;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialRequestContext;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialResponse;
import org.signal.libsignal.zkgroup.receipts.ReceiptSerial;
import org.signal.libsignal.zkgroup.receipts.ServerZkReceiptOperations;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicLoginPurchaseConfiguration;
import org.whispersystems.textsecuregcm.mappers.CompletionExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.RateLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.SubscriptionExceptionMapper;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.subscriptions.ChargeFailure;
import org.whispersystems.textsecuregcm.subscriptions.LoginPurchaseManager;
import org.whispersystems.textsecuregcm.subscriptions.PaymentProvider;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionChargeFailurePaymentRequiredException;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionInvalidArgumentsException;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionNotFoundException;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionPaymentRequiredException;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionReceiptAlreadyRedeemedException;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionReceiptRequestedForOpenPaymentException;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

@ExtendWith(DropwizardExtensionsSupport.class)
class LoginPurchaseControllerTest {

  private static final String PURCHASE_ID = "purchaseId";

  private static final ServerSecretParams SERVER_SECRET_PARAMS = ServerSecretParams.generate();
  private static final ClientZkReceiptOperations CLIENT_ZK_OPS =
      new ClientZkReceiptOperations(SERVER_SECRET_PARAMS.getPublicParams());
  private static final ServerZkReceiptOperations SERVER_ZK_OPS =
      new ServerZkReceiptOperations(SERVER_SECRET_PARAMS);

  private static final LoginPurchaseManager LOGIN_PURCHASE_MANAGER = mock(LoginPurchaseManager.class);

  private static final DynamicLoginPurchaseConfiguration ENABLED = new DynamicLoginPurchaseConfiguration(true);
  private static final DynamicLoginPurchaseConfiguration DISABLED = new DynamicLoginPurchaseConfiguration(false);

  @SuppressWarnings("unchecked")
  private static final DynamicConfigurationManager<DynamicConfiguration> DYNAMIC_CONFIGURATION_MANAGER =
      mock(DynamicConfigurationManager.class);
  private static final DynamicConfiguration DYNAMIC_CONFIGURATION = mock(DynamicConfiguration.class);

  private static final ResourceExtension RESOURCE_EXTENSION = ResourceExtension.builder()
      .addProperty(ServerProperties.UNWRAP_COMPLETION_STAGE_IN_WRITER_ENABLE, Boolean.TRUE)
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(CompletionExceptionMapper.class)
      .addProvider(new AuthValueFactoryProvider.Binder<>(AuthenticatedDevice.class))
      .addProvider(SubscriptionExceptionMapper.class)
      .addProvider(RateLimitExceededExceptionMapper.class)
      .setMapper(SystemMapper.jsonMapper())
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(new LoginPurchaseController(LOGIN_PURCHASE_MANAGER, DYNAMIC_CONFIGURATION_MANAGER))
      .build();

  private ReceiptCredentialRequestContext receiptCredentialRequestContext;

  @BeforeEach
  void setUp() throws InvalidInputException, VerificationFailedException {
    reset(LOGIN_PURCHASE_MANAGER, DYNAMIC_CONFIGURATION_MANAGER, DYNAMIC_CONFIGURATION);

    when(DYNAMIC_CONFIGURATION_MANAGER.getConfiguration()).thenReturn(DYNAMIC_CONFIGURATION);
    when(DYNAMIC_CONFIGURATION.getLoginPurchaseConfiguration()).thenReturn(ENABLED);

    receiptCredentialRequestContext = CLIENT_ZK_OPS.createReceiptCredentialRequestContext(
        new ReceiptSerial(TestRandomUtil.nextBytes(ReceiptSerial.SIZE)));
  }

  @Test
  void createReceiptCredential() throws Exception {
    final ReceiptCredentialResponse receiptCredentialResponse =
        SERVER_ZK_OPS.issueReceiptCredential(receiptCredentialRequestContext.getRequest(), 0L, 200L);

    when(LOGIN_PURCHASE_MANAGER.generateReceipt(any(), any(), any())).thenReturn(receiptCredentialResponse);

    try (final Response response = RESOURCE_EXTENSION
        .target("/v1/login-purchase/receipt_credentials")
        .request()
        .post(Entity.json(new LoginPurchaseController.CreateLoginReceiptCredentialRequest(
            PURCHASE_ID,
            receiptCredentialRequestContext.getRequest().serialize(),
            PaymentProvider.APPLE_APP_STORE)))) {
      assertThat(response.getStatus()).isEqualTo(200);
      assertThat(response.readEntity(LoginPurchaseController.CreateLoginReceiptCredentialResponse.class)
          .receiptCredentialResponse()).isEqualTo(receiptCredentialResponse.serialize());
    }

    verify(LOGIN_PURCHASE_MANAGER).generateReceipt(
        eq(PaymentProvider.APPLE_APP_STORE),
        eq(PURCHASE_ID),
        any(ReceiptCredentialRequest.class));
  }

  @Test
  void createReceiptCredentialAuthenticated() {
    try (final Response response = RESOURCE_EXTENSION.target("/v1/login-purchase/receipt_credentials")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .post(Entity.json(new LoginPurchaseController.CreateLoginReceiptCredentialRequest(
            PURCHASE_ID,
            receiptCredentialRequestContext.getRequest().serialize(),
            PaymentProvider.APPLE_APP_STORE)))) {

      assertThat(response.getStatus()).isEqualTo(403);
    }

    verifyNoInteractions(LOGIN_PURCHASE_MANAGER);
  }

  @Test
  void createReceiptCredentialNotEnabled() {
    when(DYNAMIC_CONFIGURATION.getLoginPurchaseConfiguration()).thenReturn(DISABLED);

    try (final Response response = RESOURCE_EXTENSION.target("/v1/login-purchase/receipt_credentials")
        .request()
        .post(Entity.json(new LoginPurchaseController.CreateLoginReceiptCredentialRequest(
            PURCHASE_ID,
            receiptCredentialRequestContext.getRequest().serialize(),
            PaymentProvider.APPLE_APP_STORE)))) {

      assertThat(response.getStatus()).isEqualTo(400);
    }

    verifyNoInteractions(LOGIN_PURCHASE_MANAGER);
  }

  static Stream<Arguments> createReceiptCredentialErrors() {
    return Stream.of(
        Arguments.of(new SubscriptionReceiptRequestedForOpenPaymentException(), 204),
        Arguments.of(new SubscriptionPaymentRequiredException(), 402),
        Arguments.of(new SubscriptionNotFoundException(), 404),
        Arguments.of(new SubscriptionInvalidArgumentsException("test"), 400),
        Arguments.of(new SubscriptionReceiptAlreadyRedeemedException(), 409),
        Arguments.of(new VerificationFailedException(), 400),
        Arguments.of(new RateLimitExceededException(null), 429));
  }

  @ParameterizedTest
  @MethodSource
  void createReceiptCredentialErrors(final Exception exception, final int expectedStatus) throws Exception {
    when(LOGIN_PURCHASE_MANAGER.generateReceipt(any(), any(), any())).thenThrow(exception);

    try (final Response response = RESOURCE_EXTENSION.target("/v1/login-purchase/receipt_credentials")
        .request()
        .post(Entity.json(new LoginPurchaseController.CreateLoginReceiptCredentialRequest(
            PURCHASE_ID,
            receiptCredentialRequestContext.getRequest().serialize(),
            PaymentProvider.APPLE_APP_STORE)))) {

      assertThat(response.getStatus()).isEqualTo(expectedStatus);
    }
  }

  @Test
  void createReceiptCredentialPaymentRequiredWithChargeFailure() throws Exception {
    final ChargeFailure chargeFailure =
        new ChargeFailure("generic_decline", "some failure message", null, null, null);
    when(LOGIN_PURCHASE_MANAGER.generateReceipt(any(), any(), any()))
        .thenThrow(new SubscriptionChargeFailurePaymentRequiredException(PaymentProvider.APPLE_APP_STORE, chargeFailure));

    try (final Response response = RESOURCE_EXTENSION.target("/v1/login-purchase/receipt_credentials")
        .request()
        .post(Entity.json(new LoginPurchaseController.CreateLoginReceiptCredentialRequest(
            PURCHASE_ID,
            receiptCredentialRequestContext.getRequest().serialize(),
            PaymentProvider.APPLE_APP_STORE)))) {
      final SubscriptionExceptionMapper.ChargeFailureResponse failureResponse =
          response.readEntity(SubscriptionExceptionMapper.ChargeFailureResponse.class);
      assertThat(failureResponse.chargeFailure()).isEqualTo(chargeFailure);
    }
  }
}
