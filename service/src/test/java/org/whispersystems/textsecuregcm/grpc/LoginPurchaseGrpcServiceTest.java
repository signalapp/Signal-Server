/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import java.util.stream.Stream;
import io.grpc.Status;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.signal.chat.purchase.CreateLoginReceiptCredentialRequest;
import org.signal.chat.purchase.CreateLoginReceiptCredentialResponse;
import org.signal.chat.purchase.LoginPurchaseGrpc;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.ServerSecretParams;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.receipts.ClientZkReceiptOperations;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialRequest;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialRequestContext;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialResponse;
import org.signal.libsignal.zkgroup.receipts.ReceiptSerial;
import org.signal.libsignal.zkgroup.receipts.ServerZkReceiptOperations;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicLoginPurchaseConfiguration;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
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
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

class LoginPurchaseGrpcServiceTest extends
    SimpleBaseGrpcTest<LoginPurchaseGrpcService, LoginPurchaseGrpc.LoginPurchaseBlockingStub> {

  private static final String PURCHASE_ID = "purchaseId";

  private static final ServerSecretParams SERVER_SECRET_PARAMS = ServerSecretParams.generate();
  private static final ClientZkReceiptOperations CLIENT_ZK_OPS =
      new ClientZkReceiptOperations(SERVER_SECRET_PARAMS.getPublicParams());
  private static final ServerZkReceiptOperations SERVER_ZK_OPS =
      new ServerZkReceiptOperations(SERVER_SECRET_PARAMS);

  @Mock
  private LoginPurchaseManager loginPurchaseManager;

  @Mock
  private DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager;

  @Mock
  private DynamicConfiguration dynamicConfiguration;

  @Override
  protected LoginPurchaseGrpcService createServiceBeforeEachTest() {
    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);
    when(dynamicConfiguration.getLoginPurchaseConfiguration())
        .thenReturn(new DynamicLoginPurchaseConfiguration(true));
    return new LoginPurchaseGrpcService(loginPurchaseManager, dynamicConfigurationManager);
  }

  private static ReceiptCredentialRequestContext receiptContext() {
    try {
      return CLIENT_ZK_OPS.createReceiptCredentialRequestContext(new ReceiptSerial(TestRandomUtil.nextBytes(ReceiptSerial.SIZE)));
    } catch (VerificationFailedException | InvalidInputException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void createReceiptCredential() throws Exception {
    final ReceiptCredentialRequestContext context = receiptContext();
    final ReceiptCredentialResponse receiptCredentialResponse =
        SERVER_ZK_OPS.issueReceiptCredential(context.getRequest(), 0L, 200L);

    when(loginPurchaseManager.generateReceipt(any(), any(), any())).thenReturn(receiptCredentialResponse);

    final CreateLoginReceiptCredentialResponse loginReceiptResponse =
        unauthenticatedServiceStub().createLoginReceiptCredential(createRequest(context));

    assertTrue(loginReceiptResponse.hasResult());
    assertArrayEquals(
        receiptCredentialResponse.serialize(),
        loginReceiptResponse.getResult().getReceiptCredentialResponse().toByteArray());

    verify(loginPurchaseManager).generateReceipt(
        eq(PaymentProvider.APPLE_APP_STORE),
        eq(PURCHASE_ID),
        any(ReceiptCredentialRequest.class));
  }

  private static CreateLoginReceiptCredentialRequest createRequest(final ReceiptCredentialRequestContext context) {
    return CreateLoginReceiptCredentialRequest.newBuilder()
        .setProcessor(org.signal.chat.purchase.PaymentProvider.PAYMENT_PROVIDER_APPLE_APP_STORE)
        .setPurchaseIdentifier(PURCHASE_ID)
        .setReceiptCredentialRequest(ByteString.copyFrom(context.getRequest().serialize()))
        .build();
  }

  @Test
  void createReceiptCredentialNotEnabled() {
    when(dynamicConfiguration.getLoginPurchaseConfiguration()).thenReturn(new DynamicLoginPurchaseConfiguration(false));
    GrpcTestUtils.assertStatusInvalidArgument(() ->
        unauthenticatedServiceStub().createLoginReceiptCredential(createRequest(receiptContext())));
    verifyNoInteractions(loginPurchaseManager);
  }

  static Stream<Arguments> createReceiptCredentialErrorResponses() {
    return Stream.of(
        Arguments.of( new SubscriptionReceiptRequestedForOpenPaymentException(),
            CreateLoginReceiptCredentialResponse.ResponseCase.PAYMENT_STILL_PROCESSING),
        Arguments.of( new SubscriptionPaymentRequiredException(),
            CreateLoginReceiptCredentialResponse.ResponseCase.PAYMENT_REQUIRED),
        Arguments.of(new SubscriptionNotFoundException(),
            CreateLoginReceiptCredentialResponse.ResponseCase.PAYMENT_NOT_FOUND),
        Arguments.of(new SubscriptionReceiptAlreadyRedeemedException(), CreateLoginReceiptCredentialResponse.ResponseCase.RECEIPT_ALREADY_ISSUED));
  }

  @ParameterizedTest
  @MethodSource
  void createReceiptCredentialErrorResponses(final Exception exception, final CreateLoginReceiptCredentialResponse.ResponseCase expectedResponse) throws Exception {
    when(loginPurchaseManager.generateReceipt(any(), any(), any())).thenThrow(exception);
    final CreateLoginReceiptCredentialResponse loginReceiptResponse =
        unauthenticatedServiceStub().createLoginReceiptCredential(createRequest(receiptContext()));
    assertEquals(expectedResponse, loginReceiptResponse.getResponseCase());
  }

  static Stream<Arguments> createReceiptCredentialErrorStatuses() {
    return Stream.of(
        Arguments.of(new SubscriptionInvalidArgumentsException("test"), Status.INVALID_ARGUMENT),
        Arguments.of(new VerificationFailedException(), Status.INVALID_ARGUMENT),
        Arguments.of(new RateLimitExceededException(null), Status.RESOURCE_EXHAUSTED));
  }

  @ParameterizedTest
  @MethodSource
  void createReceiptCredentialErrorStatuses(final Exception exception, final Status expectedStatus) throws Exception {
    when(loginPurchaseManager.generateReceipt(any(), any(), any())).thenThrow(exception);
    GrpcTestUtils.assertStatusException(expectedStatus, () -> unauthenticatedServiceStub().createLoginReceiptCredential(createRequest(receiptContext())));
  }

  @Test
  void createReceiptCredentialPaymentRequiredWithChargeFailure() throws Exception {
    final ChargeFailure chargeFailure =
        new ChargeFailure("generic_decline", "some failure message", "networkStatus", null, "type");
    when(loginPurchaseManager.generateReceipt(any(), any(), any())).thenThrow(
        new SubscriptionChargeFailurePaymentRequiredException(PaymentProvider.APPLE_APP_STORE, chargeFailure));
    final CreateLoginReceiptCredentialResponse loginReceiptResponse =
        unauthenticatedServiceStub().createLoginReceiptCredential(createRequest(receiptContext()));
    assertTrue(loginReceiptResponse.hasPaymentRequired());
    final org.signal.chat.purchase.ChargeFailure actualChargeFailure =
        loginReceiptResponse.getPaymentRequired().getChargeFailure();

    assertEquals(chargeFailure.code(), actualChargeFailure.getCode());
    assertEquals(chargeFailure.message(), actualChargeFailure.getMessage());
    assertEquals(chargeFailure.outcomeNetworkStatus(), actualChargeFailure.getOutcomeNetworkStatus());
    assertEquals(chargeFailure.outcomeType(), actualChargeFailure.getOutcomeType());
    assertFalse(actualChargeFailure.hasOutcomeReason());
  }

}
