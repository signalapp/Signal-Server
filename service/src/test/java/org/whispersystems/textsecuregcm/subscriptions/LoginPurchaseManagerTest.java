/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.subscriptions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.signal.libsignal.zkgroup.ServerSecretParams;
import org.signal.libsignal.zkgroup.receipts.ClientZkReceiptOperations;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredential;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialRequestContext;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialResponse;
import org.signal.libsignal.zkgroup.receipts.ReceiptSerial;
import org.signal.libsignal.zkgroup.receipts.ServerZkReceiptOperations;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.storage.IssuedReceiptsManager;
import org.whispersystems.textsecuregcm.storage.WriteConflictException;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

@ExtendWith(MockitoExtension.class)
class LoginPurchaseManagerTest {

  private static final PaymentProvider PROVIDER = PaymentProvider.APPLE_APP_STORE;
  private static final String PURCHASE_ID = "purchaseId";
  private static final Instant PURCHASED_AT = Instant.now().minus(Duration.ofHours(3));
  private static final ChargeFailure CHARGE_FAILURE =
      new ChargeFailure("generic_decline", "some failure message", null, null, null);

  private static final ServerSecretParams SERVER_SECRET_PARAMS = ServerSecretParams.generate();

  @Mock
  private OneTimePaymentProcessor paymentProcessor;
  @Mock
  private IssuedReceiptsManager issuedReceiptsManager;

  private final ClientZkReceiptOperations clientZkReceiptOperations =
      new ClientZkReceiptOperations(SERVER_SECRET_PARAMS.getPublicParams());

  private ReceiptCredentialRequestContext receiptCredentialRequestContext;
  private LoginPurchaseManager loginPurchaseManager;

  @BeforeEach
  void setUp() throws Exception {
    receiptCredentialRequestContext = clientZkReceiptOperations.createReceiptCredentialRequestContext(
        new ReceiptSerial(TestRandomUtil.nextBytes(ReceiptSerial.SIZE)));

    loginPurchaseManager = new LoginPurchaseManager(
        Map.of(PROVIDER, paymentProcessor),
        issuedReceiptsManager,
        new ServerZkReceiptOperations(SERVER_SECRET_PARAMS));
  }

  @Test
  void generateReceiptSuccess() throws Exception {
    when(paymentProcessor.claimOneTimePurchase(PURCHASE_ID))
        .thenReturn(Optional.of(new PaymentDetails(PURCHASE_ID, ReceiptLevel.LOGIN, PaymentStatus.SUCCEEDED, PURCHASED_AT, null)));

    final ReceiptCredentialResponse receipt =
        loginPurchaseManager.generateReceipt(PROVIDER, PURCHASE_ID, receiptCredentialRequestContext.getRequest());
    final ReceiptCredential receiptCredential =
        clientZkReceiptOperations.receiveReceiptCredential(receiptCredentialRequestContext, receipt);

    final Instant expectedExpiration = PURCHASED_AT
        .plus(LoginPurchaseManager.LOGIN_EXPIRATION)
        .truncatedTo(ChronoUnit.DAYS);

    assertThat(receiptCredential.getReceiptLevel()).isEqualTo(ReceiptLevel.LOGIN.getValue());
    assertThat(receiptCredential.getReceiptExpirationTime()).isEqualTo(expectedExpiration.getEpochSecond());

    verify(issuedReceiptsManager).recordOneTimeIssuance(
        PURCHASE_ID, PROVIDER, receiptCredentialRequestContext.getRequest(), expectedExpiration);
  }

  static Stream<Arguments> generateReceiptErrors() {
    return Stream.of(
        Arguments.of(Optional.empty(), SubscriptionNotFoundException.class),
        Arguments.of(
            Optional.of(new PaymentDetails(PURCHASE_ID, ReceiptLevel.LOGIN, PaymentStatus.PROCESSING, PURCHASED_AT, null)),
            SubscriptionReceiptRequestedForOpenPaymentException.class),
        Arguments.of(
            Optional.of(new PaymentDetails(PURCHASE_ID, ReceiptLevel.LOGIN, PaymentStatus.FAILED, PURCHASED_AT, CHARGE_FAILURE)),
            SubscriptionChargeFailurePaymentRequiredException.class),
        Arguments.of(
            Optional.of(new PaymentDetails(PURCHASE_ID, ReceiptLevel.LOGIN, PaymentStatus.FAILED, PURCHASED_AT, null)),
            SubscriptionPaymentRequiredException.class),
        Arguments.of(Optional.of(new PaymentDetails(PURCHASE_ID, ReceiptLevel.LOGIN, PaymentStatus.UNKNOWN, PURCHASED_AT, null)),
            SubscriptionPaymentRequiredException.class));
  }

  @ParameterizedTest
  @MethodSource
  void generateReceiptErrors(final Optional<PaymentDetails> paymentDetails, final Class<? extends Exception> expectedException)
      throws SubscriptionInvalidArgumentsException, RateLimitExceededException, IOException {
    when(paymentProcessor.claimOneTimePurchase(PURCHASE_ID)).thenReturn(paymentDetails);
    assertThatExceptionOfType(expectedException).isThrownBy(() ->
        loginPurchaseManager.generateReceipt(PROVIDER, PURCHASE_ID, receiptCredentialRequestContext.getRequest()));
  }

  @Test
  void generateReceiptUnknownLevel() throws Exception {
    when(paymentProcessor.claimOneTimePurchase(PURCHASE_ID))
        .thenReturn(Optional.of(new PaymentDetails(PURCHASE_ID, null, PaymentStatus.SUCCEEDED, PURCHASED_AT, null)));
    assertThatExceptionOfType(SubscriptionInvalidArgumentsException.class).isThrownBy(() ->
      loginPurchaseManager.generateReceipt(PROVIDER, PURCHASE_ID, receiptCredentialRequestContext.getRequest()));
    verifyNoInteractions(issuedReceiptsManager);
  }

  @Test
  void generateReceiptNonLoginLevel() throws Exception {
    when(paymentProcessor.claimOneTimePurchase(PURCHASE_ID))
        .thenReturn(Optional.of(new PaymentDetails(PURCHASE_ID, ReceiptLevel.ONE_TIME_DONATION, PaymentStatus.SUCCEEDED, PURCHASED_AT, null)));
    assertThatExceptionOfType(SubscriptionInvalidArgumentsException.class).isThrownBy(() ->
      loginPurchaseManager.generateReceipt(PROVIDER, PURCHASE_ID, receiptCredentialRequestContext.getRequest()));
    verifyNoInteractions(issuedReceiptsManager);
  }

  @Test
  void generateReceiptAlreadyRedeemed() throws Exception {
    when(paymentProcessor.claimOneTimePurchase(PURCHASE_ID))
        .thenReturn(Optional.of(new PaymentDetails(PURCHASE_ID, ReceiptLevel.LOGIN, PaymentStatus.SUCCEEDED, PURCHASED_AT, null)));
    doThrow(new WriteConflictException())
        .when(issuedReceiptsManager).recordOneTimeIssuance(any(), any(), any(), any());
    assertThatExceptionOfType(SubscriptionReceiptAlreadyRedeemedException.class).isThrownBy(() ->
      loginPurchaseManager.generateReceipt(PROVIDER, PURCHASE_ID, receiptCredentialRequestContext.getRequest()));
  }
}
