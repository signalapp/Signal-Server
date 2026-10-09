/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.purchases;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.signal.libsignal.zkgroup.ServerSecretParams;
import org.signal.libsignal.zkgroup.receipts.ClientZkReceiptOperations;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredential;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialRequestContext;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialResponse;
import org.signal.libsignal.zkgroup.receipts.ReceiptSerial;
import org.signal.libsignal.zkgroup.receipts.ServerZkReceiptOperations;
import org.whispersystems.textsecuregcm.storage.IssuedReceiptsManager;
import org.whispersystems.textsecuregcm.storage.WriteConflictException;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

@ExtendWith(MockitoExtension.class)
class LoginPurchaseManagerTest {

  private static final PaymentProvider PROVIDER = PaymentProvider.APPLE_APP_STORE;
  private static final String PURCHASE_ID = "purchaseId";
  private static final Instant PURCHASED_AT = Instant.now().minus(Duration.ofHours(3));

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

  @ParameterizedTest
  @EnumSource(mode = EnumSource.Mode.INCLUDE, names = {"LOGIN_SANDBOX", "LOGIN"})
  void generateReceiptSuccess(ReceiptLevel receiptLevel) throws Exception {
    when(paymentProcessor.claimOneTimePurchase(PURCHASE_ID))
        .thenReturn(new PaymentDetails(PURCHASE_ID, receiptLevel, PURCHASED_AT));

    final ReceiptCredentialResponse receipt =
        loginPurchaseManager.generateReceipt(PROVIDER, PURCHASE_ID, receiptCredentialRequestContext.getRequest());
    final ReceiptCredential receiptCredential =
        clientZkReceiptOperations.receiveReceiptCredential(receiptCredentialRequestContext, receipt);

    final Instant expectedExpiration = PURCHASED_AT
        .plus(switch (receiptLevel) {
          case LOGIN -> LoginPurchaseManager.LOGIN_EXPIRATION;
          case LOGIN_SANDBOX -> LoginPurchaseManager.SANDBOX_LOGIN_EXPIRATION;
          default -> throw new IllegalStateException();
        })
        .truncatedTo(ChronoUnit.DAYS);

    assertThat(receiptCredential.getReceiptLevel()).isEqualTo(receiptLevel.getValue());
    assertThat(receiptCredential.getReceiptExpirationTime()).isEqualTo(expectedExpiration.getEpochSecond());

    verify(issuedReceiptsManager).recordOneTimeIssuance(
        PURCHASE_ID, PROVIDER, receiptCredentialRequestContext.getRequest(), expectedExpiration);
  }

  @Test
  void generateReceiptNonLoginLevel() throws Exception {
    when(paymentProcessor.claimOneTimePurchase(PURCHASE_ID))
        .thenReturn(new PaymentDetails(PURCHASE_ID, ReceiptLevel.ONE_TIME_DONATION, PURCHASED_AT));
    assertThatExceptionOfType(PurchaseInvalidArgumentsException.class).isThrownBy(() ->
      loginPurchaseManager.generateReceipt(PROVIDER, PURCHASE_ID, receiptCredentialRequestContext.getRequest()));
    verifyNoInteractions(issuedReceiptsManager);
  }

  @Test
  void generateReceiptAlreadyRedeemed() throws Exception {
    when(paymentProcessor.claimOneTimePurchase(PURCHASE_ID))
        .thenReturn(new PaymentDetails(PURCHASE_ID, ReceiptLevel.LOGIN, PURCHASED_AT));
    doThrow(new WriteConflictException(null))
        .when(issuedReceiptsManager).recordOneTimeIssuance(any(), any(), any(), any());
    assertThatExceptionOfType(PurchaseReceiptAlreadyRedeemedException.class).isThrownBy(() ->
      loginPurchaseManager.generateReceipt(PROVIDER, PURCHASE_ID, receiptCredentialRequestContext.getRequest()));
  }
}
