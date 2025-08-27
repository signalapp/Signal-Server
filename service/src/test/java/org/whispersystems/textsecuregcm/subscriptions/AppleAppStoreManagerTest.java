/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.subscriptions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.apple.itunes.storekit.client.APIError;
import com.apple.itunes.storekit.client.APIException;
import com.apple.itunes.storekit.client.AppStoreServerAPIClient;
import com.apple.itunes.storekit.model.AutoRenewStatus;
import com.apple.itunes.storekit.model.JWSRenewalInfoDecodedPayload;
import com.apple.itunes.storekit.model.JWSTransactionDecodedPayload;
import com.apple.itunes.storekit.model.LastTransactionsItem;
import com.apple.itunes.storekit.model.Status;
import com.apple.itunes.storekit.model.StatusResponse;
import com.apple.itunes.storekit.model.SubscriptionGroupIdentifierItem;
import com.apple.itunes.storekit.verification.SignedDataVerifier;
import com.apple.itunes.storekit.verification.VerificationException;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.whispersystems.textsecuregcm.storage.SubscriptionException;
import org.whispersystems.textsecuregcm.util.CompletableFutureTestUtil;

class AppleAppStoreManagerTest {

  private final static long LEVEL = 123L;
  private final static String ORIGINAL_TX_ID = "originalTxIdTest";
  private final static String SUBSCRIPTION_GROUP_ID = "subscriptionGroupIdTest";
  private final static String SIGNED_RENEWAL_INFO = "signedRenewalInfoTest";
  private final static String SIGNED_TX_INFO = "signedRenewalInfoTest";
  private final static String PRODUCT_ID = "productIdTest";
  private final static String WEB_ORDER_LINE_ITEM = "webOrderLineItemTest";

  private final AppStoreServerAPIClient apiClient = mock(AppStoreServerAPIClient.class);
  private final SignedDataVerifier signedDataVerifier = mock(SignedDataVerifier.class);
  private ScheduledExecutorService executor;
  private AppleAppStoreManager appleAppStoreManager;

  @BeforeEach
  public void setup() {
    reset(apiClient, signedDataVerifier);
    executor = Executors.newSingleThreadScheduledExecutor();
    appleAppStoreManager = new AppleAppStoreManager(apiClient, signedDataVerifier,
        SUBSCRIPTION_GROUP_ID, Map.of(PRODUCT_ID, LEVEL), null, executor, executor);
  }

  @AfterEach
  public void teardown() throws InterruptedException {
    executor.shutdownNow();
    executor.awaitTermination(1, TimeUnit.SECONDS);
  }

  @Test
  public void lookupTransaction() throws APIException, IOException, VerificationException {
    mockValidSubscription();
    final SubscriptionInformation info = appleAppStoreManager.getSubscriptionInformation(ORIGINAL_TX_ID).join();

    assertThat(info.active()).isTrue();
    assertThat(info.paymentProcessing()).isFalse();
    assertThat(info.level()).isEqualTo(LEVEL);
    assertThat(info.cancelAtPeriodEnd()).isFalse();
    assertThat(info.status()).isEqualTo(SubscriptionStatus.ACTIVE);
    assertThat(info.price().amount().compareTo(new BigDecimal("150"))).isEqualTo(0); // 150 cents
  }

  @Test
  public void validateTransaction() throws VerificationException, APIException, IOException {
    mockValidSubscription();
    assertThat(appleAppStoreManager.validateTransaction(ORIGINAL_TX_ID).join()).isEqualTo(LEVEL);
  }

  @Test
  public void generateReceipt() throws VerificationException, APIException, IOException {
    mockValidSubscription();
    final SubscriptionPaymentProcessor.ReceiptItem receipt = appleAppStoreManager.getReceiptItem(ORIGINAL_TX_ID).join();
    assertThat(receipt.level()).isEqualTo(LEVEL);
    assertThat(receipt.paymentTime().receiptExpiration(Duration.ofDays(1), Duration.ZERO))
        .isEqualTo(Instant.EPOCH.plus(Duration.ofDays(2)));
    assertThat(receipt.itemId()).isEqualTo(WEB_ORDER_LINE_ITEM);
  }

  @Test
  public void generateReceiptExpired() throws VerificationException, APIException, IOException {
    mockSubscription(Status.EXPIRED, AutoRenewStatus.ON);
    CompletableFutureTestUtil.assertFailsWithCause(SubscriptionException.PaymentRequired.class,
        appleAppStoreManager.getReceiptItem(ORIGINAL_TX_ID));
  }

  @Test
  public void autoRenewOff() throws VerificationException, APIException, IOException {
    mockSubscription(Status.ACTIVE, AutoRenewStatus.OFF);
    final SubscriptionInformation info = appleAppStoreManager.getSubscriptionInformation(ORIGINAL_TX_ID).join();

    assertThat(info.cancelAtPeriodEnd()).isTrue();

    assertThat(info.active()).isTrue();
    assertThat(info.paymentProcessing()).isFalse();
    assertThat(info.level()).isEqualTo(LEVEL);
    assertThat(info.status()).isEqualTo(SubscriptionStatus.ACTIVE);
  }

  @Test
  public void lookupMultipleProducts() throws APIException, IOException, VerificationException {
    // The lookup should select the transaction at i=1
    final List<String> products = List.of("otherProduct1", PRODUCT_ID, "otherProduct3");

    when(apiClient.getAllSubscriptionStatuses(ORIGINAL_TX_ID, new Status[]{}))
        .thenReturn(new StatusResponse().data(List.of(new SubscriptionGroupIdentifierItem()
            .subscriptionGroupIdentifier(SUBSCRIPTION_GROUP_ID)
            .lastTransactions(products.stream().map(product -> new LastTransactionsItem()
                    .originalTransactionId(ORIGINAL_TX_ID)
                    .status(Status.ACTIVE)
                    .signedRenewalInfo(SIGNED_RENEWAL_INFO)
                    .signedTransactionInfo(product + "_signed_tx"))
                .toList()))));
    when(signedDataVerifier.verifyAndDecodeRenewalInfo(SIGNED_RENEWAL_INFO))
        .thenReturn(new JWSRenewalInfoDecodedPayload()
            .autoRenewStatus(AutoRenewStatus.ON));

    for (int i = 0; i < products.size(); i++) {
      // Give each productId a different price, the selected transaction should have priceMillis 1000
      final long priceMillis = i * 1000L;
      final String productId = products.get(i);
      when(signedDataVerifier.verifyAndDecodeTransaction(productId + "_signed_tx"))
          .thenReturn(new JWSTransactionDecodedPayload()
              .productId(productId)
              .currency("usd").price(priceMillis)
              .originalPurchaseDate(Instant.EPOCH.toEpochMilli())
              .expiresDate(Instant.EPOCH.plus(Duration.ofDays(1)).toEpochMilli()));
    }
    final SubscriptionInformation info = appleAppStoreManager.getSubscriptionInformation(ORIGINAL_TX_ID).join();

    assertThat(info.price().amount().compareTo(new BigDecimal("100"))).isEqualTo(0);

  }

  @Test
  public void retryEventuallyWorks() throws APIException, IOException, VerificationException {
    // Should retry up to 3 times
    when(apiClient.getAllSubscriptionStatuses(ORIGINAL_TX_ID, new Status[]{}))
        .thenThrow(new APIException(404, APIError.ORIGINAL_TRANSACTION_ID_NOT_FOUND_RETRYABLE.errorCode(), "test"))
        .thenThrow(new APIException(404, APIError.ORIGINAL_TRANSACTION_ID_NOT_FOUND_RETRYABLE.errorCode(), "test"))
        .thenReturn(new StatusResponse().data(List.of(new SubscriptionGroupIdentifierItem()
            .subscriptionGroupIdentifier(SUBSCRIPTION_GROUP_ID)
            .addLastTransactionsItem(new LastTransactionsItem()
                .originalTransactionId(ORIGINAL_TX_ID)
                .status(Status.ACTIVE)
                .signedRenewalInfo(SIGNED_RENEWAL_INFO)
                .signedTransactionInfo(SIGNED_TX_INFO)))));
    mockDecode(AutoRenewStatus.ON);
    final SubscriptionInformation info = appleAppStoreManager.getSubscriptionInformation(ORIGINAL_TX_ID).join();
    assertThat(info.status()).isEqualTo(SubscriptionStatus.ACTIVE);
  }

  @Test
  public void retryEventuallyGivesUp() throws APIException, IOException, VerificationException {
    // Should retry up to 3 times
    when(apiClient.getAllSubscriptionStatuses(ORIGINAL_TX_ID, new Status[]{}))
        .thenThrow(new APIException(404, APIError.ORIGINAL_TRANSACTION_ID_NOT_FOUND_RETRYABLE.errorCode(), "test"));
    mockDecode(AutoRenewStatus.ON);
    CompletableFutureTestUtil.assertFailsWithCause(APIException.class,
        appleAppStoreManager.getSubscriptionInformation(ORIGINAL_TX_ID));

    verify(apiClient, times(3)).getAllSubscriptionStatuses(ORIGINAL_TX_ID, new Status[]{});

  }

  @Test
  public void cancelRenewalDisabled() throws APIException, VerificationException, IOException {
    mockSubscription(Status.ACTIVE, AutoRenewStatus.OFF);
    assertDoesNotThrow(() -> appleAppStoreManager.cancelAllActiveSubscriptions(ORIGINAL_TX_ID).join());
  }

  @ParameterizedTest
  @EnumSource(mode = EnumSource.Mode.EXCLUDE, names = {"EXPIRED", "REVOKED"})
  public void cancelFailsForActiveSubscription(Status status) throws APIException, VerificationException, IOException {
    mockSubscription(status, AutoRenewStatus.ON);
    CompletableFutureTestUtil.assertFailsWithCause(SubscriptionException.InvalidArguments.class,
        appleAppStoreManager.cancelAllActiveSubscriptions(ORIGINAL_TX_ID));
  }

  @ParameterizedTest
  @EnumSource(mode = EnumSource.Mode.INCLUDE, names = {"EXPIRED", "REVOKED"})
  public void cancelInactiveStatus(Status status) throws APIException, VerificationException, IOException {
    mockSubscription(status, AutoRenewStatus.ON);
    assertDoesNotThrow(() -> appleAppStoreManager.cancelAllActiveSubscriptions(ORIGINAL_TX_ID).join());
  }

  private void mockSubscription(final Status status, final AutoRenewStatus autoRenewStatus)
      throws APIException, IOException, VerificationException {
    when(apiClient.getAllSubscriptionStatuses(ORIGINAL_TX_ID, new Status[]{}))
        .thenReturn(new StatusResponse().data(List.of(new SubscriptionGroupIdentifierItem()
            .subscriptionGroupIdentifier(SUBSCRIPTION_GROUP_ID)
            .addLastTransactionsItem(new LastTransactionsItem()
                .originalTransactionId(ORIGINAL_TX_ID)
                .status(status)
                .signedRenewalInfo(SIGNED_RENEWAL_INFO)
                .signedTransactionInfo(SIGNED_TX_INFO)))));
    mockDecode(autoRenewStatus);
  }

  private void mockValidSubscription() throws APIException, IOException, VerificationException {
    mockSubscription(Status.ACTIVE, AutoRenewStatus.ON);
  }

  private void mockDecode(final AutoRenewStatus autoRenewStatus) throws VerificationException {
    when(signedDataVerifier.verifyAndDecodeTransaction(SIGNED_TX_INFO))
        .thenReturn(new JWSTransactionDecodedPayload()
            .productId(PRODUCT_ID)
            .currency("usd").price(1500L) // $1.50
            .originalPurchaseDate(Instant.EPOCH.toEpochMilli())
            .expiresDate(Instant.EPOCH.plus(Duration.ofDays(1)).toEpochMilli())
            .webOrderLineItemId(WEB_ORDER_LINE_ITEM));
    when(signedDataVerifier.verifyAndDecodeRenewalInfo(SIGNED_RENEWAL_INFO))
        .thenReturn(new JWSRenewalInfoDecodedPayload()
            .autoRenewStatus(autoRenewStatus));
  }

}
