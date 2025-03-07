/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.subscriptions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.api.client.http.HttpResponseException;
import com.google.api.services.androidpublisher.AndroidPublisher;
import com.google.api.services.androidpublisher.model.AutoRenewingPlan;
import com.google.api.services.androidpublisher.model.BasePlan;
import com.google.api.services.androidpublisher.model.Money;
import com.google.api.services.androidpublisher.model.OfferDetails;
import com.google.api.services.androidpublisher.model.RegionalBasePlanConfig;
import com.google.api.services.androidpublisher.model.Subscription;
import com.google.api.services.androidpublisher.model.SubscriptionPurchaseLineItem;
import com.google.api.services.androidpublisher.model.SubscriptionPurchaseV2;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.storage.SubscriptionException;
import org.whispersystems.textsecuregcm.util.CompletableFutureTestUtil;
import org.whispersystems.textsecuregcm.util.MockUtils;
import org.whispersystems.textsecuregcm.util.MutableClock;

class GooglePlayBillingManagerTest {

  private static final String PRODUCT_ID = "productId";
  private static final String PACKAGE_NAME = "package.name";
  private static final String PURCHASE_TOKEN = "purchaseToken";
  private static final String ORDER_ID = "orderId";

  // Returned in response to a purchases.subscriptionsv2.get
  private final AndroidPublisher.Purchases.Subscriptionsv2.Get subscriptionsv2Get =
      mock(AndroidPublisher.Purchases.Subscriptionsv2.Get.class);

  // Returned in response to a purchases.subscriptions.acknowledge
  private final AndroidPublisher.Purchases.Subscriptions.Acknowledge acknowledge =
      mock(AndroidPublisher.Purchases.Subscriptions.Acknowledge.class);

  // Returned in response to a purchases.subscriptionscancel.
  private final AndroidPublisher.Purchases.Subscriptions.Cancel cancel =
      mock(AndroidPublisher.Purchases.Subscriptions.Cancel.class);

  // Returned in response to a monetization.subscriptions.get
  private final AndroidPublisher.Monetization.Subscriptions.Get subscriptionConfig =
      mock(AndroidPublisher.Monetization.Subscriptions.Get.class);

  private final MutableClock clock = MockUtils.mutableClock(0L);

  private ExecutorService executor;
  private GooglePlayBillingManager googlePlayBillingManager;

  @BeforeEach
  public void setup() throws IOException {
    reset(subscriptionsv2Get);
    clock.setTimeMillis(0L);

    AndroidPublisher androidPublisher = mock(AndroidPublisher.class);
    AndroidPublisher.Purchases purchases = mock(AndroidPublisher.Purchases.class);
    AndroidPublisher.Monetization monetization = mock(AndroidPublisher.Monetization.class);

    when(androidPublisher.purchases()).thenReturn(purchases);
    when(androidPublisher.monetization()).thenReturn(monetization);

    AndroidPublisher.Purchases.Subscriptionsv2 subscriptionsv2 = mock(AndroidPublisher.Purchases.Subscriptionsv2.class);
    when(purchases.subscriptionsv2()).thenReturn(subscriptionsv2);
    when(subscriptionsv2.get(PACKAGE_NAME, PURCHASE_TOKEN)).thenReturn(subscriptionsv2Get);

    AndroidPublisher.Purchases.Subscriptions subscriptions = mock(AndroidPublisher.Purchases.Subscriptions.class);
    when(purchases.subscriptions()).thenReturn(subscriptions);
    when(subscriptions.acknowledge(eq(PACKAGE_NAME), eq(PRODUCT_ID), eq(PURCHASE_TOKEN), any()))
        .thenReturn(acknowledge);
    when(subscriptions.cancel(PACKAGE_NAME, PRODUCT_ID, PURCHASE_TOKEN))
        .thenReturn(cancel);

    AndroidPublisher.Monetization.Subscriptions msubscriptions = mock(
        AndroidPublisher.Monetization.Subscriptions.class);
    when(monetization.subscriptions()).thenReturn(msubscriptions);
    when(msubscriptions.get(PACKAGE_NAME, PRODUCT_ID)).thenReturn(subscriptionConfig);

    executor = Executors.newSingleThreadExecutor();
    googlePlayBillingManager = new GooglePlayBillingManager(
        androidPublisher, clock, PACKAGE_NAME, Map.of(PRODUCT_ID, 201L), executor);
  }

  @AfterEach
  public void teardown() throws InterruptedException {
    executor.shutdownNow();
    executor.awaitTermination(1, TimeUnit.SECONDS);
  }

  @Test
  public void validatePurchase() throws IOException {
    when(subscriptionsv2Get.execute()).thenReturn(new SubscriptionPurchaseV2()
        .setAcknowledgementState(GooglePlayBillingManager.AcknowledgementState.PENDING.apiString())
        .setSubscriptionState(GooglePlayBillingManager.SubscriptionState.ACTIVE.apiString())
        .setLineItems(List.of(new SubscriptionPurchaseLineItem()
            .setExpiryTime(Instant.now().plus(Duration.ofDays(1)).toString())
            .setProductId(PRODUCT_ID))));

    final GooglePlayBillingManager.ValidatedToken result = googlePlayBillingManager
        .validateToken(PURCHASE_TOKEN).join();

    assertThat(result.getLevel()).isEqualTo(201);
    assertThatNoException().isThrownBy(() -> result.acknowledgePurchase().join());
    verify(acknowledge, times(1)).execute();
  }

  @ParameterizedTest
  @EnumSource(mode = EnumSource.Mode.EXCLUDE, names = {"ACTIVE"})
  public void rejectInactivePurchase(GooglePlayBillingManager.SubscriptionState subscriptionState) throws IOException {
    when(subscriptionsv2Get.execute()).thenReturn(new SubscriptionPurchaseV2()
        .setAcknowledgementState(GooglePlayBillingManager.AcknowledgementState.PENDING.apiString())
        .setSubscriptionState(subscriptionState.apiString())
        .setLineItems(List.of(new SubscriptionPurchaseLineItem()
            .setExpiryTime(Instant.now().plus(Duration.ofDays(1)).toString())
            .setProductId(PRODUCT_ID))));

    CompletableFutureTestUtil.assertFailsWithCause(
        SubscriptionException.PaymentRequired.class,
        googlePlayBillingManager.validateToken(PURCHASE_TOKEN));
  }

  @Test
  public void avoidDoubleAcknowledge() throws IOException {
    when(subscriptionsv2Get.execute()).thenReturn(new SubscriptionPurchaseV2()
        .setAcknowledgementState(GooglePlayBillingManager.AcknowledgementState.ACKNOWLEDGED.apiString())
        .setSubscriptionState(GooglePlayBillingManager.SubscriptionState.ACTIVE.apiString())
        .setLineItems(List.of(new SubscriptionPurchaseLineItem()
            .setExpiryTime(Instant.now().plus(Duration.ofDays(1)).toString())
            .setProductId(PRODUCT_ID))));

    final GooglePlayBillingManager.ValidatedToken result = googlePlayBillingManager
        .validateToken(PURCHASE_TOKEN).join();

    assertThat(result.getLevel()).isEqualTo(201);
    assertThatNoException().isThrownBy(() -> result.acknowledgePurchase().join());
    verifyNoInteractions(acknowledge);
  }

  @ParameterizedTest
  @EnumSource
  public void cancel(GooglePlayBillingManager.SubscriptionState subscriptionState) throws IOException {
    when(subscriptionsv2Get.execute()).thenReturn(new SubscriptionPurchaseV2()
        .setAcknowledgementState(GooglePlayBillingManager.AcknowledgementState.ACKNOWLEDGED.apiString())
        .setSubscriptionState(subscriptionState.apiString())
        .setLineItems(List.of(new SubscriptionPurchaseLineItem()
            .setExpiryTime(Instant.now().plus(Duration.ofDays(1)).toString())
            .setProductId(PRODUCT_ID))));
    assertThatNoException().isThrownBy(() ->
        googlePlayBillingManager.cancelAllActiveSubscriptions(PURCHASE_TOKEN).join());
    final int wanted = switch (subscriptionState) {
      case CANCELED, EXPIRED -> 0;
      default -> 1;
    };
    verify(cancel, times(wanted)).execute();
  }

  @Test
  public void cancelMissingSubscription() throws IOException {
    final HttpResponseException mockException = mock(HttpResponseException.class);
    when(mockException.getStatusCode()).thenReturn(404);
    when(subscriptionsv2Get.execute()).thenThrow(mockException);
    assertThatNoException().isThrownBy(() ->
        googlePlayBillingManager.cancelAllActiveSubscriptions(PURCHASE_TOKEN).join());
    verifyNoInteractions(cancel);
  }

  @Test
  public void getReceiptUnacknowledged() throws IOException {
    when(subscriptionsv2Get.execute()).thenReturn(new SubscriptionPurchaseV2()
        .setAcknowledgementState(GooglePlayBillingManager.AcknowledgementState.PENDING.apiString())
        .setSubscriptionState(GooglePlayBillingManager.SubscriptionState.ACTIVE.apiString())
        .setLineItems(List.of(new SubscriptionPurchaseLineItem()
            .setExpiryTime(Instant.now().plus(Duration.ofDays(1)).toString())
            .setProductId(PRODUCT_ID))));
    CompletableFutureTestUtil.assertFailsWithCause(
        IllegalStateException.class,
        googlePlayBillingManager.getReceiptItem(PURCHASE_TOKEN));
  }

  @Test
  public void getReceiptExpiring() throws IOException {
    final Instant day9 = Instant.EPOCH.plus(Duration.ofDays(9));
    final Instant day10 = Instant.EPOCH.plus(Duration.ofDays(10));

    when(subscriptionsv2Get.execute()).thenReturn(new SubscriptionPurchaseV2()
        .setAcknowledgementState(GooglePlayBillingManager.AcknowledgementState.ACKNOWLEDGED.apiString())
        .setSubscriptionState(GooglePlayBillingManager.SubscriptionState.CANCELED.apiString())
        .setLatestOrderId(ORDER_ID)
        .setLineItems(List.of(new SubscriptionPurchaseLineItem()
            .setExpiryTime(day10.toString().toString())
            .setProductId(PRODUCT_ID))));

    clock.setTimeInstant(day9);
    SubscriptionPaymentProcessor.ReceiptItem item = googlePlayBillingManager.getReceiptItem(PURCHASE_TOKEN).join();
    assertThat(item.itemId()).isEqualTo(ORDER_ID);
    assertThat(item.level()).isEqualTo(201L);

    // receipt expirations rounded to nearest next day
    assertThat(item.paymentTime().receiptExpiration(Duration.ofDays(1), Duration.ZERO))
        .isEqualTo(day10.plus(Duration.ofDays(1)));

    // should still be able to get a receipt the next day
    clock.setTimeInstant(day10);
    item = googlePlayBillingManager.getReceiptItem(PURCHASE_TOKEN).join();
    assertThat(item.itemId()).isEqualTo(ORDER_ID);

    // next second should be expired
    clock.setTimeInstant(day10.plus(Duration.ofSeconds(1)));

    CompletableFutureTestUtil.assertFailsWithCause(
        SubscriptionException.PaymentRequired.class,
        googlePlayBillingManager.getReceiptItem(PURCHASE_TOKEN));
  }

  @Test
  public void getSubscriptionInfo() throws IOException {
    final String basePlanId = "basePlanId";
    when(subscriptionsv2Get.execute()).thenReturn(new SubscriptionPurchaseV2()
        .setAcknowledgementState(GooglePlayBillingManager.AcknowledgementState.ACKNOWLEDGED.apiString())
        .setSubscriptionState(GooglePlayBillingManager.SubscriptionState.ACTIVE.apiString())
        .setLatestOrderId(ORDER_ID)
        .setRegionCode("US")
        .setLineItems(List.of(new SubscriptionPurchaseLineItem()
            .setExpiryTime(Instant.now().plus(Duration.ofDays(1)).toString())
            .setAutoRenewingPlan(new AutoRenewingPlan().setAutoRenewEnabled(null))
            .setProductId(PRODUCT_ID)
            .setOfferDetails(new OfferDetails().setBasePlanId(basePlanId)))));

    final BasePlan basePlan = new BasePlan()
        .setBasePlanId(basePlanId)
        .setRegionalConfigs(List.of(
            new RegionalBasePlanConfig()
                .setRegionCode("US")
                .setPrice(new Money().setCurrencyCode("USD").setUnits(1L).setNanos(750_000_000))));
    when(subscriptionConfig.execute()).thenReturn(new Subscription().setBasePlans(List.of(basePlan)));

    final SubscriptionInformation info = googlePlayBillingManager.getSubscriptionInformation(PURCHASE_TOKEN).join();
    assertThat(info.active()).isTrue();
    assertThat(info.paymentProcessing()).isFalse();
    assertThat(info.price().currency()).isEqualTo("USD");
    assertThat(info.price().amount().compareTo(new BigDecimal("175"))).isEqualTo(0); // 175 cents
    assertThat(info.level()).isEqualTo(201L);
    assertThat(info.cancelAtPeriodEnd()).isTrue();

  }

  public static Stream<Arguments> tokenErrors() {
    return Stream.of(
        Arguments.of(404, SubscriptionException.NotFound.class),
        Arguments.of(410, SubscriptionException.NotFound.class),
        Arguments.of(400, HttpResponseException.class)
    );
  }
  @ParameterizedTest
  @MethodSource
  public void tokenErrors(final int httpStatus, Class<? extends Exception> expected) throws IOException {
    final HttpResponseException mockException = mock(HttpResponseException.class);
    when(mockException.getStatusCode()).thenReturn(httpStatus);
    when(subscriptionsv2Get.execute()).thenThrow(mockException);

    CompletableFutureTestUtil.assertFailsWithCause(expected,
        googlePlayBillingManager.getSubscriptionInformation(PURCHASE_TOKEN));
  }

}
