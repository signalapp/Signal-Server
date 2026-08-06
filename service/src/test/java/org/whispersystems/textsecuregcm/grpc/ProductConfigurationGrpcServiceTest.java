/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.common.net.InetAddresses;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.signal.chat.purchase.BackupLevelConfiguration;
import org.signal.chat.purchase.CurrencyConfiguration;
import org.signal.chat.purchase.GetConfigurationRequest;
import org.signal.chat.purchase.GetConfigurationResponse;
import org.signal.chat.purchase.PaymentMethod;
import org.signal.chat.purchase.ProductConfigurationGrpc;
import org.whispersystems.textsecuregcm.configuration.LoginPurchaseConfiguration;
import org.whispersystems.textsecuregcm.configuration.OneTimeDonationConfiguration;
import org.whispersystems.textsecuregcm.configuration.SubscriptionConfiguration;
import org.whispersystems.textsecuregcm.subscriptions.BraintreeManager;
import org.whispersystems.textsecuregcm.subscriptions.ReceiptLevel;
import org.whispersystems.textsecuregcm.subscriptions.StripeManager;
import org.whispersystems.textsecuregcm.tests.util.SubscriptionConfigTestHelper;

public class ProductConfigurationGrpcServiceTest extends
    SimpleBaseGrpcTest<ProductConfigurationGrpcService, ProductConfigurationGrpc.ProductConfigurationBlockingStub> {

  private final SubscriptionConfiguration subscriptionConfiguration =
      SubscriptionConfigTestHelper.getSubscriptionConfig();

  private final OneTimeDonationConfiguration oneTimeDonationConfiguration =
      SubscriptionConfigTestHelper.getOneTimeConfig();

  private static final LoginPurchaseConfiguration LOGIN_PURCHASE_CONFIGURATION =
      new LoginPurchaseConfiguration("testLoginPlayProductId", "testLoginAppStoreProductId");

  @Mock
  private StripeManager stripeManager;

  @Mock
  private BraintreeManager braintreeManager;

  @Override
  protected ProductConfigurationGrpcService createServiceBeforeEachTest() {
    getMockRequestAttributesInterceptor().setRequestAttributes(
        new RequestAttributes(InetAddresses.forString("127.0.0.1"), null, "en-us"));

    when(stripeManager.supportsPaymentMethod(any())).thenCallRealMethod();
    when(stripeManager.getSupportedCurrenciesForPaymentMethod(
        org.whispersystems.textsecuregcm.subscriptions.PaymentMethod.CARD))
        .thenReturn(Set.of("usd", "jpy", "bif", "eur"));
    when(stripeManager.getSupportedCurrenciesForPaymentMethod(
        org.whispersystems.textsecuregcm.subscriptions.PaymentMethod.SEPA_DEBIT))
        .thenReturn(Set.of("eur"));
    when(stripeManager.getSupportedCurrenciesForPaymentMethod(
        org.whispersystems.textsecuregcm.subscriptions.PaymentMethod.IDEAL))
        .thenReturn(Set.of("eur"));
    when(braintreeManager.supportsPaymentMethod(any())).thenCallRealMethod();
    when(braintreeManager.getSupportedCurrenciesForPaymentMethod(
        org.whispersystems.textsecuregcm.subscriptions.PaymentMethod.PAYPAL))
        .thenReturn(Set.of("usd", "jpy"));


    return new ProductConfigurationGrpcService(subscriptionConfiguration, oneTimeDonationConfiguration,
        LOGIN_PURCHASE_CONFIGURATION, List.of(stripeManager, braintreeManager), 1234L);
  }

  @Test
  void getConfiguration() {
    final GetConfigurationResponse configuration = unauthenticatedServiceStub()
        .getConfiguration(GetConfigurationRequest.newBuilder().build());

    assertEquals("10000", configuration.getSepaMaximumEuros());

    assertEquals(30L, configuration.getBackup().getFreeTierMediaDays());
    final BackupLevelConfiguration backupLevel =
        configuration.getBackup().getLevelsOrThrow(201L);
    assertEquals(1234L, backupLevel.getStorageAllowanceBytes());
    assertEquals("testPlayProductId", backupLevel.getPlayProductId());
    assertEquals(40L, backupLevel.getMediaTtlDays());

    final CurrencyConfiguration usd = configuration.getCurrenciesOrThrow("usd");
    assertEquals("2.50", usd.getMinimum());
    assertEquals(List.of(PaymentMethod.PAYMENT_METHOD_CARD, PaymentMethod.PAYMENT_METHOD_PAYPAL),
        usd.getSupportedPaymentMethodsList());
    assertEquals(List.of("5.50", "6", "7", "8", "9", "10"), usd.getOneTimeOrThrow(1L).getAmountsList());
    assertEquals(List.of("20"), usd.getOneTimeOrThrow(100L).getAmountsList());
    assertEquals(Map.of(5L, "5", 15L, "15", 35L, "35"), usd.getSubscriptionMap());
    assertEquals(Map.of(201L, "5"), usd.getBackupSubscriptionMap());

    final CurrencyConfiguration jpy = configuration.getCurrenciesOrThrow("jpy");
    assertEquals("250", jpy.getMinimum());
    assertEquals(List.of(PaymentMethod.PAYMENT_METHOD_CARD, PaymentMethod.PAYMENT_METHOD_PAYPAL),
        jpy.getSupportedPaymentMethodsList());
    assertEquals(List.of("550", "600", "700", "800", "900", "1000"), jpy.getOneTimeOrThrow(1L).getAmountsList());
    assertEquals(List.of("2000"), jpy.getOneTimeOrThrow(100L).getAmountsList());
    assertEquals(Map.of(5L, "500", 15L, "1500", 35L, "3500"), jpy.getSubscriptionMap());
    assertEquals(Map.of(201L, "500"), jpy.getBackupSubscriptionMap());

    final CurrencyConfiguration bif = configuration.getCurrenciesOrThrow("bif");
    assertEquals("2500", bif.getMinimum());
    assertEquals(List.of(PaymentMethod.PAYMENT_METHOD_CARD), bif.getSupportedPaymentMethodsList());
    assertEquals(List.of("5500", "6000", "7000", "8000", "9000", "10000"), bif.getOneTimeOrThrow(1L).getAmountsList());
    assertEquals(List.of("20000"), bif.getOneTimeOrThrow(100L).getAmountsList());
    assertEquals(Map.of(5L, "5000", 15L, "15000", 35L, "35000"), bif.getSubscriptionMap());
    assertEquals(Map.of(201L, "5000"), bif.getBackupSubscriptionMap());

    final CurrencyConfiguration eur = configuration.getCurrenciesOrThrow("eur");
    assertEquals("3", eur.getMinimum());
    assertEquals(
        List.of(PaymentMethod.PAYMENT_METHOD_CARD, PaymentMethod.PAYMENT_METHOD_SEPA_DEBIT,
            PaymentMethod.PAYMENT_METHOD_IDEAL),
        eur.getSupportedPaymentMethodsList());
    assertEquals(List.of("5", "10", "20", "30", "50", "100"), eur.getOneTimeOrThrow(1L).getAmountsList());
    assertEquals(List.of("5"), eur.getOneTimeOrThrow(100L).getAmountsList());
    assertEquals(Map.of(5L, "5", 15L, "15", 35L, "35"), eur.getSubscriptionMap());
    assertEquals(Map.of(201L, "5"), eur.getBackupSubscriptionMap());

    assertEquals("B1", configuration.getBadgeLevelsOrThrow(5L).getBadgeId());
    assertEquals(0L, configuration.getBadgeLevelsOrThrow(5L).getBadgeDurationSeconds());
    assertEquals("B2", configuration.getBadgeLevelsOrThrow(15L).getBadgeId());
    assertEquals("B3", configuration.getBadgeLevelsOrThrow(35L).getBadgeId());
    assertEquals("BOOST", configuration.getBadgeLevelsOrThrow(1L).getBadgeId());
    assertTrue(configuration.getBadgeLevelsOrThrow(1L).getBadgeDurationSeconds() > 0);
    assertEquals("GIFT", configuration.getBadgeLevelsOrThrow(100L).getBadgeId());
    assertTrue(configuration.getBadgeLevelsOrThrow(100L).getBadgeDurationSeconds() > 0);

    assertEquals(ReceiptLevel.LOGIN.getValue(), configuration.getLogin().getLevel());
    assertEquals(LOGIN_PURCHASE_CONFIGURATION.playProductId(), configuration.getLogin().getPlayProductId());
    assertEquals(LOGIN_PURCHASE_CONFIGURATION.appStoreProductId(), configuration.getLogin().getAppStoreProductId());
  }


}
