/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.net.InetAddresses;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.signal.chat.purchase.BackupLevelConfiguration;
import org.signal.chat.purchase.Configuration;
import org.signal.chat.purchase.CurrencyConfiguration;
import org.signal.chat.purchase.GetConfigurationRequest;
import org.signal.chat.purchase.GetConfigurationResponse;
import org.signal.chat.purchase.PaymentMethod;
import org.signal.chat.purchase.ProductConfigurationGrpc;
import org.whispersystems.textsecuregcm.badges.BadgeTranslator;
import org.whispersystems.textsecuregcm.configuration.OneTimeDonationConfiguration;
import org.whispersystems.textsecuregcm.configuration.SubscriptionConfiguration;
import org.whispersystems.textsecuregcm.entities.Badge;
import org.whispersystems.textsecuregcm.entities.BadgeSvg;
import org.whispersystems.textsecuregcm.subscriptions.BraintreeManager;
import org.whispersystems.textsecuregcm.subscriptions.StripeManager;
import org.whispersystems.textsecuregcm.tests.util.SubscriptionConfigTestHelper;

public class ProductConfigurationGrpcServiceTest extends
    SimpleBaseGrpcTest<ProductConfigurationGrpcService, ProductConfigurationGrpc.ProductConfigurationBlockingStub> {

  private final SubscriptionConfiguration subscriptionConfiguration =
      SubscriptionConfigTestHelper.getSubscriptionConfig();

  private final OneTimeDonationConfiguration oneTimeDonationConfiguration =
      SubscriptionConfigTestHelper.getOneTimeConfig();

  @Mock
  private StripeManager stripeManager;

  @Mock
  private BraintreeManager braintreeManager;

  @Mock
  private BadgeTranslator badgeTranslator;

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

    when(badgeTranslator.translate(any(), eq("B1"))).thenReturn(new Badge("B1", "cat1", "name1", "desc1",
        List.of("l", "m", "h", "x", "xx", "xxx"), "SVG", List.of(new BadgeSvg("sl", "sd"))));
    when(badgeTranslator.translate(any(), eq("B2"))).thenReturn(new Badge("B2", "cat2", "name2", "desc2",
        List.of("l", "m", "h", "x", "xx", "xxx"), "SVG", List.of(new BadgeSvg("sl", "sd"))));
    when(badgeTranslator.translate(any(), eq("B3"))).thenReturn(new Badge("B3", "cat3", "name3", "desc3",
        List.of("l", "m", "h", "x", "xx", "xxx"), "SVG", List.of(new BadgeSvg("sl", "sd"))));
    when(badgeTranslator.translate(any(), eq("BOOST"))).thenReturn(new Badge("BOOST", "boost1", "boost1", "boost1",
        List.of("l", "m", "h", "x", "xx", "xxx"), "SVG", List.of(new BadgeSvg("sl", "sd"))));
    when(badgeTranslator.translate(any(), eq("GIFT"))).thenReturn(new Badge("GIFT", "gift1", "gift1", "gift1",
        List.of("l", "m", "h", "x", "xx", "xxx"), "SVG", List.of(new BadgeSvg("sl", "sd"))));

    when(badgeTranslator.resolveLocale(any())).thenReturn(Locale.US);


    return new ProductConfigurationGrpcService(subscriptionConfiguration, oneTimeDonationConfiguration,
        List.of(stripeManager, braintreeManager), badgeTranslator, 1234L);
  }

  private void checkConfiguration(final Configuration configuration) {
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

    assertEquals("B1", configuration.getBadgeLevelsOrThrow(5L).getBadge().getId());
    assertEquals(0L, configuration.getBadgeLevelsOrThrow(5L).getBadgeDurationSeconds());
    assertEquals("B2", configuration.getBadgeLevelsOrThrow(15L).getBadge().getId());
    assertEquals("B3", configuration.getBadgeLevelsOrThrow(35L).getBadge().getId());
    assertEquals("BOOST", configuration.getBadgeLevelsOrThrow(1L).getBadge().getId());
    assertTrue(configuration.getBadgeLevelsOrThrow(1L).getBadgeDurationSeconds() > 0);
    assertEquals("GIFT", configuration.getBadgeLevelsOrThrow(100L).getBadge().getId());
    assertTrue(configuration.getBadgeLevelsOrThrow(100L).getBadgeDurationSeconds() > 0);
  }

  @Test
  void getConfigurationNoEtag() {
    final GetConfigurationResponse response = unauthenticatedServiceStub()
        .getConfiguration(GetConfigurationRequest.newBuilder().build());

    assertFalse(response.hasEtagMatched());
    checkConfiguration(response.getTaggedConfiguration().getConfiguration());
  }

  @Test
  void getConfigurationWrongEtag() {
    final GetConfigurationResponse response = unauthenticatedServiceStub()
        .getConfiguration(GetConfigurationRequest.newBuilder().setEtag(ByteString.copyFrom(new byte[32])).build());
    assertFalse(response.hasEtagMatched());
    checkConfiguration(response.getTaggedConfiguration().getConfiguration());
  }

  @Test
  void getConfigurationCorrectEtag() {
    final GetConfigurationResponse response = unauthenticatedServiceStub()
        .getConfiguration(GetConfigurationRequest.newBuilder().build());
    final ByteString etag = response.getTaggedConfiguration().getEtag();

    final GetConfigurationResponse withCorrectEtag = unauthenticatedServiceStub()
        .getConfiguration(GetConfigurationRequest.newBuilder().setEtag(etag).build());
    assertTrue(withCorrectEtag.hasEtagMatched());
    assertTrue(withCorrectEtag.getEtagMatched());
    assertFalse(withCorrectEtag.hasTaggedConfiguration());
  }

  @Test
  void getConfigurationComputesOnce() {
    final RequestAttributes frAttributes =
        new RequestAttributes(InetAddresses.forString("127.0.0.1"), null, "fr-fr");
    final List<Locale> frLocales = List.of(Locale.FRANCE);
    final RequestAttributes caAttributes =
        new RequestAttributes(InetAddresses.forString("127.0.0.1"), null, "en-ca");
    final List<Locale> caLocales = List.of(Locale.CANADA);

    when(badgeTranslator.resolveLocale(frLocales)).thenReturn(Locale.FRANCE);
    when(badgeTranslator.resolveLocale(caLocales)).thenReturn(Locale.CANADA);

    // return slightly different values based on the language so the two configurations have different etags
    when(badgeTranslator.translate(eq(frLocales), eq("B1")))
        .thenReturn(new Badge("B1", "cat1", "name1", "desc1", List.of("l", "m", "h", "x", "xx", "xxx"), "SVG", List.of(new BadgeSvg("sl", "sd"))));
    when(badgeTranslator.translate(eq(caLocales), eq("B1")))
        .thenReturn(new Badge("B1", "dog1", "name1", "desc1", List.of("l", "m", "h", "x", "xx", "xxx"), "SVG", List.of(new BadgeSvg("sl", "sd"))));

    getMockRequestAttributesInterceptor().setRequestAttributes(caAttributes);
    final GetConfigurationResponse caResponse =
        unauthenticatedServiceStub().getConfiguration(GetConfigurationRequest.newBuilder().build());
    final GetConfigurationResponse caResponseCached = unauthenticatedServiceStub()
        .getConfiguration(GetConfigurationRequest.newBuilder()
            .setEtag(caResponse.getTaggedConfiguration().getEtag())
            .build());
    assertTrue(caResponseCached.hasEtagMatched());
    verify(badgeTranslator, times(1)).translate(any(), eq("B1"));

    getMockRequestAttributesInterceptor().setRequestAttributes(frAttributes);
    final GetConfigurationResponse frResponse =
        unauthenticatedServiceStub().getConfiguration(GetConfigurationRequest.newBuilder().build());
    assertFalse(frResponse.hasEtagMatched());
    verify(badgeTranslator, times(2)).translate(any(), eq("B1"));

  }

}
