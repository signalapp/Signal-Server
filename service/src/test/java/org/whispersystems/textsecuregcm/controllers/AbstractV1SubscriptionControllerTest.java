/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Clock;
import org.signal.libsignal.zkgroup.receipts.ServerZkReceiptOperations;
import org.whispersystems.textsecuregcm.configuration.OneTimeDonationConfiguration;
import org.whispersystems.textsecuregcm.configuration.SubscriptionConfiguration;
import org.whispersystems.textsecuregcm.storage.IssuedReceiptsManager;
import org.whispersystems.textsecuregcm.subscriptions.BraintreeManager;
import org.whispersystems.textsecuregcm.subscriptions.PaymentProvider;
import org.whispersystems.textsecuregcm.subscriptions.StripeManager;
import org.whispersystems.textsecuregcm.util.MockUtils;
import org.whispersystems.textsecuregcm.util.SystemMapper;

class AbstractV1SubscriptionControllerTest {

  static final Clock CLOCK = mock(Clock.class);

  private static final ObjectMapper YAML_MAPPER = SystemMapper.yamlMapper();

  static final OneTimeDonationConfiguration ONETIME_CONFIG = ConfigHelper.getOneTimeConfig();
  static final StripeManager STRIPE_MANAGER = MockUtils.buildMock(StripeManager.class, mgr ->
      when(mgr.getProvider()).thenReturn(PaymentProvider.STRIPE));
  static final BraintreeManager BRAINTREE_MANAGER = MockUtils.buildMock(BraintreeManager.class, mgr ->
      when(mgr.getProvider()).thenReturn(PaymentProvider.BRAINTREE));
  static final IssuedReceiptsManager ISSUED_RECEIPTS_MANAGER = mock(IssuedReceiptsManager.class);

  static final ServerZkReceiptOperations ZK_OPS = mock(ServerZkReceiptOperations.class);


  /**
   * Encapsulates {@code static} configuration, to keep the class header simpler and avoid illegal forward references
   */
  record ConfigHelper() {

    static SubscriptionConfiguration getSubscriptionConfig() {
      return readValue(SUBSCRIPTION_CONFIG_YAML, SubscriptionConfiguration.class);
    }

    static OneTimeDonationConfiguration getOneTimeConfig() {
      return readValue(ONETIME_CONFIG_YAML, OneTimeDonationConfiguration.class);
    }

    private static <T> T readValue(String yaml, Class<T> type) {
      try {
        return YAML_MAPPER.readValue(yaml, type);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private static final String SUBSCRIPTION_CONFIG_YAML = """
        badgeExpiration: P30D
        badgeGracePeriod: P15D
        backupExpiration: P3D
        backupGracePeriod: P10D
        backupFreeTierMediaDuration: P30D
        backupLevels:
          201:
            playProductId: testPlayProductId
            mediaTtl: P40D
            prices:
              usd:
                amount: '5'
                processorIds:
                  STRIPE: R4
                  BRAINTREE: M4
              jpy:
                amount: '500'
                processorIds:
                  STRIPE: Q4
                  BRAINTREE: N4
              bif:
                amount: '5000'
                processorIds:
                  STRIPE: S4
                  BRAINTREE: O4
              eur:
                amount: '5'
                processorIds:
                  STRIPE: A4
                  BRAINTREE: B4
        levels:
          5:
            badge: B1
            prices:
              usd:
                amount: '5'
                processorIds:
                  STRIPE: R1
                  BRAINTREE: M1
              jpy:
                amount: '500'
                processorIds:
                  STRIPE: Q1
                  BRAINTREE: N1
              bif:
                amount: '5000'
                processorIds:
                  STRIPE: S1
                  BRAINTREE: O1
              eur:
                amount: '5'
                processorIds:
                  STRIPE: A1
                  BRAINTREE: B1
          15:
            badge: B2
            prices:
              usd:
                amount: '15'
                processorIds:
                  STRIPE: R2
                  BRAINTREE: M2
              jpy:
                amount: '1500'
                processorIds:
                  STRIPE: Q2
                  BRAINTREE: N2
              bif:
                amount: '15000'
                processorIds:
                  STRIPE: S2
                  BRAINTREE: O2
              eur:
                amount: '15'
                processorIds:
                  STRIPE: A2
                  BRAINTREE: B2
          35:
            badge: B3
            prices:
              usd:
                amount: '35'
                processorIds:
                  STRIPE: R3
                  BRAINTREE: M3
              jpy:
                amount: '3500'
                processorIds:
                  STRIPE: Q3
                  BRAINTREE: N3
              bif:
                amount: '35000'
                processorIds:
                  STRIPE: S3
                  BRAINTREE: O3
              eur:
                amount: '35'
                processorIds:
                  STRIPE: A3
                  BRAINTREE: B3
        """;

    private static final String ONETIME_CONFIG_YAML = """
        boost:
          level: 1
          expiration: P45D
          badge: BOOST
        gift:
          level: 100
          expiration: P60D
          badge: GIFT
        currencies:
          usd:
            minimum: '2.50' # fractional to test BigDecimal conversion
            gift: '20'
            boosts:
              - '5.50'
              - '6'
              - '7'
              - '8'
              - '9'
              - '10'
          eur:
            minimum: '3'
            gift: '5'
            boosts:
              - '5'
              - '10'
              - '20'
              - '30'
              - '50'
              - '100'
          jpy:
            minimum: '250'
            gift: '2000'
            boosts:
              - '550'
              - '600'
              - '700'
              - '800'
              - '900'
              - '1000'
          bif:
            minimum: '2500'
            gift: '20000'
            boosts:
              - '5500'
              - '6000'
              - '7000'
              - '8000'
              - '9000'
              - '10000'
        sepaMaximumEuros: '10000'
        """;

  }

}
