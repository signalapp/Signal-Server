/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;
import static org.whispersystems.textsecuregcm.grpc.GrpcTestUtils.assertStatusException;

import io.grpc.Status;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.signal.chat.payments.GetCurrencyConversionsRequest;
import org.signal.chat.payments.GetCurrencyConversionsResponse;
import org.signal.chat.payments.PaymentsGrpc;
import org.whispersystems.textsecuregcm.currency.CurrencyConversionManager;
import org.whispersystems.textsecuregcm.entities.CurrencyConversionEntity;
import org.whispersystems.textsecuregcm.entities.CurrencyConversionEntityList;

class PaymentsGrpcServiceTest extends SimpleBaseGrpcTest<PaymentsGrpcService, PaymentsGrpc.PaymentsBlockingStub> {

  @Mock
  private CurrencyConversionManager currencyManager;

  @Override
  protected PaymentsGrpcService createServiceBeforeEachTest() {
    return new PaymentsGrpcService(currencyManager);
  }

  @Test
  void testGetCurrencyConversions() {
    final long timestamp = System.currentTimeMillis();
    when(currencyManager.getCurrencyConversions()).thenReturn(Optional.of(
        new CurrencyConversionEntityList(List.of(
            new CurrencyConversionEntity("FOO", Map.of(
                "USD", new BigDecimal("2.35"),
                "EUR", new BigDecimal("1.89")
            )),
            new CurrencyConversionEntity("BAR", Map.of(
                "USD", new BigDecimal("1.50"),
                "EUR", new BigDecimal("0.98")
            ))
        ), timestamp)));

    final GetCurrencyConversionsResponse currencyConversions = authenticatedServiceStub().getCurrencyConversions(
        GetCurrencyConversionsRequest.newBuilder().build());

    assertEquals(timestamp, currencyConversions.getTimestamp());
    assertEquals(2, currencyConversions.getCurrenciesCount());
    assertEquals("FOO", currencyConversions.getCurrencies(0).getBase());
    assertEquals("2.35", currencyConversions.getCurrencies(0).getConversionsMap().get("USD"));
  }

  @Test
  void testUnavailable() {
    when(currencyManager.getCurrencyConversions()).thenReturn(Optional.empty());
    assertStatusException(Status.UNAVAILABLE, () -> authenticatedServiceStub().getCurrencyConversions(
        GetCurrencyConversionsRequest.newBuilder().build()));
  }

  @Test
  public void testUnauthenticated() throws Exception {
    assertStatusException(Status.UNAUTHENTICATED, () -> unauthenticatedServiceStub().getCurrencyConversions(
        GetCurrencyConversionsRequest.newBuilder().build()));
  }
}
