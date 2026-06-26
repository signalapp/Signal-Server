/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.subscriptions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.braintree.graphql.clientoperation.CreatePayPalOneTimePaymentMutation;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.http.HttpHeaders;
import java.net.http.HttpResponse;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.whispersystems.textsecuregcm.http.FaultTolerantHttpClient;

@ExtendWith(MockitoExtension.class)
class BraintreeGraphqlClientTest {

  private static final String CURRENCY = "xts";
  private static final String RETURN_URL = "https://example.com/return";
  private static final String CANCEL_URL = "https://example.com/cancel";
  private static final String LOCALE = "xx";
  private static final String LOCALIZED_LINE_ITEM_NAME = "Donation to Signal Technology Foundation";

  @Mock
  private FaultTolerantHttpClient httpClient;

  @Mock
  private HttpResponse<Object> response;

  private BraintreeGraphqlClient braintreeGraphqlClient;


  @BeforeEach
  void setUp() throws IOException {
    lenient().when(httpClient.send(any(), any())).thenReturn(response);

    braintreeGraphqlClient = new BraintreeGraphqlClient(httpClient, "https://example.com", "public", "super-secret");
  }

  @Test
  void createPayPalOneTimePayment() throws IOException {
    final String paymentId = "PAYID-AAA1AAAA1A11111AA111111A";
    when(response.body())
        .thenReturn(createPayPalOneTimePaymentResponse(paymentId));
    when(response.statusCode())
        .thenReturn(200);

    final CreatePayPalOneTimePaymentMutation.CreatePayPalOneTimePayment result = braintreeGraphqlClient.createPayPalOneTimePayment(
        BigDecimal.ONE, CURRENCY,
        RETURN_URL, CANCEL_URL, LOCALE, LOCALIZED_LINE_ITEM_NAME);

      assertEquals(paymentId, result.paymentId);
      assertNotNull(result.approvalUrl);
  }

  @Test
  void createPayPalOneTimePaymentHttpError() throws IOException {
    when(response.statusCode())
        .thenReturn(500);
    final HttpHeaders httpheaders = mock(HttpHeaders.class);
    when(httpheaders.firstValue(any())).thenReturn(Optional.empty());
    when(response.headers())
        .thenReturn(httpheaders);

    assertThrows(IOException.class, () -> braintreeGraphqlClient.createPayPalOneTimePayment(
        BigDecimal.ONE, CURRENCY,
        RETURN_URL, CANCEL_URL, LOCALE, LOCALIZED_LINE_ITEM_NAME));
  }

  @Test
  void createPayPalOneTimePaymentGraphQlError() {
    when(response.body())
        .thenReturn(createErrorResponse("createPayPalOneTimePayment", "12345"));
    when(response.statusCode())
        .thenReturn(200);

    assertThrows(IOException.class, () ->  braintreeGraphqlClient.createPayPalOneTimePayment(
        BigDecimal.ONE, CURRENCY,
        RETURN_URL, CANCEL_URL, LOCALE, LOCALIZED_LINE_ITEM_NAME));
  }

  private String createPayPalOneTimePaymentResponse(final String paymentId) {
    final String cannedToken = "EC-1AA11111AA111111A";
    return String.format("""
        {
          "data": {
            "createPayPalOneTimePayment": {
              "approvalUrl": "https://www.sandbox.paypal.com/checkoutnow?nolegacy=1&token=%2$s",
              "paymentId": "%1$s"
            }
          },
          "extensions": {
            "requestId": "%3$s"
          }
        }
        """, paymentId, cannedToken, UUID.randomUUID());
  }

  private String createErrorResponse(final String operationName, final String legacyCode) {
    return String.format("""
        {
          "data": {
            "%1$s": null
          },
          "errors": [ {
            "message": "This is a test error message.",
            "locations": [ {
              "line": 2,
              "column": 7
             } ],
            "path": [ "%1$s" ],
            "extensions": {
              "errorType": "user_error",
              "errorClass": "VALIDATION",
              "legacyCode": "%2$s",
              "inputPath": [ "input", "testField" ]
            }
          }],
          "extensions": {
            "requestId": "%3$s"
          }
        }
        """, operationName, legacyCode, UUID.randomUUID());
  }

  @Test
  void tokenizePayPalOneTimePayment() {
  }

  @Test
  void chargeOneTimePayment() {
  }
}
