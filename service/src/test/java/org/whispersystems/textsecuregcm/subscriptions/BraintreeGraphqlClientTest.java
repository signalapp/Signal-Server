/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.subscriptions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.braintree.graphql.clientoperation.CreatePayPalOneTimePaymentMutation;
import java.math.BigDecimal;
import java.net.http.HttpHeaders;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import javax.ws.rs.ServiceUnavailableException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.http.FaultTolerantHttpClient;

class BraintreeGraphqlClientTest {

  private static final String CURRENCY = "xts";
  private static final String RETURN_URL = "https://example.com/return";
  private static final String CANCEL_URL = "https://example.com/cancel";
  private static final String LOCALE = "xx";

  private FaultTolerantHttpClient httpClient;
  private BraintreeGraphqlClient braintreeGraphqlClient;


  @BeforeEach
  void setUp() {
    httpClient = mock(FaultTolerantHttpClient.class);

    braintreeGraphqlClient = new BraintreeGraphqlClient(httpClient, "https://example.com", "public", "super-secret");
  }

  @Test
  void createPayPalOneTimePayment() {

    final HttpResponse<Object> response = mock(HttpResponse.class);
    when(httpClient.sendAsync(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(response));

    final String paymentId = "PAYID-AAA1AAAA1A11111AA111111A";
    when(response.body())
        .thenReturn(createPayPalOneTimePaymentResponse(paymentId));
    when(response.statusCode())
        .thenReturn(200);

    final CompletableFuture<CreatePayPalOneTimePaymentMutation.CreatePayPalOneTimePayment> future = braintreeGraphqlClient.createPayPalOneTimePayment(
        BigDecimal.ONE, CURRENCY,
        RETURN_URL, CANCEL_URL, LOCALE);

    assertTimeoutPreemptively(Duration.ofSeconds(3), () -> {
      final CreatePayPalOneTimePaymentMutation.CreatePayPalOneTimePayment result = future.get();

      assertEquals(paymentId, result.paymentId);
      assertNotNull(result.approvalUrl);
    });
  }

  @Test
  void createPayPalOneTimePaymentHttpError() {

    final HttpResponse<Object> response = mock(HttpResponse.class);
    when(httpClient.sendAsync(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(response));

    when(response.statusCode())
        .thenReturn(500);
    final HttpHeaders httpheaders = mock(HttpHeaders.class);
    when(httpheaders.firstValue(any())).thenReturn(Optional.empty());
    when(response.headers())
        .thenReturn(httpheaders);

    final CompletableFuture<CreatePayPalOneTimePaymentMutation.CreatePayPalOneTimePayment> future = braintreeGraphqlClient.createPayPalOneTimePayment(
        BigDecimal.ONE, CURRENCY,
        RETURN_URL, CANCEL_URL, LOCALE);

    assertTimeoutPreemptively(Duration.ofSeconds(3), () -> {

      final ExecutionException e = assertThrows(ExecutionException.class, future::get);

      assertTrue(e.getCause() instanceof ServiceUnavailableException);
    });
  }

  @Test
  void createPayPalOneTimePaymentGraphQlError() {

    final HttpResponse<Object> response = mock(HttpResponse.class);
    when(httpClient.sendAsync(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(response));

    when(response.body())
        .thenReturn(createErrorResponse("createPayPalOneTimePayment", "12345"));
    when(response.statusCode())
        .thenReturn(200);

    final CompletableFuture<CreatePayPalOneTimePaymentMutation.CreatePayPalOneTimePayment> future = braintreeGraphqlClient.createPayPalOneTimePayment(
        BigDecimal.ONE, CURRENCY,
        RETURN_URL, CANCEL_URL, LOCALE);

    assertTimeoutPreemptively(Duration.ofSeconds(3), () -> {

      final ExecutionException e = assertThrows(ExecutionException.class, future::get);
      assertTrue(e.getCause() instanceof ServiceUnavailableException);
    });
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
