/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.subscriptions;

import com.braintreegateway.BraintreeGateway;
import com.braintreegateway.ResourceCollection;
import com.braintreegateway.Transaction;
import com.braintreegateway.TransactionSearchRequest;
import com.braintreegateway.exceptions.NotFoundException;
import java.math.BigDecimal;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.http.FaultTolerantHttpClient;

public class BraintreeManager implements SubscriptionProcessorManager {

  private static final Logger logger = LoggerFactory.getLogger(BraintreeManager.class);

  private static final String PAYPAL_PAYMENT_ALREADY_COMPLETED_PROCESSOR_CODE = "2094";
  private final BraintreeGateway braintreeGateway;
  private final BraintreeGraphqlClient braintreeGraphqlClient;
  private final Executor executor;
  private final Set<String> supportedCurrencies;
  private final Map<String, String> currenciesToMerchantAccounts;

  public BraintreeManager(final String braintreeMerchantId, final String braintreePublicKey,
      final String braintreePrivateKey,
      final String braintreeEnvironment,
      final Set<String> supportedCurrencies,
      final Map<String, String> currenciesToMerchantAccounts,
      final String graphqlUri,
      final CircuitBreakerConfiguration circuitBreakerConfiguration,
      final Executor executor) {

    this.braintreeGateway = new BraintreeGateway(braintreeEnvironment, braintreeMerchantId, braintreePublicKey,
        braintreePrivateKey);
    this.supportedCurrencies = supportedCurrencies;
    this.currenciesToMerchantAccounts = currenciesToMerchantAccounts;

    final FaultTolerantHttpClient httpClient = FaultTolerantHttpClient.newBuilder()
        .withName("braintree-graphql")
        .withCircuitBreaker(circuitBreakerConfiguration)
        .withExecutor(executor)
        .build();
    this.braintreeGraphqlClient = new BraintreeGraphqlClient(httpClient, graphqlUri, braintreePublicKey,
        braintreePrivateKey);
    this.executor = executor;
  }

  @Override
  public Set<String> getSupportedCurrencies() {
    return supportedCurrencies;
  }

  @Override
  public SubscriptionProcessor getProcessor() {
    return SubscriptionProcessor.BRAINTREE;
  }

  @Override
  public boolean supportsPaymentMethod(final PaymentMethod paymentMethod) {
    return paymentMethod == PaymentMethod.PAYPAL;
  }

  @Override
  public boolean supportsCurrency(final String currency) {
    return supportedCurrencies.contains(currency.toLowerCase(Locale.ROOT));
  }


  @Override
  public CompletableFuture<PaymentDetails> getPaymentDetails(final String paymentId) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        final Transaction transaction = braintreeGateway.transaction().find(paymentId);

        return new PaymentDetails(transaction.getGraphQLId(),
            transaction.getCustomFields(),
            getPaymentStatus(transaction.getStatus()),
            transaction.getCreatedAt().toInstant());

      } catch (final NotFoundException e) {
        return null;
      }
    }, executor);
  }

  @Override
  public CompletableFuture<ProcessorCustomer> createCustomer(final byte[] subscriberUser) {
    return CompletableFuture.failedFuture(new BadRequestException("Unsupported"));
  }

  @Override
  public CompletableFuture<String> createPaymentMethodSetupToken(final String customerId) {
    return CompletableFuture.failedFuture(new BadRequestException("Unsupported"));
  }

  public CompletableFuture<PayPalOneTimePaymentApprovalDetails> createOneTimePayment(String currency, long amount,
      String locale, String returnUrl, String cancelUrl) {
    return braintreeGraphqlClient.createPayPalOneTimePayment(convertApiAmountToBraintreeAmount(currency, amount),
            currency.toUpperCase(Locale.ROOT), returnUrl,
            cancelUrl, locale)
        .thenApply(result -> new PayPalOneTimePaymentApprovalDetails((String) result.approvalUrl, result.paymentId));
  }

  public CompletableFuture<PayPalChargeSuccessDetails> captureOneTimePayment(String payerId, String paymentId,
      String paymentToken, String currency, long amount, long level) {
    return braintreeGraphqlClient.tokenizePayPalOneTimePayment(payerId, paymentId, paymentToken)
        .thenCompose(response -> braintreeGraphqlClient.chargeOneTimePayment(
                response.paymentMethod.id,
                convertApiAmountToBraintreeAmount(currency, amount),
                currenciesToMerchantAccounts.get(currency.toLowerCase(Locale.ROOT)),
                level)
            .thenComposeAsync(chargeResponse -> {

              final PaymentStatus paymentStatus = getPaymentStatus(chargeResponse.transaction.status);
              if (paymentStatus == PaymentStatus.SUCCEEDED || paymentStatus == PaymentStatus.PROCESSING) {
                return CompletableFuture.completedFuture(new PayPalChargeSuccessDetails(chargeResponse.transaction.id));
              }

              // the GraphQL/Apollo interfaces are a tad unwieldy for this type of status checking
              final Transaction unsuccessfulTx = braintreeGateway.transaction().find(chargeResponse.transaction.id);

              if (PAYPAL_PAYMENT_ALREADY_COMPLETED_PROCESSOR_CODE.equals(unsuccessfulTx.getProcessorResponseCode())
                  || Transaction.GatewayRejectionReason.DUPLICATE.equals(
                  unsuccessfulTx.getGatewayRejectionReason())) {
                // the payment has already been charged - maybe a previous call timed out or was interrupted -
                // in any case, check for a successful transaction with the paymentId
                final ResourceCollection<Transaction> search = braintreeGateway.transaction()
                    .search(new TransactionSearchRequest()
                        .paypalPaymentId().is(paymentId)
                        .status().in(
                            Transaction.Status.SETTLED,
                            Transaction.Status.SETTLING,
                            Transaction.Status.SUBMITTED_FOR_SETTLEMENT,
                            Transaction.Status.SETTLEMENT_PENDING
                        )
                    );

                if (search.getMaximumSize() == 0) {
                  return CompletableFuture.failedFuture(
                      new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR));
                }

                final Transaction successfulTx = search.getFirst();

                return CompletableFuture.completedFuture(
                    new PayPalChargeSuccessDetails(successfulTx.getGraphQLId()));
              }

              logger.info("PayPal charge unexpectedly failed: {}", unsuccessfulTx.getProcessorResponseCode());

              return CompletableFuture.failedFuture(
                  new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR));

            }, executor));
  }

  private static PaymentStatus getPaymentStatus(Transaction.Status status) {
    return switch (status) {
      case SETTLEMENT_CONFIRMED, SETTLING, SUBMITTED_FOR_SETTLEMENT, SETTLED -> PaymentStatus.SUCCEEDED;
      case AUTHORIZATION_EXPIRED, GATEWAY_REJECTED, PROCESSOR_DECLINED, SETTLEMENT_DECLINED, VOIDED, FAILED ->
          PaymentStatus.FAILED;
      default -> PaymentStatus.UNKNOWN;
    };
  }

  private static PaymentStatus getPaymentStatus(com.braintree.graphql.client.type.PaymentStatus status) {
    try {
      Transaction.Status transactionStatus = Transaction.Status.valueOf(status.rawValue);

      return getPaymentStatus(transactionStatus);
    } catch (final Exception e) {
      return PaymentStatus.UNKNOWN;
    }
  }

  private BigDecimal convertApiAmountToBraintreeAmount(final String currency, final long amount) {
    return switch (currency.toLowerCase(Locale.ROOT)) {
      // JPY is the only supported zero-decimal currency
      case "jpy" -> BigDecimal.valueOf(amount);
      default -> BigDecimal.valueOf(amount).scaleByPowerOfTen(-2);
    };
  }

  public record PayPalOneTimePaymentApprovalDetails(String approvalUrl, String paymentId) {

  }

  public record PayPalChargeSuccessDetails(String paymentId) {

  }

}
