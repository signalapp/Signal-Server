/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.subscriptions;

import com.braintreegateway.BraintreeGateway;
import com.braintreegateway.ClientTokenRequest;
import com.braintreegateway.Customer;
import com.braintreegateway.CustomerRequest;
import com.braintreegateway.Plan;
import com.braintreegateway.ResourceCollection;
import com.braintreegateway.Result;
import com.braintreegateway.Subscription;
import com.braintreegateway.SubscriptionRequest;
import com.braintreegateway.Transaction;
import com.braintreegateway.TransactionSearchRequest;
import com.braintreegateway.exceptions.BraintreeException;
import com.braintreegateway.exceptions.NotFoundException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import javax.annotation.Nullable;
import javax.ws.rs.ClientErrorException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import org.apache.commons.codec.binary.Hex;
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

  private static SubscriptionStatus getSubscriptionStatus(final Subscription.Status status) {
    return switch (status) {
      case ACTIVE -> SubscriptionStatus.ACTIVE;
      case CANCELED, EXPIRED -> SubscriptionStatus.CANCELED;
      case PAST_DUE -> SubscriptionStatus.PAST_DUE;
      case PENDING -> SubscriptionStatus.INCOMPLETE;
      case UNRECOGNIZED -> {
        logger.error("Subscription has unrecognized status; library may need to be updated: {}", status);
        yield SubscriptionStatus.UNKNOWN;
      }
    };
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

  private void assertResultSuccess(Result<?> result) throws CompletionException {
    if (!result.isSuccess()) {
      throw new CompletionException(new BraintreeException(result.getMessage()));
    }
  }

  @Override
  public CompletableFuture<ProcessorCustomer> createCustomer(final byte[] subscriberUser) {
    return CompletableFuture.supplyAsync(() -> {
          final CustomerRequest request = new CustomerRequest()
                  .customField("subscriber_user", Hex.encodeHexString(subscriberUser));
          try {
            return braintreeGateway.customer().create(request);
          } catch (BraintreeException e) {
            throw new CompletionException(e);
          }
        }, executor)
        .thenApply(result -> {
          assertResultSuccess(result);

          return new ProcessorCustomer(result.getTarget().getId(), SubscriptionProcessor.BRAINTREE);
        });

  }

  @Override
  public CompletableFuture<String> createPaymentMethodSetupToken(final String customerId) {
    return CompletableFuture.supplyAsync(() -> {
      ClientTokenRequest request = new ClientTokenRequest()
          .customerId(customerId);

      return braintreeGateway.clientToken().generate(request);
    }, executor);
  }

  @Override
  public CompletableFuture<Void> setDefaultPaymentMethodForCustomer(String customerId, String billingAgreementToken,
      @Nullable String currentSubscriptionId) {
    final Optional<String> maybeSubscriptionId = Optional.ofNullable(currentSubscriptionId);
    return braintreeGraphqlClient.tokenizePayPalBillingAgreement(billingAgreementToken)
        .thenCompose(tokenizePayPalBillingAgreement ->
            braintreeGraphqlClient.vaultPaymentMethod(customerId, tokenizePayPalBillingAgreement.paymentMethod.id))
        .thenApplyAsync(vaultPaymentMethod -> braintreeGateway.customer()
                .update(customerId, new CustomerRequest()
                    .defaultPaymentMethodToken(vaultPaymentMethod.paymentMethod.id)),
            executor)
        .thenAcceptAsync(result -> {
          maybeSubscriptionId.ifPresent(
              subscriptionId -> braintreeGateway.subscription()
                  .update(subscriptionId, new SubscriptionRequest()
                      .paymentMethodToken(result.getTarget().getDefaultPaymentMethod().getToken())));
        }, executor);
  }

  @Override
  public CompletableFuture<Object> getSubscription(String subscriptionId) {
    return CompletableFuture.supplyAsync(() -> braintreeGateway.subscription().find(subscriptionId), executor);
  }

  @Override
  public CompletableFuture<SubscriptionId> createSubscription(String customerId, String planId, long level,
                                                              long lastSubscriptionCreatedAt) {

    return getDefaultPaymentMethod(customerId)
        .thenCompose(paymentMethod -> {
          if (paymentMethod == null) {
            throw new ClientErrorException(Response.Status.CONFLICT);
          }

          final Optional<Subscription> maybeExistingSubscription = paymentMethod.getSubscriptions().stream()
              .filter(sub -> sub.getStatus().equals(Subscription.Status.ACTIVE))
              .filter(Subscription::neverExpires)
              .findAny();

          final CompletableFuture<Plan> planFuture = maybeExistingSubscription.map(sub ->
              findPlan(sub.getPlanId()).thenApply(plan -> {
                if (getLevelForPlan(plan) != level) {
                  // if this happens, the likely cause is retrying an apparently failed request (likely some sort of timeout or network interruption)
                  // with a different level.
                  // In this case, it’s safer and easier to recover by returning this subscription, rather than
                  // returning an error
                  logger.warn("existing subscription had unexpected level");
                }
                return plan;
              })).orElseGet(() -> findPlan(planId));

          return maybeExistingSubscription
              .map(subscription -> {
                return findPlan(subscription.getPlanId())
                    .thenApply(plan -> {
                      if (getLevelForPlan(plan) != level) {
                        // if this happens, the likely cause is retrying an apparently failed request (likely some sort of timeout or network interruption)
                        // with a different level.
                        // In this case, it’s safer and easier to recover by returning this subscription, rather than
                        // returning an error
                        logger.warn("existing subscription had unexpected level");
                      }
                      return subscription;
                    });
              })
              .orElseGet(() -> findPlan(planId).thenApplyAsync(plan -> {
                final Result<Subscription> result = braintreeGateway.subscription().create(new SubscriptionRequest()
                    .planId(planId)
                    .paymentMethodToken(paymentMethod.getToken())
                    .merchantAccountId(
                        currenciesToMerchantAccounts.get(plan.getCurrencyIsoCode().toLowerCase(Locale.ROOT)))
                    .options()
                    .startImmediately(true)
                    .done()
                );

                assertResultSuccess(result);

                return result.getTarget();
              }));
        }).thenApply(subscription -> new SubscriptionId(subscription.getId()));
  }

  private CompletableFuture<com.braintreegateway.PaymentMethod> getDefaultPaymentMethod(String customerId) {
    return CompletableFuture.supplyAsync(() -> braintreeGateway.customer().find(customerId).getDefaultPaymentMethod(),
        executor);
  }


  @Override
  public CompletableFuture<SubscriptionId> updateSubscription(Object subscriptionObj, String planId, long level,
      String idempotencyKey) {

    if (!(subscriptionObj instanceof final Subscription subscription)) {
      throw new IllegalArgumentException("invalid subscription object: " + subscriptionObj.getClass().getName());
    }

    // since badge redemption is untrackable by design and unrevokable, subscription changes must be immediate and
    // and not prorated. Braintree subscriptions cannot change their next billing date,
    // so we must end the existing one and create a new one
    return cancelSubscriptionAtEndOfCurrentPeriod(subscription)
        .thenCompose(ignored -> {

          final Transaction transaction = getLatestTransactionForSubscription(subscription).orElseThrow(
              () -> new ClientErrorException(
                  Response.Status.CONFLICT));

          final Customer customer = transaction.getCustomer();

          return createSubscription(customer.getId(), planId, level,
              subscription.getCreatedAt().toInstant().getEpochSecond());
        });
  }

  @Override
  public CompletableFuture<Long> getLevelForSubscription(Object subscriptionObj) {
    final Subscription subscription = getSubscription(subscriptionObj);

    return findPlan(subscription.getPlanId())
        .thenApply(this::getLevelForPlan);
  }

  private CompletableFuture<Plan> findPlan(String planId) {
    return CompletableFuture.supplyAsync(() -> braintreeGateway.plan().find(planId), executor);
  }

  private long getLevelForPlan(final Plan plan) {
    final BraintreePlanMetadata metadata;
    try {
      metadata = new ObjectMapper().readValue(plan.getDescription(), BraintreePlanMetadata.class);

    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    return metadata.level();
  }

  @Override
  public CompletableFuture<SubscriptionInformation> getSubscriptionInformation(Object subscriptionObj) {
    final Subscription subscription = getSubscription(subscriptionObj);

    return CompletableFuture.supplyAsync(() -> {

      final Plan plan = braintreeGateway.plan().find(subscription.getPlanId());

      final long level = getLevelForPlan(plan);

      final Instant anchor = subscription.getFirstBillingDate().toInstant();
      final Instant endOfCurrentPeriod = subscription.getBillingPeriodEndDate().toInstant();

      final Optional<Transaction> maybeTransaction = getLatestTransactionForSubscription(subscription);

      final ChargeFailure chargeFailure = maybeTransaction.map(transaction -> {

        if (getPaymentStatus(transaction.getStatus()).equals(PaymentStatus.SUCCEEDED)) {
          return null;
        }

        final String code;
        final String message;
        if (transaction.getProcessorResponseCode() != null) {
          code = transaction.getProcessorResponseCode();
          message = transaction.getProcessorResponseText();
        } else if (transaction.getGatewayRejectionReason() != null) {
          code = "gateway";
          message = transaction.getGatewayRejectionReason().toString();
        } else {
          code = "unknown";
          message = "unknown";
        }

        return new ChargeFailure(
                code,
                message,
                null,
                null,
                null);

      }).orElse(null);


      return new SubscriptionInformation(
          new SubscriptionPrice(plan.getCurrencyIsoCode().toUpperCase(Locale.ROOT),
              SubscriptionCurrencyUtil.convertBraintreeAmountToApiAmount(plan.getCurrencyIsoCode(), plan.getPrice())),
          level,
          anchor,
          endOfCurrentPeriod,
          Subscription.Status.ACTIVE == subscription.getStatus(),
          !subscription.neverExpires(),
          getSubscriptionStatus(subscription.getStatus()),
          chargeFailure
      );
    }, executor);
  }

  @Override
  public CompletableFuture<Void> cancelAllActiveSubscriptions(String customerId) {

    return CompletableFuture.supplyAsync(() -> braintreeGateway.customer().find(customerId), executor).thenCompose(customer -> {

      final List<CompletableFuture<Void>> subscriptionCancelFutures = customer.getDefaultPaymentMethod().getSubscriptions().stream()
              .map(this::cancelSubscriptionAtEndOfCurrentPeriod)
              .toList();

      return CompletableFuture.allOf(subscriptionCancelFutures.toArray(new CompletableFuture[0]));
    });
  }

  private CompletableFuture<Void> cancelSubscriptionAtEndOfCurrentPeriod(Subscription subscription) {
    return CompletableFuture.supplyAsync(() -> {
      braintreeGateway.subscription().update(subscription.getId(),
              new SubscriptionRequest().numberOfBillingCycles(subscription.getCurrentBillingCycle()));
      return null;
    }, executor);
  }


  @Override
  public CompletableFuture<ReceiptItem> getReceiptItem(String subscriptionId) {

    return getLatestTransactionForSubscription(subscriptionId).thenApply(maybeTransaction -> maybeTransaction.map(transaction -> {

      if (!getPaymentStatus(transaction.getStatus()).equals(PaymentStatus.SUCCEEDED)) {
        throw new WebApplicationException(Response.Status.PAYMENT_REQUIRED);
      }

      final Instant expiration = transaction.getSubscriptionDetails().getBillingPeriodEndDate().toInstant();
      final Plan plan = braintreeGateway.plan().find(transaction.getPlanId());

      final BraintreePlanMetadata metadata;
      try {
        metadata = new ObjectMapper().readValue(plan.getDescription(), BraintreePlanMetadata.class);

      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }

      return new ReceiptItem(transaction.getId(), expiration, metadata.level());

    }).orElseThrow(() -> new WebApplicationException(Response.Status.NO_CONTENT)));
  }

  private static Subscription getSubscription(Object subscriptionObj) {
    if (!(subscriptionObj instanceof final Subscription subscription)) {
      throw new IllegalArgumentException("Invalid subscription object: " + subscriptionObj.getClass().getName());
    }
    return subscription;
  }

  public CompletableFuture<Optional<Transaction>> getLatestTransactionForSubscription(String subscriptionId) {
    return getSubscription(subscriptionId)
            .thenApply(BraintreeManager::getSubscription)
            .thenApply(this::getLatestTransactionForSubscription);
  }

  private Optional<Transaction> getLatestTransactionForSubscription(Subscription subscription) {
    return subscription.getTransactions().stream()
            .max(Comparator.comparing(Transaction::getCreatedAt));
  }

  public CompletableFuture<PayPalBillingAgreementApprovalDetails> createPayPalBillingAgreement(final String returnUrl,
                                                                                               final String cancelUrl, final String locale) {
    return braintreeGraphqlClient.createPayPalBillingAgreement(returnUrl, cancelUrl, locale)
            .thenApply(response ->
                    new PayPalBillingAgreementApprovalDetails((String) response.approvalUrl, response.billingAgreementToken)
            );
  }

  public record PayPalBillingAgreementApprovalDetails(String approvalUrl, String billingAgreementToken) {

  }
}
