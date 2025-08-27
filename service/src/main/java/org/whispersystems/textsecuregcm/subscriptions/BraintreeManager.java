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
import com.google.cloud.pubsub.v1.PublisherInterface;
import com.google.common.annotations.VisibleForTesting;
import com.google.pubsub.v1.PubsubMessage;
import io.micrometer.core.instrument.Metrics;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.currency.CurrencyConversionManager;
import org.whispersystems.textsecuregcm.http.FaultTolerantHttpClient;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.storage.PaymentTime;
import org.whispersystems.textsecuregcm.storage.SubscriptionException;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import org.whispersystems.textsecuregcm.util.GoogleApiUtil;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.ua.ClientPlatform;

public class BraintreeManager implements CustomerAwareSubscriptionPaymentProcessor {

  private static final Logger logger = LoggerFactory.getLogger(BraintreeManager.class);

  private static final String GENERIC_DECLINED_PROCESSOR_CODE = "2046";
  private static final String PAYPAL_FUNDING_INSTRUMENT_DECLINED_PROCESSOR_CODE = "2074";
  private static final String PAYPAL_PAYMENT_ALREADY_COMPLETED_PROCESSOR_CODE = "2094";

  private static final BigDecimal ONE_MILLION = BigDecimal.valueOf(1_000_000);

  private final BraintreeGateway braintreeGateway;
  private final BraintreeGraphqlClient braintreeGraphqlClient;
  private final CurrencyConversionManager currencyConversionManager;
  private final PublisherInterface pubsubPublisher;
  private final Executor executor;
  private final Map<PaymentMethod, Set<String>> supportedCurrenciesByPaymentMethod;
  private final Map<String, String> currenciesToMerchantAccounts;

  private final String PUBSUB_MESSAGE_COUNTER_NAME = MetricsUtil.name(BraintreeManager.class, "pubSubMessage");

  public BraintreeManager(final String braintreeMerchantId, final String braintreePublicKey,
      final String braintreePrivateKey,
      final String braintreeEnvironment,
      final Map<PaymentMethod, Set<String>> supportedCurrenciesByPaymentMethod,
      final Map<String, String> currenciesToMerchantAccounts,
      final String graphqlUri,
      final CurrencyConversionManager currencyConversionManager,
      final PublisherInterface pubsubPublisher,
      @Nullable final String circuitBreakerConfigurationName,
      final Executor executor,
      final ScheduledExecutorService retryExecutor) {

    this(new BraintreeGateway(braintreeEnvironment, braintreeMerchantId, braintreePublicKey,
            braintreePrivateKey),
        supportedCurrenciesByPaymentMethod,
        currenciesToMerchantAccounts,
        new BraintreeGraphqlClient(FaultTolerantHttpClient.newBuilder("braintree-graphql", executor)
            .withCircuitBreaker(circuitBreakerConfigurationName)
            // Braintree documents its internal timeout at 60 seconds, and we want to make sure we don’t miss
            // a response
            // https://developer.paypal.com/braintree/docs/reference/general/best-practices/java#timeouts
            .withRequestTimeout(Duration.ofSeconds(70))
            .build(), graphqlUri, braintreePublicKey, braintreePrivateKey),
        currencyConversionManager,
        pubsubPublisher,
        executor);
  }

  @VisibleForTesting
  BraintreeManager(final BraintreeGateway braintreeGateway,
      final Map<PaymentMethod, Set<String>> supportedCurrenciesByPaymentMethod,
      final Map<String, String> currenciesToMerchantAccounts, final BraintreeGraphqlClient braintreeGraphqlClient,
      final CurrencyConversionManager currencyConversionManager, final PublisherInterface pubsubPublisher,
      final Executor executor) {
    this.braintreeGateway = braintreeGateway;
    this.supportedCurrenciesByPaymentMethod = supportedCurrenciesByPaymentMethod;
    this.currenciesToMerchantAccounts = currenciesToMerchantAccounts;
    this.braintreeGraphqlClient = braintreeGraphqlClient;
    this.currencyConversionManager = currencyConversionManager;
    this.pubsubPublisher = pubsubPublisher;
    this.executor = executor;
  }

  @Override
  public Set<String> getSupportedCurrenciesForPaymentMethod(final PaymentMethod paymentMethod) {
    return supportedCurrenciesByPaymentMethod.getOrDefault(paymentMethod, Collections.emptySet());
  }

  @Override
  public PaymentProvider getProvider() {
    return PaymentProvider.BRAINTREE;
  }

  @Override
  public boolean supportsPaymentMethod(final PaymentMethod paymentMethod) {
    return paymentMethod == PaymentMethod.PAYPAL;
  }

  public CompletableFuture<PaymentDetails> getPaymentDetails(final String paymentId) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        final Transaction transaction = braintreeGateway.transaction().find(paymentId);
        ChargeFailure chargeFailure = null;
        if (!getPaymentStatus(transaction.getStatus()).equals(PaymentStatus.SUCCEEDED)) {
          chargeFailure = createChargeFailure(transaction);
        }
        return new PaymentDetails(transaction.getGraphQLId(),
            transaction.getCustomFields(),
            getPaymentStatus(transaction.getStatus()),
            transaction.getCreatedAt().toInstant(),
            chargeFailure);

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
      String paymentToken, String currency, long amount, long level, @Nullable ClientPlatform clientPlatform) {
    return braintreeGraphqlClient.tokenizePayPalOneTimePayment(payerId, paymentId, paymentToken)
        .thenCompose(response -> braintreeGraphqlClient.chargeOneTimePayment(
                response.paymentMethod.id,
                convertApiAmountToBraintreeAmount(currency, amount),
                currenciesToMerchantAccounts.get(currency.toLowerCase(Locale.ROOT)),
                level)
            .thenComposeAsync(chargeResponse -> {

              final PaymentStatus paymentStatus = getPaymentStatus(chargeResponse.transaction.status);
              if (paymentStatus == PaymentStatus.SUCCEEDED || paymentStatus == PaymentStatus.PROCESSING) {
                publishDonationEvent(amount, currency, Instant.now(), clientPlatform);
                return CompletableFuture.completedFuture(new PayPalChargeSuccessDetails(chargeResponse.transaction.id));
              }

              // the GraphQL/Apollo interfaces are a tad unwieldy for this type of status checking
              final Transaction unsuccessfulTx = braintreeGateway.transaction().find(chargeResponse.transaction.id);

              if (PAYPAL_PAYMENT_ALREADY_COMPLETED_PROCESSOR_CODE.equals(unsuccessfulTx.getProcessorResponseCode())
                  || Transaction.GatewayRejectionReason.DUPLICATE.equals(unsuccessfulTx.getGatewayRejectionReason())) {
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
                  return CompletableFuture.failedFuture(ExceptionUtils.wrap(new IOException()));
                }

                final Transaction successfulTx = search.getFirst();

                publishDonationEvent(amount, currency, successfulTx.getCreatedAt().toInstant(), clientPlatform);

                return CompletableFuture.completedFuture(
                    new PayPalChargeSuccessDetails(successfulTx.getGraphQLId()));
              }

              return switch (unsuccessfulTx.getProcessorResponseCode()) {
                case GENERIC_DECLINED_PROCESSOR_CODE, PAYPAL_FUNDING_INSTRUMENT_DECLINED_PROCESSOR_CODE ->
                    CompletableFuture.failedFuture(
                        new SubscriptionException.ProcessorException(getProvider(), createChargeFailure(unsuccessfulTx)));

                default -> {
                  logger.info("PayPal charge unexpectedly failed: {}", unsuccessfulTx.getProcessorResponseCode());

                  yield CompletableFuture.failedFuture(ExceptionUtils.wrap(new IOException()));
                }
              };
            }, executor));
  }

  private void publishDonationEvent(final long amount,
      final String currency,
      final Instant timestamp,
      @Nullable final ClientPlatform clientPlatform) {

    try {
      final BigDecimal originalAmount = convertApiAmountToBraintreeAmount(currency, amount);

      final BigDecimal originalAmountUsd =
          currencyConversionManager.convertToUsd(originalAmount, currency)
              .orElseThrow(() -> new IllegalArgumentException("Could not convert to USD from " + currency));

      final DonationsPubsub.DonationPubSubMessage.Builder donationPubSubMessageBuilder =
          DonationsPubsub.DonationPubSubMessage.newBuilder()
              .setTimestamp(timestamp.toEpochMilli() * 1000)
              .setSource("app")
              .setProvider("braintree")
              .setRecurring(false)
              .setPaymentMethodType("paypal")
              .setOriginalAmountMicros(toMicros(originalAmount))
              .setOriginalCurrency(currency)
              .setOriginalAmountUsdMicros(toMicros(originalAmountUsd));

      if (clientPlatform != null) {
        donationPubSubMessageBuilder.setClientPlatform(clientPlatform.name().toLowerCase(Locale.ROOT));
      }

      GoogleApiUtil.toCompletableFuture(pubsubPublisher.publish(PubsubMessage.newBuilder()
              .setData(donationPubSubMessageBuilder.build().toByteString())
              .build()), executor)
          .whenComplete((messageId, throwable) -> {
            if (throwable != null) {
              logger.warn("Failed to publish donation pub/sub message", throwable);
            }

            Metrics.counter(PUBSUB_MESSAGE_COUNTER_NAME, "success", String.valueOf(throwable == null))
                .increment();
          });
    } catch (final Exception e) {
      logger.warn("Failed to construct donation pub/sub message", e);
    }
  }

  @VisibleForTesting
  long toMicros(final BigDecimal amount) {
    return amount.multiply(ONE_MILLION).longValueExact();
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

  private static SubscriptionStatus getSubscriptionStatus(final Subscription.Status status, final boolean latestTransactionFailed) {
    return switch (status) {
      // Stripe returns a PAST_DUE status if the subscription's most recent payment failed.
      // This check ensures that Braintree is consistent with Stripe.
      case ACTIVE -> latestTransactionFailed ? SubscriptionStatus.PAST_DUE : SubscriptionStatus.ACTIVE;
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

  @Override
  public CompletableFuture<ProcessorCustomer> createCustomer(final byte[] subscriberUser, @Nullable final ClientPlatform clientPlatform) {
    return CompletableFuture.supplyAsync(() -> {
          CustomerRequest request = new CustomerRequest()
              .customField("subscriber_user", HexFormat.of().formatHex(subscriberUser));

          if (clientPlatform != null) {
            request.customField("client_platform", clientPlatform.name().toLowerCase());
          }

          try {
            return braintreeGateway.customer().create(request);
          } catch (BraintreeException e) {
            throw new CompletionException(e);
          }
        }, executor)
        .thenApply(result -> {
          if (!result.isSuccess()) {
            throw new CompletionException(new BraintreeException(result.getMessage()));
          }

          return new ProcessorCustomer(result.getTarget().getId(), PaymentProvider.BRAINTREE);
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
            throw ExceptionUtils.wrap(new SubscriptionException.ProcessorConflict());
          }

          final Optional<Subscription> maybeExistingSubscription = paymentMethod.getSubscriptions().stream()
              .filter(sub -> sub.getStatus().equals(Subscription.Status.ACTIVE))
              .filter(Subscription::neverExpires)
              .findAny();

          return maybeExistingSubscription.map(subscription -> findPlan(subscription.getPlanId())
                  .thenApply(plan -> {
                    if (getLevelForPlan(plan) != level) {
                      // if this happens, the likely cause is retrying an apparently failed request (likely some sort of timeout or network interruption)
                      // with a different level.
                      // In this case, it’s safer and easier to recover by returning this subscription, rather than
                      // returning an error
                      logger.warn("existing subscription had unexpected level");
                    }
                    return subscription;
                  }))
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

                if (!result.isSuccess()) {
                  final CompletionException completionException;
                  if (result.getTarget() != null) {
                    completionException = result.getTarget().getTransactions().stream().findFirst()
                        .map(transaction -> new CompletionException(
                            new SubscriptionException.ProcessorException(getProvider(), createChargeFailure(transaction))))
                        .orElseGet(() -> new CompletionException(new BraintreeException(result.getMessage())));
                  } else {
                    completionException = new CompletionException(new BraintreeException(result.getMessage()));
                  }

                  throw completionException;
                }

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
    // not prorated. Braintree subscriptions cannot change their next billing date,
    // so we must end the existing one and create a new one
    return endSubscription(subscription)
        .thenCompose(ignored -> {

          final Transaction transaction = getLatestTransactionForSubscription(subscription)
              .orElseThrow(() -> ExceptionUtils.wrap(new SubscriptionException.ProcessorConflict()));

          final Customer customer = transaction.getCustomer();

          return createSubscription(customer.getId(), planId, level,
              subscription.getCreatedAt().toInstant().getEpochSecond());
        });
  }

  @Override
  public CompletableFuture<LevelAndCurrency> getLevelAndCurrencyForSubscription(Object subscriptionObj) {
    final Subscription subscription = getSubscription(subscriptionObj);

    return findPlan(subscription.getPlanId())
        .thenApply(
            plan -> new LevelAndCurrency(getLevelForPlan(plan), plan.getCurrencyIsoCode().toLowerCase(Locale.ROOT)));

  }

  private CompletableFuture<Plan> findPlan(String planId) {
    return CompletableFuture.supplyAsync(() -> braintreeGateway.plan().find(planId), executor);
  }

  private long getLevelForPlan(final Plan plan) {
    final BraintreePlanMetadata metadata;
    try {
      metadata = SystemMapper.jsonMapper().readValue(plan.getDescription(), BraintreePlanMetadata.class);

    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    return metadata.level();
  }

  @Override
  public CompletableFuture<SubscriptionInformation> getSubscriptionInformation(final String subscriptionId) {
    return getSubscription(subscriptionId).thenApplyAsync(subscriptionObj -> {
      final Subscription subscription = getSubscription(subscriptionObj);

      final Plan plan = braintreeGateway.plan().find(subscription.getPlanId());

      final long level = getLevelForPlan(plan);

      final Instant anchor = subscription.getFirstBillingDate().toInstant();
      final Instant endOfCurrentPeriod = subscription.getBillingPeriodEndDate().toInstant();

      final TransactionInfo latestTransactionInfo = getLatestTransactionForSubscription(subscription)
          .map(this::getTransactionInfo)
          .orElse(new TransactionInfo(PaymentMethod.PAYPAL, false, false, null));

      return new SubscriptionInformation(
          new SubscriptionPrice(plan.getCurrencyIsoCode().toUpperCase(Locale.ROOT),
              SubscriptionCurrencyUtil.convertBraintreeAmountToApiAmount(plan.getCurrencyIsoCode(), plan.getPrice())),
          level,
          anchor,
          endOfCurrentPeriod,
          Subscription.Status.ACTIVE == subscription.getStatus(),
          !subscription.neverExpires(),
          getSubscriptionStatus(subscription.getStatus(), latestTransactionInfo.transactionFailed()),
          PaymentProvider.BRAINTREE,
          latestTransactionInfo.paymentMethod(),
          latestTransactionInfo.paymentProcessing(),
          latestTransactionInfo.chargeFailure()
      );

    }, executor);
  }

  private record TransactionInfo(
      PaymentMethod paymentMethod,
      boolean paymentProcessing,
      boolean transactionFailed,
      @Nullable ChargeFailure chargeFailure) {}

  private TransactionInfo getTransactionInfo(final Transaction transaction) {
    final boolean paymentProcessing = isPaymentProcessing(transaction.getStatus());
    final PaymentMethod paymentMethod = getPaymentMethodFromTransaction(transaction);
    if (getPaymentStatus(transaction.getStatus()) != PaymentStatus.SUCCEEDED) {
      return new TransactionInfo(paymentMethod, paymentProcessing, true, createChargeFailure(transaction));
    }
    return new TransactionInfo(paymentMethod, paymentProcessing, false, null);
  }

  private PaymentMethod getPaymentMethodFromTransaction(Transaction transaction) {
    if (transaction.getPayPalDetails() != null) {
      return PaymentMethod.PAYPAL;
    }
    logger.error("Unexpected payment method from Braintree: {}, transaction id {}", transaction.getPaymentInstrumentType(), transaction.getId());
    return PaymentMethod.UNKNOWN;
  }

  private static boolean isPaymentProcessing(final Transaction.Status status) {
    return status == Transaction.Status.SETTLEMENT_PENDING;
  }

  private ChargeFailure createChargeFailure(Transaction transaction) {

    final String code;
    final String message;
    if (transaction.getStatus() == Transaction.Status.VOIDED) {
      code = "voided";
      message = "voided";
    } else if (transaction.getProcessorResponseCode() != null) {
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
  }

  @Override
  public CompletableFuture<Void> cancelAllActiveSubscriptions(String customerId) {

    return CompletableFuture.supplyAsync(() -> braintreeGateway.customer().find(customerId), executor).thenCompose(customer -> {

      final List<CompletableFuture<Void>> subscriptionCancelFutures = Optional.ofNullable(customer.getDefaultPaymentMethod())
              .map(com.braintreegateway.PaymentMethod::getSubscriptions)
              .orElse(Collections.emptyList())
              .stream()
              .map(this::endSubscription)
              .toList();

      return CompletableFuture.allOf(subscriptionCancelFutures.toArray(new CompletableFuture[0]));
    });
  }

  private CompletableFuture<Void> endSubscription(Subscription subscription) {
    final boolean latestTransactionFailed = getLatestTransactionForSubscription(subscription)
        .map(this::getTransactionInfo)
        .map(TransactionInfo::transactionFailed)
        .orElse(false);
    return switch (getSubscriptionStatus(subscription.getStatus(), latestTransactionFailed)) {
      // The payment for this period has not processed yet, we should immediately cancel to prevent any payment from
      // going through.
      case INCOMPLETE, PAST_DUE, UNPAID -> cancelSubscriptionImmediately(subscription);
      // Otherwise, set the subscription to cancel at the current period end. If the subscription is active, it may
      // continue to be used until the end of the period.
      default -> cancelSubscriptionAtEndOfCurrentPeriod(subscription);
    };
  }

  private CompletableFuture<Void> cancelSubscriptionAtEndOfCurrentPeriod(Subscription subscription) {
    return CompletableFuture.runAsync(() -> braintreeGateway
        .subscription()
        .update(subscription.getId(),
            new SubscriptionRequest().numberOfBillingCycles(subscription.getCurrentBillingCycle())), executor);
  }

  private CompletableFuture<Void> cancelSubscriptionImmediately(Subscription subscription) {
    return CompletableFuture.runAsync(() -> braintreeGateway.subscription().cancel(subscription.getId()), executor);
  }


  @Override
  public CompletableFuture<ReceiptItem> getReceiptItem(String subscriptionId) {
    return getSubscription(subscriptionId)
        .thenApply(BraintreeManager::getSubscription)
        .thenApply(subscription -> getLatestTransactionForSubscription(subscription)
            .map(transaction -> {
              if (!getPaymentStatus(transaction.getStatus()).equals(PaymentStatus.SUCCEEDED)) {
                final SubscriptionStatus subscriptionStatus = getSubscriptionStatus(subscription.getStatus(), true);
                if (subscriptionStatus.equals(SubscriptionStatus.ACTIVE) || subscriptionStatus.equals(SubscriptionStatus.PAST_DUE)) {
                  throw ExceptionUtils.wrap(new SubscriptionException.ReceiptRequestedForOpenPayment());
                }
                throw ExceptionUtils.wrap(new SubscriptionException.ChargeFailurePaymentRequired(getProvider(), createChargeFailure(transaction)));
              }

              final Instant paidAt = transaction.getSubscriptionDetails().getBillingPeriodStartDate().toInstant();
              final Plan plan = braintreeGateway.plan().find(transaction.getPlanId());

              final BraintreePlanMetadata metadata;
              try {
                metadata = SystemMapper.jsonMapper().readValue(plan.getDescription(), BraintreePlanMetadata.class);

              } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
              }

              return new ReceiptItem(transaction.getId(), PaymentTime.periodStart(paidAt), metadata.level());
            })
            .orElseThrow(() -> ExceptionUtils.wrap(new SubscriptionException.ReceiptRequestedForOpenPayment())));
  }

  private static Subscription getSubscription(Object subscriptionObj) {
    if (!(subscriptionObj instanceof final Subscription subscription)) {
      throw new IllegalArgumentException("Invalid subscription object: " + subscriptionObj.getClass().getName());
    }
    return subscription;
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
