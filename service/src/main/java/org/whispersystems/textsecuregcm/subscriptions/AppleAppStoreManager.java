/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.subscriptions;

import com.apple.itunes.storekit.client.APIException;
import com.apple.itunes.storekit.client.AppStoreServerAPIClient;
import com.apple.itunes.storekit.model.AutoRenewStatus;
import com.apple.itunes.storekit.model.Environment;
import com.apple.itunes.storekit.model.JWSRenewalInfoDecodedPayload;
import com.apple.itunes.storekit.model.JWSTransactionDecodedPayload;
import com.apple.itunes.storekit.model.LastTransactionsItem;
import com.apple.itunes.storekit.model.Status;
import com.apple.itunes.storekit.model.StatusResponse;
import com.apple.itunes.storekit.model.SubscriptionGroupIdentifierItem;
import com.apple.itunes.storekit.verification.SignedDataVerifier;
import com.apple.itunes.storekit.verification.VerificationException;
import com.google.common.annotations.VisibleForTesting;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.micrometer.core.instrument.Metrics;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.storage.PaymentTime;
import org.whispersystems.textsecuregcm.storage.SubscriptionException;
import org.whispersystems.textsecuregcm.util.ResilienceUtil;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;

/**
 * Manages subscriptions made with the Apple App Store
 * <p>
 * Clients create a subscription using storekit directly, and then notify us about their subscription with their
 * subscription's <a
 * href="https://developer.apple.com/documentation/appstoreserverapi/originaltransactionid">originalTransactionId</a>.
 */
public class AppleAppStoreManager implements SubscriptionPaymentProcessor {

  private static final Logger logger = LoggerFactory.getLogger(AppleAppStoreManager.class);

  private final AppStoreServerAPIClient apiClient;
  private final SignedDataVerifier signedDataVerifier;
  private final ExecutorService executor;
  private final ScheduledExecutorService retryExecutor;
  private final Map<String, Long> productIdToLevel;

  private static final Status[] EMPTY_STATUSES = new Status[0];

  private static final String GET_SUBSCRIPTION_ERROR_COUNTER_NAME =
      MetricsUtil.name(AppleAppStoreManager.class, "getSubscriptionsError");

  private final String subscriptionGroupId;
  private final Retry retry;


  public AppleAppStoreManager(
      final Environment env,
      final String bundleId,
      final long appAppleId,
      final String issuerId,
      final String keyId,
      final String encodedKey,
      final String subscriptionGroupId,
      final Map<String, Long> productIdToLevel,
      final List<String> base64AppleRootCerts,
      @Nullable final String retryConfigurationName,
      final ExecutorService executor,
      final ScheduledExecutorService retryExecutor) {
    this(new AppStoreServerAPIClient(encodedKey, keyId, issuerId, bundleId, env),
        new SignedDataVerifier(decodeRootCerts(base64AppleRootCerts), bundleId, appAppleId, env, true),
        subscriptionGroupId, productIdToLevel, retryConfigurationName, executor, retryExecutor);
  }

  @VisibleForTesting
  AppleAppStoreManager(
      final AppStoreServerAPIClient apiClient,
      final SignedDataVerifier signedDataVerifier,
      final String subscriptionGroupId,
      final Map<String, Long> productIdToLevel,
      @Nullable final String retryConfigurationName,
      final ExecutorService executor,
      final ScheduledExecutorService retryExecutor) {
    this.apiClient = apiClient;
    this.signedDataVerifier = signedDataVerifier;
    this.subscriptionGroupId = subscriptionGroupId;
    this.productIdToLevel = productIdToLevel;
    this.executor = Objects.requireNonNull(executor);
    this.retryExecutor = Objects.requireNonNull(retryExecutor);

    final RetryConfig.Builder<HttpResponse<?>> retryConfigBuilder =
        RetryConfig.from(Optional.ofNullable(retryConfigurationName)
            .flatMap(name -> ResilienceUtil.getRetryRegistry().getConfiguration(name))
            .orElseGet(() -> ResilienceUtil.getRetryRegistry().getDefaultConfig()));

    retryConfigBuilder.retryOnException(AppleAppStoreManager::shouldRetry);

    this.retry = ResilienceUtil.getRetryRegistry()
        .retry(ResilienceUtil.name(AppleAppStoreManager.class, "appstore-retry"), retryConfigBuilder.build());
  }

  @Override
  public PaymentProvider getProvider() {
    return PaymentProvider.APPLE_APP_STORE;
  }


  /**
   * Check if the subscription with the provided originalTransactionId is valid.
   *
   * @param originalTransactionId The originalTransactionId associated with the subscription
   * @return A stage that completes successfully when the transaction has been validated, or fails if the token does not
   * represent an active subscription.
   */
  public CompletableFuture<Long> validateTransaction(final String originalTransactionId) {
    return lookup(originalTransactionId).thenApplyAsync(tx -> {
      if (!isSubscriptionActive(tx)) {
        throw ExceptionUtils.wrap(new SubscriptionException.PaymentRequired());
      }
      return getLevel(tx);
    }, executor);
  }


  /**
   * Cancel the subscription
   * <p>
   * The App Store does not support backend cancellation, so this does not actually cancel, but it does verify that the
   * user has no active subscriptions. End-users must cancel their subscription directly through storekit before calling
   * this method.
   *
   * @param originalTransactionId The originalTransactionId associated with the subscription
   * @return A stage that completes when the subscription has successfully been cancelled
   */
  @Override
  public CompletableFuture<Void> cancelAllActiveSubscriptions(String originalTransactionId) {
    return lookup(originalTransactionId).thenApplyAsync(tx -> {
      if (tx.signedTransaction.getStatus() != Status.EXPIRED &&
          tx.signedTransaction.getStatus() != Status.REVOKED &&
          tx.renewalInfo.getAutoRenewStatus() != AutoRenewStatus.OFF) {
        throw ExceptionUtils.wrap(
            new SubscriptionException.InvalidArguments("must cancel subscription with storekit before deleting"));
      }
      // The subscription will not auto-renew, so we can stop tracking it
      return null;
    }, executor);
  }

  @Override
  public CompletableFuture<SubscriptionInformation> getSubscriptionInformation(final String originalTransactionId) {
    return lookup(originalTransactionId).thenApplyAsync(tx -> {

      final SubscriptionStatus status = switch (tx.signedTransaction.getStatus()) {
        case ACTIVE -> SubscriptionStatus.ACTIVE;
        case BILLING_RETRY -> SubscriptionStatus.PAST_DUE;
        case BILLING_GRACE_PERIOD -> SubscriptionStatus.UNPAID;
        case EXPIRED, REVOKED -> SubscriptionStatus.CANCELED;
      };

      return new SubscriptionInformation(
          getSubscriptionPrice(tx),
          getLevel(tx),
          Instant.ofEpochMilli(tx.transaction.getOriginalPurchaseDate()),
          Instant.ofEpochMilli(tx.transaction.getExpiresDate()),
          isSubscriptionActive(tx),
          tx.renewalInfo.getAutoRenewStatus() == AutoRenewStatus.OFF,
          status,
          PaymentProvider.APPLE_APP_STORE,
          PaymentMethod.APPLE_APP_STORE,
          false,
          null);
    }, executor);
  }


  @Override
  public CompletableFuture<ReceiptItem> getReceiptItem(String originalTransactionId) {
    return lookup(originalTransactionId).thenApplyAsync(tx -> {
      if (!isSubscriptionActive(tx)) {
        throw ExceptionUtils.wrap(new SubscriptionException.PaymentRequired());
      }

      // A new transactionId might be generated if you restore a subscription on a new device. webOrderLineItemId is
      // guaranteed not to change for a specific renewal purchase.
      // See: https://developer.apple.com/documentation/appstoreservernotifications/weborderlineitemid
      final String itemId = tx.transaction.getWebOrderLineItemId();
      final PaymentTime paymentTime = PaymentTime.periodEnds(Instant.ofEpochMilli(tx.transaction.getExpiresDate()));

      return new ReceiptItem(itemId, paymentTime, getLevel(tx));

    }, executor);
  }

  private CompletableFuture<DecodedTransaction> lookup(final String originalTransactionId) {
    return getAllSubscriptions(originalTransactionId).thenApplyAsync(statuses -> {

      final SubscriptionGroupIdentifierItem item = statuses.getData().stream()
          .filter(s -> subscriptionGroupId.equals(s.getSubscriptionGroupIdentifier())).findFirst()
          .orElseThrow(() -> ExceptionUtils.wrap(
              new SubscriptionException.InvalidArguments("transaction did not contain a backup subscription", null)));

      final List<DecodedTransaction> txs = item.getLastTransactions().stream()
          .map(this::decode)
          .filter(decoded -> productIdToLevel.containsKey(decoded.transaction.getProductId()))
          .toList();

      if (txs.isEmpty()) {
        throw ExceptionUtils.wrap(
            new SubscriptionException.InvalidArguments("transactionId did not include a paid subscription", null));
      }

      if (txs.size() > 1) {
        logger.warn("Multiple matching product transactions found for transactionId {}, only considering first",
            originalTransactionId);
      }

      if (!originalTransactionId.equals(txs.getFirst().signedTransaction.getOriginalTransactionId())) {
        // Get All Subscriptions only requires that the transaction be some transaction associated with the
        // subscription. This is too flexible, since we'd like to key on the originalTransactionId in the
        // SubscriptionManager.
        throw ExceptionUtils.wrap(
            new SubscriptionException.InvalidArguments(
                "transactionId was not the transaction's originalTransactionId", null));
      }

      return txs.getFirst();
    }, executor).toCompletableFuture();
  }

  private CompletionStage<StatusResponse> getAllSubscriptions(final String originalTransactionId) {
    Supplier<CompletionStage<StatusResponse>> supplier = () -> CompletableFuture.supplyAsync(() -> {
      try {
        return apiClient.getAllSubscriptionStatuses(originalTransactionId, EMPTY_STATUSES);
      } catch (final APIException e) {
        Metrics.counter(GET_SUBSCRIPTION_ERROR_COUNTER_NAME, "reason", e.getApiError().name()).increment();
        throw ExceptionUtils.wrap(switch (e.getApiError()) {
          case ORIGINAL_TRANSACTION_ID_NOT_FOUND, TRANSACTION_ID_NOT_FOUND -> new SubscriptionException.NotFound();
          case RATE_LIMIT_EXCEEDED -> new RateLimitExceededException(null);
          case INVALID_ORIGINAL_TRANSACTION_ID -> new SubscriptionException.InvalidArguments(e.getApiErrorMessage());
          default -> e;
        });
      } catch (final IOException e) {
        Metrics.counter(GET_SUBSCRIPTION_ERROR_COUNTER_NAME, "reason", "io_error").increment();
        throw ExceptionUtils.wrap(e);
      }
    }, executor);
    return retry.executeCompletionStage(retryExecutor, supplier);
  }

  private static boolean shouldRetry(Throwable e) {
    return ExceptionUtils.unwrap(e) instanceof APIException apiException && switch (apiException.getApiError()) {
      case ORIGINAL_TRANSACTION_ID_NOT_FOUND_RETRYABLE, GENERAL_INTERNAL_RETRYABLE, APP_NOT_FOUND_RETRYABLE -> true;
      default -> false;
    };
  }

  private record DecodedTransaction(
      LastTransactionsItem signedTransaction,
      JWSTransactionDecodedPayload transaction,
      JWSRenewalInfoDecodedPayload renewalInfo) {}

  /**
   * Verify signature and decode transaction payloads
   */
  private DecodedTransaction decode(final LastTransactionsItem tx) {
    try {
      return new DecodedTransaction(
          tx,
          signedDataVerifier.verifyAndDecodeTransaction(tx.getSignedTransactionInfo()),
          signedDataVerifier.verifyAndDecodeRenewalInfo(tx.getSignedRenewalInfo()));
    } catch (VerificationException e) {
      throw ExceptionUtils.wrap(new IOException("Failed to verify payload from App Store Server", e));
    }
  }

  private SubscriptionPrice getSubscriptionPrice(final DecodedTransaction tx) {
    final BigDecimal amount = new BigDecimal(tx.transaction.getPrice()).scaleByPowerOfTen(-3);
    return new SubscriptionPrice(
        tx.transaction.getCurrency().toUpperCase(Locale.ROOT),
        SubscriptionCurrencyUtil.convertConfiguredAmountToApiAmount(tx.transaction.getCurrency(), amount));
  }

  private long getLevel(final DecodedTransaction tx) {
    final Long level = productIdToLevel.get(tx.transaction.getProductId());
    if (level == null) {
      throw ExceptionUtils.wrap(
          new SubscriptionException.InvalidArguments(
              "Transaction for unknown productId " + tx.transaction.getProductId()));
    }
    return level;
  }

  /**
   * Return true if the subscription's entitlement can currently be granted
   */
  private boolean isSubscriptionActive(final DecodedTransaction tx) {
    return tx.signedTransaction.getStatus() == Status.ACTIVE
        || tx.signedTransaction.getStatus() == Status.BILLING_GRACE_PERIOD;
  }

  private static Set<InputStream> decodeRootCerts(final List<String> rootCerts) {
    return rootCerts.stream()
        .map(Base64.getDecoder()::decode)
        .map(ByteArrayInputStream::new)
        .collect(Collectors.toSet());
  }

}
