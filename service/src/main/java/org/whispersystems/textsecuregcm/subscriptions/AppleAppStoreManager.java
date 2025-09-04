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
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.storage.PaymentTime;
import org.whispersystems.textsecuregcm.util.ResilienceUtil;

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
      @Nullable final String retryConfigurationName) {
    this(new AppStoreServerAPIClient(encodedKey, keyId, issuerId, bundleId, env),
        new SignedDataVerifier(decodeRootCerts(base64AppleRootCerts), bundleId, appAppleId, env, true),
        subscriptionGroupId, productIdToLevel, retryConfigurationName);
  }

  @VisibleForTesting
  AppleAppStoreManager(
      final AppStoreServerAPIClient apiClient,
      final SignedDataVerifier signedDataVerifier,
      final String subscriptionGroupId,
      final Map<String, Long> productIdToLevel,
      @Nullable final String retryConfigurationName) {
    this.apiClient = apiClient;
    this.signedDataVerifier = signedDataVerifier;
    this.subscriptionGroupId = subscriptionGroupId;
    this.productIdToLevel = productIdToLevel;
    this.retry = ResilienceUtil.getRetryRegistry().retry("appstore-retry", RetryConfig
        .<HttpResponse<?>>from(Optional.ofNullable(retryConfigurationName)
            .flatMap(name -> ResilienceUtil.getRetryRegistry().getConfiguration(name))
            .orElseGet(() -> ResilienceUtil.getRetryRegistry().getDefaultConfig()))
        .retryOnException(AppleAppStoreManager::shouldRetry).build());
  }

  @Override
  public PaymentProvider getProvider() {
    return PaymentProvider.APPLE_APP_STORE;
  }


  /**
   * Check if the subscription with the provided originalTransactionId is valid.
   *
   * @param originalTransactionId The originalTransactionId associated with the subscription
   * @return the subscription level of the valid transaction.
   * @throws RateLimitExceededException            If rate-limited
   * @throws SubscriptionNotFoundException        If the provided originalTransactionId was not found
   * @throws SubscriptionPaymentRequiredException If the originalTransactionId exists but is in a state that does not
   *                                               grant the user an entitlement
   * @throws SubscriptionInvalidArgumentsException If the transaction is valid but does not contain a subscription
   */
  public Long validateTransaction(final String originalTransactionId)
      throws SubscriptionInvalidArgumentsException, RateLimitExceededException, SubscriptionNotFoundException, SubscriptionPaymentRequiredException {
    final DecodedTransaction tx = lookupAndValidateTransaction(originalTransactionId);
    if (!isSubscriptionActive(tx)) {
      throw new SubscriptionPaymentRequiredException();
    }
    return getLevel(tx);
  }


  /**
   * Cancel the subscription
   * <p>
   * The App Store does not support backend cancellation, so this does not actually cancel, but it does verify that the
   * user has no active subscriptions. End-users must cancel their subscription directly through storekit before calling
   * this method.
   *
   * @param originalTransactionId The originalTransactionId associated with the subscription
   * @throws RateLimitExceededException            If rate-limited
   * @throws SubscriptionInvalidArgumentsException If the transaction is valid but does not contain a subscription, or
   *                                                the transaction has not already been cancelled with storekit
   */
  @Override
  public void cancelAllActiveSubscriptions(String originalTransactionId)
      throws SubscriptionInvalidArgumentsException, RateLimitExceededException {
    try {
      final DecodedTransaction tx = lookup(originalTransactionId);
      if (tx.signedTransaction.getStatus() != Status.EXPIRED &&
          tx.signedTransaction.getStatus() != Status.REVOKED &&
          tx.renewalInfo.getAutoRenewStatus() != AutoRenewStatus.OFF) {
        throw new SubscriptionInvalidArgumentsException("must cancel subscription with storekit before deleting");
      }
    } catch (SubscriptionNotFoundException _) {
      // If the subscription is not found there is no need to do anything, so we can squash it
    }
    // The subscription will not auto-renew, so we can stop tracking it
  }

  @Override
  public SubscriptionInformation getSubscriptionInformation(final String originalTransactionId)
      throws RateLimitExceededException, SubscriptionNotFoundException {
    final DecodedTransaction tx = lookup(originalTransactionId);
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
  }


  @Override
  public ReceiptItem getReceiptItem(String originalTransactionId)
      throws RateLimitExceededException, SubscriptionNotFoundException, SubscriptionPaymentRequiredException {
    final DecodedTransaction tx = lookup(originalTransactionId);
    if (!isSubscriptionActive(tx)) {
      throw new SubscriptionPaymentRequiredException();
    }

    // A new transactionId might be generated if you restore a subscription on a new device. webOrderLineItemId is
    // guaranteed not to change for a specific renewal purchase.
    // See: https://developer.apple.com/documentation/appstoreservernotifications/weborderlineitemid
    final String itemId = tx.transaction.getWebOrderLineItemId();
    final PaymentTime paymentTime = PaymentTime.periodEnds(Instant.ofEpochMilli(tx.transaction.getExpiresDate()));

    return new ReceiptItem(itemId, paymentTime, getLevel(tx));

  }

  private DecodedTransaction lookup(final String originalTransactionId)
      throws RateLimitExceededException, SubscriptionNotFoundException {
    try {
      return lookupAndValidateTransaction(originalTransactionId);
    } catch (SubscriptionInvalidArgumentsException e) {
      // Shouldn't happen because we previously validated this transactionId before storing it
      throw new UncheckedIOException(new IOException(e));
    }
  }

  private DecodedTransaction lookupAndValidateTransaction(final String originalTransactionId)
      throws SubscriptionInvalidArgumentsException, RateLimitExceededException, SubscriptionNotFoundException {
    final StatusResponse statuses = getAllSubscriptions(originalTransactionId);
    final SubscriptionGroupIdentifierItem item = statuses.getData().stream()
        .filter(s -> subscriptionGroupId.equals(s.getSubscriptionGroupIdentifier())).findFirst()
        .orElseThrow(() -> new SubscriptionInvalidArgumentsException("transaction did not contain a backup subscription", null));

    final List<DecodedTransaction> txs = item.getLastTransactions().stream()
        .map(this::decode)
        .filter(decoded -> productIdToLevel.containsKey(decoded.transaction.getProductId()))
        .toList();

    if (txs.isEmpty()) {
      throw new SubscriptionInvalidArgumentsException("transactionId did not include a paid subscription", null);
    }

    if (txs.size() > 1) {
      logger.warn("Multiple matching product transactions found for transactionId {}, only considering first",
          originalTransactionId);
    }

    if (!originalTransactionId.equals(txs.getFirst().signedTransaction.getOriginalTransactionId())) {
      // Get All Subscriptions only requires that the transaction be some transaction associated with the
      // subscription. This is too flexible, since we'd like to key on the originalTransactionId in the
      // SubscriptionManager.
      throw new SubscriptionInvalidArgumentsException("transactionId was not the transaction's originalTransactionId", null);
    }
    return txs.getFirst();
  }

  private StatusResponse getAllSubscriptions(final String originalTransactionId)
      throws SubscriptionNotFoundException, SubscriptionInvalidArgumentsException, RateLimitExceededException {
    try {
      return retry.executeCallable(() -> {
        try {
          return apiClient.getAllSubscriptionStatuses(originalTransactionId, EMPTY_STATUSES);
        } catch (final APIException e) {
          Metrics.counter(GET_SUBSCRIPTION_ERROR_COUNTER_NAME, "reason", e.getApiError().name()).increment();
          throw switch (e.getApiError()) {
            case ORIGINAL_TRANSACTION_ID_NOT_FOUND, TRANSACTION_ID_NOT_FOUND -> new SubscriptionNotFoundException();
            case RATE_LIMIT_EXCEEDED -> new RateLimitExceededException(null);
            case INVALID_ORIGINAL_TRANSACTION_ID -> new SubscriptionInvalidArgumentsException(e.getApiErrorMessage());
            default -> e;
          };
        } catch (final IOException e) {
          Metrics.counter(GET_SUBSCRIPTION_ERROR_COUNTER_NAME, "reason", "io_error").increment();
          throw e;
        }
      });
    } catch (SubscriptionNotFoundException | SubscriptionInvalidArgumentsException | RateLimitExceededException e) {
      throw e;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (APIException e) {
      throw new UncheckedIOException(new IOException(e));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static boolean shouldRetry(Throwable e) {
    return e instanceof APIException apiException && switch (apiException.getApiError()) {
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
      throw new UncheckedIOException(new IOException("Failed to verify payload from App Store Server", e));
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
      throw new UncheckedIOException(new IOException(
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
