/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.subscriptions;

import com.apple.itunes.storekit.model.AutoRenewStatus;
import com.apple.itunes.storekit.model.Status;
import com.apple.itunes.storekit.model.StatusResponse;
import com.apple.itunes.storekit.model.SubscriptionGroupIdentifierItem;
import io.micrometer.core.instrument.Tags;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.storage.PaymentTime;

/**
 * Manages subscriptions made with the Apple App Store
 * <p>
 * Clients create a subscription using storekit directly, and then notify us about their subscription with their
 * subscription's <a
 * href="https://developer.apple.com/documentation/appstoreserverapi/originaltransactionid">originalTransactionId</a>.
 */
public class AppleAppStoreManager implements SubscriptionPaymentProcessor {

  private static final Logger logger = LoggerFactory.getLogger(AppleAppStoreManager.class);

  private static final String LOOKUP_TYPE_TAG = "lookup_type";

  private final AppleAppStoreClient appleAppStoreClient;
  private final Map<String, Long> productIdToLevel;
  private final String subscriptionGroupId;

  public AppleAppStoreManager(
      AppleAppStoreClient appleAppStoreClient,
      final String subscriptionGroupId,
      final Map<String, Long> productIdToLevel) {
    this.appleAppStoreClient = appleAppStoreClient;
    this.subscriptionGroupId = subscriptionGroupId;
    this.productIdToLevel = productIdToLevel;
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
    final AppleAppStoreDecodedTransaction tx = lookupAndValidateTransaction(originalTransactionId, Tags.of(LOOKUP_TYPE_TAG, "validate"));
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
      final AppleAppStoreDecodedTransaction tx = lookup(originalTransactionId, Tags.of(LOOKUP_TYPE_TAG, "cancel"));
      if (tx.signedTransaction().getStatus() != Status.EXPIRED &&
          tx.signedTransaction().getStatus() != Status.REVOKED &&
          tx.renewalInfo().getAutoRenewStatus() != AutoRenewStatus.OFF) {
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
    final AppleAppStoreDecodedTransaction tx = lookup(originalTransactionId, Tags.of(LOOKUP_TYPE_TAG, "info"));
    final SubscriptionStatus status = switch (tx.signedTransaction().getStatus()) {
      case ACTIVE -> SubscriptionStatus.ACTIVE;
      case BILLING_RETRY -> SubscriptionStatus.PAST_DUE;
      case BILLING_GRACE_PERIOD -> SubscriptionStatus.UNPAID;
      case EXPIRED, REVOKED -> SubscriptionStatus.CANCELED;
    };

    return new SubscriptionInformation(
        getSubscriptionPrice(tx),
        getLevel(tx),
        Instant.ofEpochMilli(tx.transaction().getOriginalPurchaseDate()),
        Instant.ofEpochMilli(tx.transaction().getExpiresDate()),
        isSubscriptionActive(tx),
        tx.renewalInfo().getAutoRenewStatus() == AutoRenewStatus.OFF,
        status,
        PaymentProvider.APPLE_APP_STORE,
        PaymentMethod.APPLE_APP_STORE,
        false,
        null);
  }


  @Override
  public ReceiptItem getReceiptItem(String originalTransactionId)
      throws RateLimitExceededException, SubscriptionNotFoundException, SubscriptionPaymentRequiredException {
    final AppleAppStoreDecodedTransaction tx = lookup(originalTransactionId, Tags.of(LOOKUP_TYPE_TAG, "receipt"));
    if (!isSubscriptionActive(tx)) {
      throw new SubscriptionPaymentRequiredException();
    }

    // A new transactionId might be generated if you restore a subscription on a new device. webOrderLineItemId is
    // guaranteed not to change for a specific renewal purchase.
    // See: https://developer.apple.com/documentation/appstoreservernotifications/weborderlineitemid
    final String itemId = tx.transaction().getWebOrderLineItemId();
    final PaymentTime paymentTime = PaymentTime.periodEnds(Instant.ofEpochMilli(tx.transaction().getExpiresDate()));

    return new ReceiptItem(itemId, paymentTime, getLevel(tx));

  }

  private AppleAppStoreDecodedTransaction lookup(final String originalTransactionId, final Tags tags)
      throws RateLimitExceededException, SubscriptionNotFoundException {
    try {
      return lookupAndValidateTransaction(originalTransactionId, tags);
    } catch (SubscriptionInvalidArgumentsException e) {
      // Shouldn't happen because we previously validated this transactionId before storing it
      throw new UncheckedIOException(new IOException(e));
    }
  }

  private AppleAppStoreDecodedTransaction lookupAndValidateTransaction(final String originalTransactionId, final Tags errorTags)
      throws SubscriptionInvalidArgumentsException, RateLimitExceededException, SubscriptionNotFoundException {
    final StatusResponse statuses = appleAppStoreClient.getAllSubscriptions(originalTransactionId, errorTags);
    final SubscriptionGroupIdentifierItem item = statuses.getData().stream()
        .filter(s -> subscriptionGroupId.equals(s.getSubscriptionGroupIdentifier())).findFirst()
        .orElseThrow(() -> new SubscriptionInvalidArgumentsException("transaction did not contain a backup subscription", null));

    final List<AppleAppStoreDecodedTransaction> txs = item.getLastTransactions().stream()
        .map(txItem -> appleAppStoreClient.verify(statuses.getEnvironment(), txItem))
        .filter(tx -> tx.signedTransaction().getOriginalTransactionId().equals(originalTransactionId))
        .filter(decoded -> productIdToLevel.containsKey(decoded.transaction().getProductId()))
        .toList();

    if (txs.isEmpty()) {
      // Get All Subscriptions only requires that the transaction be some transaction associated with the
      // subscription. This is too flexible, since we'd like to key on the originalTransactionId in the
      // SubscriptionManager.
      throw new SubscriptionInvalidArgumentsException("transactionId did not include a paid subscription or the provided transactionId was not an originalTransactionId", null);
    }

    if (txs.size() > 1) {
      logger.warn("Multiple matching product transactions found with a sha256(originalTransactionId)={}, only considering first",
          sha256(originalTransactionId));
    }
    return txs.getFirst();
  }

  private SubscriptionPrice getSubscriptionPrice(final AppleAppStoreDecodedTransaction tx) {
    final BigDecimal amount = new BigDecimal(tx.transaction().getPrice()).scaleByPowerOfTen(-3);
    return new SubscriptionPrice(
        tx.transaction().getCurrency().toUpperCase(Locale.ROOT),
        SubscriptionCurrencyUtil.convertConfiguredAmountToApiAmount(tx.transaction().getCurrency(), amount));
  }

  private long getLevel(final AppleAppStoreDecodedTransaction tx) {
    final Long level = productIdToLevel.get(tx.transaction().getProductId());
    if (level == null) {
      throw new UncheckedIOException(new IOException(
          "Transaction for unknown productId " + tx.transaction().getProductId()));
    }
    return level;
  }

  /**
   * Return true if the subscription's entitlement can currently be granted
   */
  private boolean isSubscriptionActive(final AppleAppStoreDecodedTransaction tx) {
    return tx.signedTransaction().getStatus() == Status.ACTIVE
        || tx.signedTransaction().getStatus() == Status.BILLING_GRACE_PERIOD;
  }

  private static String sha256(final String input) {
    final MessageDigest sha256;
    try {
      sha256 = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError("Every implementation of the Java platform is required to support SHA-256", e);
    }
    return Base64.getEncoder().encodeToString(sha256.digest(input.getBytes(StandardCharsets.UTF_8)));
  }

}
