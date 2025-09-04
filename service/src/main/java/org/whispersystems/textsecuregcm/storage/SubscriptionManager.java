/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialRequest;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialResponse;
import org.signal.libsignal.zkgroup.receipts.ServerZkReceiptOperations;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.controllers.SubscriptionController;
import org.whispersystems.textsecuregcm.subscriptions.AppleAppStoreManager;
import org.whispersystems.textsecuregcm.subscriptions.CustomerAwareSubscriptionPaymentProcessor;
import org.whispersystems.textsecuregcm.subscriptions.GooglePlayBillingManager;
import org.whispersystems.textsecuregcm.subscriptions.PaymentProvider;
import org.whispersystems.textsecuregcm.subscriptions.ProcessorCustomer;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionInformation;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionPaymentProcessor;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionForbiddenException;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionInvalidArgumentsException;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionInvalidLevelException;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionNotFoundException;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionPaymentRequiredException;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionProcessorConflictException;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionProcessorException;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionReceiptRequestedForOpenPaymentException;
import org.whispersystems.textsecuregcm.util.ua.ClientPlatform;

/**
 * Manages updates to the Subscriptions table and the upstream subscription payment providers.
 * <p>
 * This handles a number of common subscription management operations like adding/removing subscribers and creating ZK
 * receipt credentials for a subscriber's active subscription. Some subscription management operations only apply to
 * certain payment providers. In those cases, the operation will take the payment provider that implements the specific
 * functionality as an argument to the method.
 */
public class SubscriptionManager {

  private final Subscriptions subscriptions;
  private final EnumMap<PaymentProvider, SubscriptionPaymentProcessor> processors;
  private final ServerZkReceiptOperations zkReceiptOperations;
  private final IssuedReceiptsManager issuedReceiptsManager;

  public SubscriptionManager(
      @Nonnull Subscriptions subscriptions,
      @Nonnull List<SubscriptionPaymentProcessor> processors,
      @Nonnull ServerZkReceiptOperations zkReceiptOperations,
      @Nonnull IssuedReceiptsManager issuedReceiptsManager) {
    this.subscriptions = Objects.requireNonNull(subscriptions);
    this.processors = new EnumMap<>(processors.stream()
        .collect(Collectors.toMap(SubscriptionPaymentProcessor::getProvider, Function.identity())));
    this.zkReceiptOperations = Objects.requireNonNull(zkReceiptOperations);
    this.issuedReceiptsManager = Objects.requireNonNull(issuedReceiptsManager);
  }

  /**
   * Cancel a subscription with the upstream payment provider and remove the subscription from the table
   *
   * @param subscriberCredentials Subscriber credentials derived from the subscriberId
   * @throws RateLimitExceededException            if rate-limited
   * @throws SubscriptionNotFoundException         if the provided credentials are incorrect or the subscriber does not
   *                                               exist
   * @throws SubscriptionInvalidArgumentsException if a precondition for cancellation was not met
   */
  public void deleteSubscriber(final SubscriberCredentials subscriberCredentials)
      throws SubscriptionNotFoundException, SubscriptionInvalidArgumentsException, RateLimitExceededException {
    final Subscriptions.GetResult getResult =
        subscriptions.get(subscriberCredentials.subscriberUser(), subscriberCredentials.hmac()).join();
    if (getResult == Subscriptions.GetResult.NOT_STORED
        || getResult == Subscriptions.GetResult.PASSWORD_MISMATCH) {
      throw new SubscriptionNotFoundException();
    }

    // a missing customer ID is OK; it means the subscriber never started to add a payment method, so we can skip cancelling
    if (getResult.record.getProcessorCustomer().isPresent()) {
      final ProcessorCustomer processorCustomer = getResult.record.getProcessorCustomer().get();
      getProcessor(processorCustomer.processor()).cancelAllActiveSubscriptions(processorCustomer.customerId());
    }
    subscriptions.setCanceledAt(subscriberCredentials.subscriberUser(), subscriberCredentials.now()).join();
  }

  /**
   * Create or update a subscriber in the subscriptions table
   * <p>
   * If the subscriber does not exist, a subscriber with the provided credentials will be created. If the subscriber
   * already exists, its last access time will be updated.
   *
   * @param subscriberCredentials Subscriber credentials derived from the subscriberId
   * @throws SubscriptionForbiddenException if the subscriber credentials were incorrect
   */
  public void updateSubscriber(final SubscriberCredentials subscriberCredentials)
      throws SubscriptionForbiddenException {
    final Subscriptions.GetResult getResult =
        subscriptions.get(subscriberCredentials.subscriberUser(), subscriberCredentials.hmac()).join();

    if (getResult == Subscriptions.GetResult.PASSWORD_MISMATCH) {
      throw new SubscriptionForbiddenException("subscriberId mismatch");
    } else if (getResult == Subscriptions.GetResult.NOT_STORED) {
      // create a customer and write it to ddb
      final Subscriptions.Record updatedRecord = subscriptions.create(subscriberCredentials.subscriberUser(),
          subscriberCredentials.hmac(),
          subscriberCredentials.now()).join();
      if (updatedRecord == null) {
        throw new SubscriptionForbiddenException("subscriberId mismatch");
      }
    } else {
      // already exists so just touch access time and return
      subscriptions.accessedAt(subscriberCredentials.subscriberUser(), subscriberCredentials.now()).join();
    }
  }

  public Optional<SubscriptionInformation> getSubscriptionInformation(
      final SubscriberCredentials subscriberCredentials)
      throws SubscriptionForbiddenException, SubscriptionNotFoundException, RateLimitExceededException {
    final Subscriptions.Record record = getSubscriber(subscriberCredentials);
    if (record.subscriptionId == null) {
      return Optional.empty();
    }
    final SubscriptionPaymentProcessor manager = getProcessor(record.processorCustomer.processor());
    return Optional.of(manager.getSubscriptionInformation(record.subscriptionId));
  }

  /**
   * Get the subscriber record
   *
   * @param subscriberCredentials Subscriber credentials derived from the subscriberId
   * @throws SubscriptionForbiddenException if the subscriber credentials were incorrect
   * @throws SubscriptionNotFoundException  if the subscriber did not exist
   */
  public Subscriptions.Record getSubscriber(final SubscriberCredentials subscriberCredentials)
      throws SubscriptionForbiddenException, SubscriptionNotFoundException {
    final Subscriptions.GetResult getResult =
        subscriptions.get(subscriberCredentials.subscriberUser(), subscriberCredentials.hmac()).join();
    if (getResult == Subscriptions.GetResult.PASSWORD_MISMATCH) {
      throw new SubscriptionForbiddenException("subscriberId mismatch");
    } else if (getResult == Subscriptions.GetResult.NOT_STORED) {
      throw new SubscriptionNotFoundException();
    } else {
      return getResult.record;
    }
  }

  public record ReceiptResult(
      ReceiptCredentialResponse receiptCredentialResponse,
      CustomerAwareSubscriptionPaymentProcessor.ReceiptItem receiptItem,
      PaymentProvider paymentProvider) {}

  /**
   * Create a ZK receipt credential for a subscription that can be used to obtain the user entitlement
   *
   * @param subscriberCredentials Subscriber credentials derived from the subscriberId
   * @param request               The ZK Receipt credential request
   * @param expiration            A function that takes a {@link CustomerAwareSubscriptionPaymentProcessor.ReceiptItem}
   *                              and returns the expiration time of the receipt
   * @return the requested ZK receipt credential
   * @throws SubscriptionForbiddenException                      if the subscriber credentials were incorrect
   * @throws SubscriptionNotFoundException                       if the subscriber did not exist or did not have a
   *                                                             subscription attached
   * @throws SubscriptionInvalidArgumentsException               if the receipt credential request failed verification
   * @throws SubscriptionPaymentRequiredException                if the subscription is in a state does not grant the
   *                                                             user an entitlement
   * @throws SubscriptionReceiptRequestedForOpenPaymentException if a receipt was requested while a payment transaction
   *                                                             was still open
   * @throws RateLimitExceededException                          if rate-limited
   */
  public ReceiptResult createReceiptCredentials(
      final SubscriberCredentials subscriberCredentials,
      final SubscriptionController.GetReceiptCredentialsRequest request,
      final Function<CustomerAwareSubscriptionPaymentProcessor.ReceiptItem, Instant> expiration)
      throws SubscriptionForbiddenException, SubscriptionNotFoundException, SubscriptionInvalidArgumentsException, SubscriptionPaymentRequiredException, RateLimitExceededException, SubscriptionReceiptRequestedForOpenPaymentException {
    final Subscriptions.Record record = getSubscriber(subscriberCredentials);
    if (record.subscriptionId == null) {
      throw new SubscriptionNotFoundException();
    }

    ReceiptCredentialRequest receiptCredentialRequest;
    try {
      receiptCredentialRequest = new ReceiptCredentialRequest(request.receiptCredentialRequest());
    } catch (InvalidInputException e) {
      throw new SubscriptionInvalidArgumentsException("invalid receipt credential request", e);
    }

    final PaymentProvider processor = record.getProcessorCustomer().orElseThrow().processor();
    final SubscriptionPaymentProcessor manager = getProcessor(processor);
    final SubscriptionPaymentProcessor.ReceiptItem receipt = manager.getReceiptItem(record.subscriptionId);
    issuedReceiptsManager
        .recordIssuance(receipt.itemId(), manager.getProvider(), receiptCredentialRequest, subscriberCredentials.now())
        .join();
    ReceiptCredentialResponse receiptCredentialResponse;
    try {
      receiptCredentialResponse = zkReceiptOperations.issueReceiptCredential(
          receiptCredentialRequest,
          expiration.apply(receipt).getEpochSecond(),
          receipt.level());
    } catch (VerificationFailedException e) {
      throw new SubscriptionInvalidArgumentsException("receipt credential request failed verification", e);
    }
    return new ReceiptResult(receiptCredentialResponse, receipt, processor);
  }

  /**
   * Add a payment method to a customer in a payment processor and update the table.
   * <p>
   * If the customer does not exist in the table, a customer is created via the subscriptionPaymentProcessor and added
   * to the table. Not all payment processors support server-managed customers, so a payment processor that implements
   * {@link CustomerAwareSubscriptionPaymentProcessor} must be passed in.
   *
   * @param subscriberCredentials        Subscriber credentials derived from the subscriberId
   * @param subscriptionPaymentProcessor A customer-aware payment processor to use. If the subscriber already has a
   *                                     payment processor, it must match the existing one.
   * @param clientPlatform               The platform of the client making the request
   * @param paymentSetupFunction         A function that takes the payment processor and the customer ID and begins
   *                                     adding a payment method. The function should return something that allows the
   *                                     client to configure the newly added payment method like a payment method setup
   *                                     token.
   * @param <T>                          A payment processor that has a notion of server-managed customers
   * @param <R>                          The return type of the paymentSetupFunction, which should be used by a client
   *                                     to configure the newly created payment method
   * @return The return value of the paymentSetupFunction
   * @throws SubscriptionForbiddenException         if the subscriber credentials were incorrect
   * @throws SubscriptionNotFoundException          if the subscriber did not exist or did not have a subscription
   *                                                attached
   * @throws SubscriptionProcessorConflictException if the new payment processor the existing processor associated with
   *                                                the subscriberId
   */
  public <T extends CustomerAwareSubscriptionPaymentProcessor, R> R addPaymentMethodToCustomer(
      final SubscriberCredentials subscriberCredentials,
      final T subscriptionPaymentProcessor,
      final ClientPlatform clientPlatform,
      final BiFunction<T, String, R> paymentSetupFunction)
      throws SubscriptionForbiddenException, SubscriptionNotFoundException, SubscriptionProcessorConflictException {

    Subscriptions.Record record = this.getSubscriber(subscriberCredentials);
    if (record.getProcessorCustomer().isEmpty()) {
      final ProcessorCustomer pc = subscriptionPaymentProcessor
          .createCustomer(subscriberCredentials.subscriberUser(), clientPlatform);
      record = subscriptions.setProcessorAndCustomerId(record,
          new ProcessorCustomer(pc.customerId(), subscriptionPaymentProcessor.getProvider()),
          Instant.now()).join();
    }
    final ProcessorCustomer processorCustomer = record.getProcessorCustomer()
        .orElseThrow(() -> new UncheckedIOException(new IOException("processor must now exist")));

    if (processorCustomer.processor() != subscriptionPaymentProcessor.getProvider()) {
      throw new SubscriptionProcessorConflictException("existing processor does not match");
    }
    return paymentSetupFunction.apply(subscriptionPaymentProcessor, processorCustomer.customerId());
  }

  public interface LevelTransitionValidator {

    /**
     * Check is a level update is valid
     *
     * @param oldLevel The current level of the subscription
     * @param newLevel The proposed updated level of the subscription
     * @return true if the subscription can be changed from oldLevel to newLevel, otherwise false
     */
    boolean isTransitionValid(long oldLevel, long newLevel);
  }

  /**
   * Update the subscription level in the payment processor and update the table.
   * <p>
   * If we don't have an existing subscription, create one in the payment processor and then update the table. If we do
   * already have a subscription, and it does not match the requested subscription, update it in the payment processor
   * and then update the table. When an update occurs, this is where a user's recurring charge to a payment method is
   * created or modified.
   *
   * @param subscriberCredentials  Subscriber credentials derived from the subscriberId
   * @param record                 A subscription record previous read with {@link #getSubscriber}
   * @param processor              A subscription payment processor with a notion of server-managed customers
   * @param level                  The desired subscription level
   * @param currency               The desired currency type for the subscription
   * @param idempotencyKey         An idempotencyKey that can be used to deduplicate requests within the payment
   *                               processor
   * @param subscriptionTemplateId Specifies the product associated with the provided level within the payment
   *                               processor
   * @param transitionValidator    A function that checks if the level update is valid
   * @throws SubscriptionInvalidArgumentsException  if the transitionValidator failed for the level transition, or the
   *                                                subscription could not be created because the payment provider
   *                                                requires additional action, or there was a failure because an
   *                                                idempotency key was reused on a * modified request
   * @throws SubscriptionProcessorConflictException if the new payment processor the existing processor associated with
   *                                                the subscriber
   * @throws SubscriptionProcessorException         if there was no payment method on the customer
   */
  public void updateSubscriptionLevelForCustomer(
      final SubscriberCredentials subscriberCredentials,
      final Subscriptions.Record record,
      final CustomerAwareSubscriptionPaymentProcessor processor,
      final long level,
      final String currency,
      final String idempotencyKey,
      final String subscriptionTemplateId,
      final LevelTransitionValidator transitionValidator)
      throws SubscriptionInvalidArgumentsException, SubscriptionProcessorConflictException, SubscriptionProcessorException {

    if (record.subscriptionId != null) {
      // we already have a subscription in our records so let's check the level and currency,
      // and only change it if needed
      final Object subscription = processor.getSubscription(record.subscriptionId);
      final CustomerAwareSubscriptionPaymentProcessor.LevelAndCurrency existingLevelAndCurrency =
          processor.getLevelAndCurrencyForSubscription(subscription);
      final CustomerAwareSubscriptionPaymentProcessor.LevelAndCurrency desiredLevelAndCurrency =
          new CustomerAwareSubscriptionPaymentProcessor.LevelAndCurrency(level, currency.toLowerCase(Locale.ROOT));
      if (existingLevelAndCurrency.equals(desiredLevelAndCurrency)) {
        return;
      }
      if (!transitionValidator.isTransitionValid(existingLevelAndCurrency.level(), level)) {
        throw new SubscriptionInvalidLevelException();
      }
      final CustomerAwareSubscriptionPaymentProcessor.SubscriptionId updatedSubscriptionId =
          processor.updateSubscription(subscription, subscriptionTemplateId, level, idempotencyKey);

      subscriptions.subscriptionLevelChanged(subscriberCredentials.subscriberUser(),
          subscriberCredentials.now(),
          level,
          updatedSubscriptionId.id()).join();
    } else {
      // Otherwise, we don't have a subscription yet so create it and then record the subscription id
      long lastSubscriptionCreatedAt = record.subscriptionCreatedAt != null
          ? record.subscriptionCreatedAt.getEpochSecond()
          : 0;

      final CustomerAwareSubscriptionPaymentProcessor.SubscriptionId subscription = processor.createSubscription(
          record.processorCustomer.customerId(),
          subscriptionTemplateId,
          level,
          lastSubscriptionCreatedAt);
      subscriptions.subscriptionCreated(
          subscriberCredentials.subscriberUser(), subscription.id(), subscriberCredentials.now(), level);

    }
  }

  /**
   * Check the provided play billing purchase token and write it the subscriptions table if is valid.
   *
   * @param subscriberCredentials    Subscriber credentials derived from the subscriberId
   * @param googlePlayBillingManager Performs play billing API operations
   * @param purchaseToken            The client provided purchaseToken that represents a purchased subscription in the
   *                                 play store
   * @return the subscription level for the accepted subscription
   * @throws SubscriptionForbiddenException         if the subscriber credentials were incorrect
   * @throws SubscriptionNotFoundException          if the subscriber did not exist or did not have a subscription
   *                                                attached
   * @throws SubscriptionProcessorConflictException if the new payment processor the existing processor associated with
   *                                                the subscriberId
   * @throws SubscriptionPaymentRequiredException   if the subscription is not in a state that grants the user an
   *                                                entitlement
   * @throws RateLimitExceededException             if rate-limited
   */
  public long updatePlayBillingPurchaseToken(
      final SubscriberCredentials subscriberCredentials,
      final GooglePlayBillingManager googlePlayBillingManager,
      final String purchaseToken)
      throws SubscriptionProcessorConflictException, SubscriptionForbiddenException, SubscriptionNotFoundException, RateLimitExceededException, SubscriptionPaymentRequiredException {

    // For IAP providers, the subscriptionId and the customerId are both just the purchaseToken. Changes to the
    // subscription always just result in a new purchaseToken
    final ProcessorCustomer pc = new ProcessorCustomer(purchaseToken, PaymentProvider.GOOGLE_PLAY_BILLING);

    final Subscriptions.Record record = getSubscriber(subscriberCredentials);

    // Check the record for an existing subscription
    if (record.processorCustomer != null
        && record.processorCustomer.processor() != PaymentProvider.GOOGLE_PLAY_BILLING) {
      throw new SubscriptionProcessorConflictException("existing processor does not match");
    }

    // If we're replacing an existing purchaseToken, cancel it first
    if (record.processorCustomer != null && !purchaseToken.equals(record.processorCustomer.customerId())) {
      googlePlayBillingManager.cancelAllActiveSubscriptions(record.processorCustomer.customerId());
    }

    // Validating ensures we don't allow a user-determined token that's totally bunk into the subscription manager,
    // but we don't want to acknowledge it until it's successfully persisted.
    final GooglePlayBillingManager.ValidatedToken validatedToken = googlePlayBillingManager.validateToken(purchaseToken);

    // Store the valid purchaseToken with the subscriber
    subscriptions.setIapPurchase(record, pc, purchaseToken, validatedToken.getLevel(), subscriberCredentials.now());

    // Now that the purchaseToken is durable, we can acknowledge it
    validatedToken.acknowledgePurchase();

    return validatedToken.getLevel();
  }

  /**
   * Check the provided app store transactionId and write it the subscriptions table if is valid.
   *
   * @param subscriberCredentials Subscriber credentials derived from the subscriberId
   * @param appleAppStoreManager  Performs app store API operations
   * @param originalTransactionId The client provided originalTransactionId that represents a purchased subscription in
   *                              the app store
   * @return the subscription level for the accepted subscription
   * @throws SubscriptionForbiddenException         if the subscriber credentials are incorrect
   * @throws SubscriptionNotFoundException          if the originalTransactionId does not exist
   * @throws SubscriptionProcessorConflictException if the new payment processor the existing processor associated with
   *                                                the subscriber
   * @throws SubscriptionInvalidArgumentsException  if the originalTransactionId is malformed or does not represent a
   *                                                valid subscription
   * @throws SubscriptionPaymentRequiredException   if the subscription is not in a state that grants the user an
   *                                                entitlement
   * @throws RateLimitExceededException             if rate-limited
   */
  public long updateAppStoreTransactionId(
      final SubscriberCredentials subscriberCredentials,
      final AppleAppStoreManager appleAppStoreManager,
      final String originalTransactionId)
      throws SubscriptionForbiddenException, SubscriptionNotFoundException, SubscriptionProcessorConflictException, SubscriptionInvalidArgumentsException, SubscriptionPaymentRequiredException, RateLimitExceededException {

    final Subscriptions.Record record = getSubscriber(subscriberCredentials);
    if (record.processorCustomer != null
        && record.processorCustomer.processor() != PaymentProvider.APPLE_APP_STORE) {
      throw new SubscriptionProcessorConflictException("existing processor does not match");
    }

    // For IAP providers, the subscriptionId and the customerId are both just the identifier for the subscription in
    // the provider (in this case, the originalTransactionId). Changes to the subscription always just result in a new
    // originalTransactionId
    final ProcessorCustomer pc = new ProcessorCustomer(originalTransactionId, PaymentProvider.APPLE_APP_STORE);

    final Long level = appleAppStoreManager.validateTransaction(originalTransactionId);
    subscriptions.setIapPurchase(record, pc, originalTransactionId, level, subscriberCredentials.now()).join();
    return level;
  }

  private SubscriptionPaymentProcessor getProcessor(PaymentProvider provider) {
    return processors.get(provider);
  }
}
