/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import com.stripe.exception.StripeException;
import java.time.Instant;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialRequest;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialResponse;
import org.signal.libsignal.zkgroup.receipts.ServerZkReceiptOperations;
import org.whispersystems.textsecuregcm.controllers.SubscriptionController;
import org.whispersystems.textsecuregcm.subscriptions.AppleAppStoreManager;
import org.whispersystems.textsecuregcm.subscriptions.CustomerAwareSubscriptionPaymentProcessor;
import org.whispersystems.textsecuregcm.subscriptions.GooglePlayBillingManager;
import org.whispersystems.textsecuregcm.subscriptions.PaymentProvider;
import org.whispersystems.textsecuregcm.subscriptions.ProcessorCustomer;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionInformation;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionPaymentProcessor;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import org.whispersystems.textsecuregcm.util.Util;
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
   * @return A stage that completes when the subscription has been cancelled with the upstream payment provider and the
   * subscription has been removed from the table.
   */
  public CompletableFuture<Void> deleteSubscriber(final SubscriberCredentials subscriberCredentials) {
    return subscriptions.get(subscriberCredentials.subscriberUser(), subscriberCredentials.hmac())
        .thenCompose(getResult -> {
          if (getResult == Subscriptions.GetResult.NOT_STORED
              || getResult == Subscriptions.GetResult.PASSWORD_MISMATCH) {
            return CompletableFuture.failedFuture(new SubscriptionException.NotFound());
          }
          return getResult.record.getProcessorCustomer()
              .map(processorCustomer -> getProcessor(processorCustomer.processor())
                  .cancelAllActiveSubscriptions(processorCustomer.customerId()))
              // a missing customer ID is OK; it means the subscriber never started to add a payment method
              .orElseGet(() -> CompletableFuture.completedFuture(null));
        })
        .thenCompose(unused ->
            subscriptions.setCanceledAt(subscriberCredentials.subscriberUser(), subscriberCredentials.now()));
  }

  /**
   * Create or update a subscriber in the subscriptions table
   * <p>
   * If the subscriber does not exist, a subscriber with the provided credentials will be created. If the subscriber
   * already exists, its last access time will be updated.
   *
   * @param subscriberCredentials Subscriber credentials derived from the subscriberId
   * @return A stage that completes when the subscriber has been updated.
   */
  public CompletableFuture<Void> updateSubscriber(final SubscriberCredentials subscriberCredentials) {
    return subscriptions.get(subscriberCredentials.subscriberUser(), subscriberCredentials.hmac())
        .thenCompose(getResult -> {
          if (getResult == Subscriptions.GetResult.PASSWORD_MISMATCH) {
            return CompletableFuture.failedFuture(new SubscriptionException.Forbidden("subscriberId mismatch"));
          } else if (getResult == Subscriptions.GetResult.NOT_STORED) {
            // create a customer and write it to ddb
            return subscriptions.create(subscriberCredentials.subscriberUser(), subscriberCredentials.hmac(),
                    subscriberCredentials.now())
                .thenApply(updatedRecord -> {
                  if (updatedRecord == null) {
                    throw ExceptionUtils.wrap(new SubscriptionException.Forbidden("subscriberId mismatch"));
                  }
                  return updatedRecord;
                });
          } else {
            // already exists so just touch access time and return
            return subscriptions.accessedAt(subscriberCredentials.subscriberUser(), subscriberCredentials.now())
                .thenApply(unused -> getResult.record);
          }
        })
        .thenRun(Util.NOOP);
  }

  public CompletableFuture<Optional<SubscriptionInformation>> getSubscriptionInformation(
      final SubscriberCredentials subscriberCredentials) {
    return getSubscriber(subscriberCredentials).thenCompose(record -> {
      if (record.subscriptionId == null) {
        return CompletableFuture.completedFuture(Optional.empty());
      }
      final SubscriptionPaymentProcessor manager = getProcessor(record.processorCustomer.processor());
      return manager.getSubscriptionInformation(record.subscriptionId).thenApply(Optional::of);
    });
  }

  /**
   * Get the subscriber record
   *
   * @param subscriberCredentials Subscriber credentials derived from the subscriberId
   * @return A stage that completes with the requested subscriber if it exists and the credentials are correct.
   */
  public CompletableFuture<Subscriptions.Record> getSubscriber(final SubscriberCredentials subscriberCredentials) {
    return subscriptions.get(subscriberCredentials.subscriberUser(), subscriberCredentials.hmac())
        .thenApply(getResult -> {
          if (getResult == Subscriptions.GetResult.PASSWORD_MISMATCH) {
            throw ExceptionUtils.wrap(new SubscriptionException.Forbidden("subscriberId mismatch"));
          } else if (getResult == Subscriptions.GetResult.NOT_STORED) {
            throw ExceptionUtils.wrap(new SubscriptionException.NotFound());
          } else {
            return getResult.record;
          }
        });
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
   * @return If the subscription had a valid payment, the requested ZK receipt credential
   */
  public CompletableFuture<ReceiptResult> createReceiptCredentials(
      final SubscriberCredentials subscriberCredentials,
      final SubscriptionController.GetReceiptCredentialsRequest request,
      final Function<CustomerAwareSubscriptionPaymentProcessor.ReceiptItem, Instant> expiration) {
    return getSubscriber(subscriberCredentials).thenCompose(record -> {
      if (record.subscriptionId == null) {
        return CompletableFuture.failedFuture(new SubscriptionException.NotFound());
      }

      ReceiptCredentialRequest receiptCredentialRequest;
      try {
        receiptCredentialRequest = new ReceiptCredentialRequest(request.receiptCredentialRequest());
      } catch (InvalidInputException e) {
        return CompletableFuture.failedFuture(
            new SubscriptionException.InvalidArguments("invalid receipt credential request", e));
      }

      final PaymentProvider processor = record.getProcessorCustomer().orElseThrow().processor();
      final SubscriptionPaymentProcessor manager = getProcessor(processor);
      return manager.getReceiptItem(record.subscriptionId)
          .thenCompose(receipt -> issuedReceiptsManager.recordIssuance(
                  receipt.itemId(), manager.getProvider(), receiptCredentialRequest,
                  subscriberCredentials.now())
              .thenApply(unused -> receipt))
          .thenApply(receipt -> {
            ReceiptCredentialResponse receiptCredentialResponse;
            try {
              receiptCredentialResponse = zkReceiptOperations.issueReceiptCredential(
                  receiptCredentialRequest,
                  expiration.apply(receipt).getEpochSecond(),
                  receipt.level());
            } catch (VerificationFailedException e) {
              throw ExceptionUtils.wrap(
                  new SubscriptionException.InvalidArguments("receipt credential request failed verification", e));
            }
            return new ReceiptResult(receiptCredentialResponse, receipt, processor);
          });
    });
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
   * @return A stage that completes when the payment method has been created in the payment processor and the table has
   * been updated
   */
  public <T extends CustomerAwareSubscriptionPaymentProcessor, R> CompletableFuture<R> addPaymentMethodToCustomer(
      final SubscriberCredentials subscriberCredentials,
      final T subscriptionPaymentProcessor,
      final ClientPlatform clientPlatform,
      final BiFunction<T, String, CompletableFuture<R>> paymentSetupFunction) {
    return this.getSubscriber(subscriberCredentials).thenCompose(record -> record.getProcessorCustomer()
            .map(ProcessorCustomer::processor)
            .map(processor -> {
              if (processor != subscriptionPaymentProcessor.getProvider()) {
                return CompletableFuture.<Subscriptions.Record>failedFuture(
                    new SubscriptionException.ProcessorConflict("existing processor does not match"));
              }
              return CompletableFuture.completedFuture(record);
            })
            .orElseGet(() -> subscriptionPaymentProcessor
                .createCustomer(subscriberCredentials.subscriberUser(), clientPlatform)
                .thenApply(ProcessorCustomer::customerId)
                .thenCompose(customerId -> subscriptions.setProcessorAndCustomerId(record,
                    new ProcessorCustomer(customerId, subscriptionPaymentProcessor.getProvider()),
                    Instant.now()))))
        .thenCompose(updatedRecord -> {
          final String customerId = updatedRecord.getProcessorCustomer()
              .filter(pc -> pc.processor().equals(subscriptionPaymentProcessor.getProvider()))
              .orElseThrow(() ->
                  ExceptionUtils.wrap(new SubscriptionException(null, "record should not be missing customer")))
              .customerId();
          return paymentSetupFunction.apply(subscriptionPaymentProcessor, customerId);
        });
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
   * @return A stage that completes when the level has been updated in the payment processor and the table
   */
  public CompletableFuture<Void> updateSubscriptionLevelForCustomer(
      final SubscriberCredentials subscriberCredentials,
      final Subscriptions.Record record,
      final CustomerAwareSubscriptionPaymentProcessor processor,
      final long level,
      final String currency,
      final String idempotencyKey,
      final String subscriptionTemplateId,
      final LevelTransitionValidator transitionValidator) {

    return Optional.ofNullable(record.subscriptionId)

        // we already have a subscription in our records so let's check the level and currency,
        // and only change it if needed
        .map(subId -> processor
            .getSubscription(subId)
            .thenCompose(subscription -> processor.getLevelAndCurrencyForSubscription(subscription)
                .thenCompose(existingLevelAndCurrency -> {
                  if (existingLevelAndCurrency.equals(
                      new CustomerAwareSubscriptionPaymentProcessor.LevelAndCurrency(level,
                          currency.toLowerCase(Locale.ROOT)))) {
                    return CompletableFuture.completedFuture(null);
                  }
                  if (!transitionValidator.isTransitionValid(existingLevelAndCurrency.level(), level)) {
                    return CompletableFuture.failedFuture(new SubscriptionException.InvalidLevel());
                  }
                  return processor.updateSubscription(subscription, subscriptionTemplateId, level, idempotencyKey)
                      .thenCompose(updatedSubscription ->
                          subscriptions.subscriptionLevelChanged(subscriberCredentials.subscriberUser(),
                              subscriberCredentials.now(),
                              level, updatedSubscription.id()));
                })))

        // Otherwise, we don't have a subscription yet so create it and then record the subscription id
        .orElseGet(() -> {
          long lastSubscriptionCreatedAt = record.subscriptionCreatedAt != null
              ? record.subscriptionCreatedAt.getEpochSecond()
              : 0;

          return processor.createSubscription(record.processorCustomer.customerId(),
                  subscriptionTemplateId,
                  level,
                  lastSubscriptionCreatedAt)
              .exceptionally(ExceptionUtils.exceptionallyHandler(StripeException.class, stripeException -> {
                if ("subscription_payment_intent_requires_action".equals(stripeException.getCode())) {
                  throw ExceptionUtils.wrap(new SubscriptionException.PaymentRequiresAction());
                }
                throw ExceptionUtils.wrap(stripeException);
              }))
              .thenCompose(subscription -> subscriptions.subscriptionCreated(
                  subscriberCredentials.subscriberUser(), subscription.id(), subscriberCredentials.now(), level));
        });
  }

  /**
   * Check the provided play billing purchase token and write it the subscriptions table if is valid.
   *
   * @param subscriberCredentials    Subscriber credentials derived from the subscriberId
   * @param googlePlayBillingManager Performs play billing API operations
   * @param purchaseToken            The client provided purchaseToken that represents a purchased subscription in the
   *                                 play store
   * @return A stage that completes with the subscription level for the accepted subscription
   */
  public CompletableFuture<Long> updatePlayBillingPurchaseToken(
      final SubscriberCredentials subscriberCredentials,
      final GooglePlayBillingManager googlePlayBillingManager,
      final String purchaseToken) {

    // For IAP providers, the subscriptionId and the customerId are both just the purchaseToken. Changes to the
    // subscription always just result in a new purchaseToken
    final ProcessorCustomer pc = new ProcessorCustomer(purchaseToken, PaymentProvider.GOOGLE_PLAY_BILLING);

    return getSubscriber(subscriberCredentials)

        // Check the record for an existing subscription
        .thenCompose(record -> {
          if (record.processorCustomer != null
              && record.processorCustomer.processor() != PaymentProvider.GOOGLE_PLAY_BILLING) {
            return CompletableFuture.failedFuture(
                new SubscriptionException.ProcessorConflict("existing processor does not match"));
          }

          // If we're replacing an existing purchaseToken, cancel it first
          return Optional.ofNullable(record.processorCustomer)
              .map(ProcessorCustomer::customerId)
              .filter(existingToken -> !purchaseToken.equals(existingToken))
              .map(googlePlayBillingManager::cancelAllActiveSubscriptions)
              .orElseGet(() -> CompletableFuture.completedFuture(null))
              .thenApply(ignored -> record);
        })

        // Validate and set the purchaseToken
        .thenCompose(record -> googlePlayBillingManager

            // Validating ensures we don't allow a user-determined token that's totally bunk into the subscription manager,
            // but we don't want to acknowledge it until it's successfully persisted.
            .validateToken(purchaseToken)

            // Store the purchaseToken with the subscriber
            .thenCompose(validatedToken -> subscriptions.setIapPurchase(
                    record, pc, purchaseToken, validatedToken.getLevel(), subscriberCredentials.now())
                // Now that the purchaseToken is durable, we can acknowledge it
                .thenCompose(ignore -> validatedToken.acknowledgePurchase())
                .thenApply(ignore -> validatedToken.getLevel())));
  }

  /**
   * Check the provided app store transactionId and write it the subscriptions table if is valid.
   *
   * @param subscriberCredentials Subscriber credentials derived from the subscriberId
   * @param appleAppStoreManager  Performs app store API operations
   * @param originalTransactionId The client provided originalTransactionId that represents a purchased subscription in
   *                              the app store
   * @return A stage that completes with the subscription level for the accepted subscription
   */
  public CompletableFuture<Long> updateAppStoreTransactionId(
      final SubscriberCredentials subscriberCredentials,
      final AppleAppStoreManager appleAppStoreManager,
      final String originalTransactionId) {

    return getSubscriber(subscriberCredentials).thenCompose(record -> {
      if (record.processorCustomer != null
          && record.processorCustomer.processor() != PaymentProvider.APPLE_APP_STORE) {
        return CompletableFuture.failedFuture(
            new SubscriptionException.ProcessorConflict("existing processor does not match"));
      }

      // For IAP providers, the subscriptionId and the customerId are both just the identifier for the subscription in
      // the provider (in this case, the originalTransactionId). Changes to the subscription always just result in a new
      // originalTransactionId
      final ProcessorCustomer pc = new ProcessorCustomer(originalTransactionId, PaymentProvider.APPLE_APP_STORE);

      return appleAppStoreManager
          .validateTransaction(originalTransactionId)
          .thenCompose(level -> subscriptions
              .setIapPurchase(record, pc, originalTransactionId, level, subscriberCredentials.now())
              .thenApply(ignore -> level));
    });

  }

  private SubscriptionPaymentProcessor getProcessor(PaymentProvider provider) {
    return processors.get(provider);
  }
}
