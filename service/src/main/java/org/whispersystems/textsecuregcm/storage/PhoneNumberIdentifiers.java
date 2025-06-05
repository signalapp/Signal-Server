/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import org.whispersystems.textsecuregcm.util.Util;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.CancellationReason;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnValuesOnConditionCheckFailure;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.TransactionConflictException;
import software.amazon.awssdk.services.dynamodb.model.Update;

/**
 * Manages a global, persistent mapping of phone numbers to phone number identifiers regardless of whether those
 * numbers/identifiers are actually associated with an account.
 */
public class PhoneNumberIdentifiers {

  private final DynamoDbAsyncClient dynamoDbClient;
  private final String tableName;

  @VisibleForTesting
  static final String KEY_E164 = "P";
  @VisibleForTesting
  static final String INDEX_NAME = "pni_to_p";
  @VisibleForTesting
  static final String ATTR_PHONE_NUMBER_IDENTIFIER = "PNI";

  private static final String CONDITIONAL_CHECK_FAILED = "ConditionalCheckFailed";

  private static final Timer GET_PNI_TIMER = Metrics.timer(name(PhoneNumberIdentifiers.class, "get"));
  private static final Timer SET_PNI_TIMER = Metrics.timer(name(PhoneNumberIdentifiers.class, "set"));
  private static final int MAX_RETRIES = 10;

  private static final Logger logger = LoggerFactory.getLogger(PhoneNumberIdentifiers.class);

  public PhoneNumberIdentifiers(final DynamoDbAsyncClient dynamoDbClient, final String tableName) {
    this.dynamoDbClient = dynamoDbClient;
    this.tableName = tableName;
  }

  /**
   * Returns the phone number identifier (PNI) associated with the given phone number. If one doesn't exist, it is
   * created.
   *
   * @param phoneNumber the phone number for which to retrieve a phone number identifier
   * @return the phone number identifier associated with the given phone number
   */
  public CompletableFuture<UUID> getPhoneNumberIdentifier(final String phoneNumber) {
    // Each e164 phone number string represents a potential equivalence class e164s that represent the same number. If
    // this is a new phone number, we'll want to set all the numbers in the equivalence class to the same PNI
    final List<String> allPhoneNumberForms = Util.getAlternateForms(phoneNumber);

    return retry(MAX_RETRIES, TransactionConflictException.class, () -> fetchPhoneNumbers(allPhoneNumberForms)
        .thenCompose(mappings -> setPniIfRequired(phoneNumber, allPhoneNumberForms, mappings)));
  }

  /**
   * Returns the list of phone numbers associated with a given phone number identifier. If this
   * UUID was not previously assigned as a PNI by {@link #getPhoneNumberIdentifier(String)}, the
   * returned list will be empty.
   *
   * @param phoneNumberIdentifier a phone number identifier
   * @return the list of all e164s associated with the given phone number identifier
   */
  public CompletableFuture<List<String>> getPhoneNumber(final UUID phoneNumberIdentifier) {
    return dynamoDbClient.query(QueryRequest.builder()
            .tableName(tableName)
            .indexName(INDEX_NAME)
            .keyConditionExpression("#pni = :pni")
            .projectionExpression("#phone_number")
            .expressionAttributeNames(Map.of(
                "#phone_number", KEY_E164,
                "#pni", ATTR_PHONE_NUMBER_IDENTIFIER
            ))
            .expressionAttributeValues(Map.of(
                ":pni", AttributeValues.fromUUID(phoneNumberIdentifier)
            ))
            .build())
        .thenApply(response -> response.items().stream().map(item -> item.get(KEY_E164).s()).toList());
  }

  @VisibleForTesting
  static <T, E extends Exception> CompletableFuture<T> retry(
      final int numRetries, final Class<E> exceptionToRetry, final Supplier<CompletableFuture<T>> supplier) {
    return supplier.get().exceptionallyCompose(ExceptionUtils.exceptionallyHandler(exceptionToRetry, e -> {
      if (numRetries - 1 <= 0) {
        throw ExceptionUtils.wrap(e);
      }
      return retry(numRetries - 1, exceptionToRetry, supplier);
    }));
  }

  /**
   * Determine what PNI to set for the provided numbers, and set them if required
   *
   * @param phoneNumber          The original e164 the operation is for
   * @param allPhoneNumberForms  The e164s to set. The first e164 in this list should be phoneNumber
   * @param existingAssociations The current associations of allPhoneNumberForms in the table
   * @return The PNI now associated with phoneNumber
   */
  @VisibleForTesting
  CompletableFuture<UUID> setPniIfRequired(
      final String phoneNumber,
      final List<String> allPhoneNumberForms,
      Map<String, UUID> existingAssociations) {
    if (!phoneNumber.equals(allPhoneNumberForms.getFirst())) {
      throw new IllegalArgumentException("allPhoneNumberForms must start with the target phoneNumber");
    }

    if (existingAssociations.containsKey(phoneNumber)) {
      // If the provided phone number already has an association, just return that
      return CompletableFuture.completedFuture(existingAssociations.get(phoneNumber));
    }

    if (allPhoneNumberForms.size() == 1 || existingAssociations.isEmpty()) {
      // Easy case, if we're the only phone number in our equivalence class or there are no existing associations,
      // we can just make an association for a new PNI
      return setPni(phoneNumber, allPhoneNumberForms, UUID.randomUUID());
    }

    // Otherwise, what members of the equivalence class have a PNI association?
    final Map<UUID, List<String>> byPni = existingAssociations.entrySet().stream().collect(Collectors.groupingBy(
        Map.Entry::getValue,
        Collectors.mapping(Map.Entry::getKey, Collectors.toList())));

    // Usually there should be only a single PNI associated with the equivalence class, but it's possible there's
    // more. This could only happen if an equivalence class had more than two numbers, and had accumulated 2 unique
    // PNI associations before they were merged into a single class. In this case we've picked one of those pnis
    // arbitrarily (according to their ordering as returned by getAlternateForms)
    final UUID existingPni = allPhoneNumberForms.stream()
        .filter(existingAssociations::containsKey)
        .findFirst()
        .map(existingAssociations::get)
        .orElseThrow(() -> new IllegalStateException("Previously checked that a mapping existed"));

    if (byPni.size() > 1) {
      logger.warn("More than one PNI existed in the PNI table for the numbers that map to {}. " +
              "Arbitrarily picking {} to be the representative PNI for the numbers without PNI associations",
          phoneNumber, existingPni);
    }

    // Find all the unmapped phoneNumbers and set them to the PNI we chose from another member of the equivalence class
    final List<String> unmappedNumbers = allPhoneNumberForms.stream()
        .filter(number -> !existingAssociations.containsKey(number))
        .toList();

    return setPni(phoneNumber, unmappedNumbers, existingPni);
  }


  /**
   * Attempt to associate phoneNumbers with the provided pni. If any of the phoneNumbers have an existing association
   * that is not the target pni, no update will occur. If the first phoneNumber in phoneNumbers has an existing
   * association, it will be returned, otherwise an exception will be thrown.
   *
   * @param originalPhoneNumber The original e164 the operation is for
   * @param allPhoneNumberForms The e164s to set. The first e164 in this list should be originalPhoneNumber
   * @param pni                 The PNI to set
   * @return The provided PNI if the update occurred, or the existing PNI associated with originalPhoneNumber
   */
  @VisibleForTesting
  CompletableFuture<UUID> setPni(final String originalPhoneNumber, final List<String> allPhoneNumberForms,
      final UUID pni) {
    if (!originalPhoneNumber.equals(allPhoneNumberForms.getFirst())) {
      throw new IllegalArgumentException("allPhoneNumberForms must start with the target phoneNumber");
    }

    final Timer.Sample sample = Timer.start();
    final List<TransactWriteItem> transactWriteItems = allPhoneNumberForms
        .stream()
        .map(phoneNumber -> TransactWriteItem.builder()
            .update(Update.builder()
                .tableName(tableName)
                .key(Map.of(KEY_E164, AttributeValues.fromString(phoneNumber)))
                .updateExpression("SET #pni = :pni")
                // It's possible we're racing with someone else to update, but both of us selected the same PNI because
                // an equivalent number already had it. That's fine, as long as the association happens.
                .conditionExpression("attribute_not_exists(#pni) OR #pni = :pni")
                .expressionAttributeNames(Map.of("#pni", ATTR_PHONE_NUMBER_IDENTIFIER))
                .expressionAttributeValues(Map.of(":pni", AttributeValues.fromUUID(pni)))
                .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD)
                .build()).build())
        .toList();

    return dynamoDbClient.transactWriteItems(TransactWriteItemsRequest.builder()
            .transactItems(transactWriteItems)
            .build())
        .thenApply(ignored -> pni)
        .exceptionally(ExceptionUtils.exceptionallyHandler(TransactionCanceledException.class, e -> {
          if (e.hasCancellationReasons()) {
            // Get the cancellation reason for the number that we were primarily trying to associate with a PNI
            final CancellationReason cancelReason = e.cancellationReasons().getFirst();
            if (CONDITIONAL_CHECK_FAILED.equals(cancelReason.code())) {
              // Someone else beat us to the update, use the PNI they set.
              return AttributeValues.getUUID(cancelReason.item(), ATTR_PHONE_NUMBER_IDENTIFIER, null);
            }
          }
          throw e;
        }))
        .whenComplete((ignored, throwable) -> sample.stop(SET_PNI_TIMER));
  }

  @VisibleForTesting
  CompletableFuture<Map<String, UUID>> fetchPhoneNumbers(List<String> phoneNumbers) {
    final Timer.Sample sample = Timer.start();
    return dynamoDbClient.batchGetItem(
            BatchGetItemRequest.builder().requestItems(Map.of(tableName, KeysAndAttributes.builder()
                    // If we have a stale value, the subsequent conditional update will fail
                    .consistentRead(false)
                    .projectionExpression("#number,#pni")
                    .expressionAttributeNames(Map.of("#number", KEY_E164, "#pni", ATTR_PHONE_NUMBER_IDENTIFIER))
                    .keys(phoneNumbers.stream()
                        .map(number -> Map.of(KEY_E164, AttributeValues.fromString(number)))
                        .toArray(Map[]::new))
                    .build()))
                .build())
        .thenApply(batchResponse -> batchResponse.responses().get(tableName).stream().collect(Collectors.toMap(
            item -> AttributeValues.getString(item, KEY_E164, null),
            item -> AttributeValues.getUUID(item, ATTR_PHONE_NUMBER_IDENTIFIER, null))))
        .whenComplete((ignored, throwable) -> sample.stop(GET_PNI_TIMER));
  }

  CompletableFuture<Void> regeneratePhoneNumberIdentifierMappings(final Account account) {
    return setPni(account.getNumber(), Util.getAlternateForms(account.getNumber()), account.getIdentifier(IdentityType.PNI))
        .thenRun(Util.NOOP);
  }
}
