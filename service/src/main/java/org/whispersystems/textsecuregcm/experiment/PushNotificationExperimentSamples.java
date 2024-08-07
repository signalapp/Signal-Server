package org.whispersystems.textsecuregcm.experiment;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;
import reactor.util.retry.Retry;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

public class PushNotificationExperimentSamples {

  private final DynamoDbAsyncClient dynamoDbAsyncClient;
  private final String tableName;
  private final Clock clock;

  // Experiment name; DynamoDB string; partition key
  public static final String KEY_EXPERIMENT_NAME = "N";

  // Combined ACI and device ID; DynamoDB byte array; sort key
  public static final String ATTR_ACI_AND_DEVICE_ID = "AD";

  // Whether the device is enrolled in the experiment group (as opposed to control group); DynamoDB boolean
  static final String ATTR_IN_EXPERIMENT_GROUP = "X";

  // The experiment-specific state of the device at the start of the experiment, represented as a JSON blob; DynamoDB
  // string
  static final String ATTR_INITIAL_STATE = "I";

  // The experiment-specific state of the device at the end of the experiment, represented as a JSON blob; DynamoDB
  // string
  static final String ATTR_FINAL_STATE = "F";

  // The time, in seconds since the epoch, at which this sample should be deleted automatically
  static final String ATTR_TTL = "E";

  private static final Duration FINAL_SAMPLE_TTL = Duration.ofDays(7);

  private static final Logger log = LoggerFactory.getLogger(PushNotificationExperimentSamples.class);

  public PushNotificationExperimentSamples(final DynamoDbAsyncClient dynamoDbAsyncClient,
      final String tableName,
      final Clock clock) {

    this.dynamoDbAsyncClient = dynamoDbAsyncClient;
    this.tableName = tableName;
    this.clock = clock;
  }

  /**
   * Writes the initial state of a device participating in a push notification experiment.
   *
   * @param accountIdentifier the account identifier for the account to which the target device is linked
   * @param deviceId the identifier for the device within the given account
   * @param experimentName the name of the experiment
   * @param inExperimentGroup whether the given device is in the experiment group (as opposed to control group)
   * @param initialState the initial state of the object; must be serializable as a JSON text
   *
   * @return a future that completes when the record has been stored; the future yields {@code true} if a new record
   * was stored or {@code false} if a conflicting record already exists
   *
   * @param <T> the type of state object for this sample
   *
   * @throws JsonProcessingException if the given {@code initialState} could not be serialized as a JSON text
   */
  public <T> CompletableFuture<Boolean> recordInitialState(final UUID accountIdentifier,
      final byte deviceId,
      final String experimentName,
      final boolean inExperimentGroup,
      final T initialState) throws JsonProcessingException {

    final AttributeValue initialStateAttributeValue =
        AttributeValue.fromS(SystemMapper.jsonMapper().writeValueAsString(initialState));

    final AttributeValue inExperimentGroupAttributeValue = AttributeValue.fromBool(inExperimentGroup);

    return dynamoDbAsyncClient.putItem(PutItemRequest.builder()
            .tableName(tableName)
            .item(Map.of(
                KEY_EXPERIMENT_NAME, AttributeValue.fromS(experimentName),
                ATTR_ACI_AND_DEVICE_ID, buildSortKey(accountIdentifier, deviceId),
                ATTR_IN_EXPERIMENT_GROUP, inExperimentGroupAttributeValue,
                ATTR_INITIAL_STATE, initialStateAttributeValue,
                ATTR_TTL, AttributeValue.fromN(String.valueOf(clock.instant().plus(FINAL_SAMPLE_TTL).getEpochSecond()))))
            .conditionExpression("(attribute_not_exists(#inExperimentGroup) OR #inExperimentGroup = :inExperimentGroup) AND (attribute_not_exists(#initialState) OR #initialState = :initialState) AND attribute_not_exists(#finalState)")
            .expressionAttributeNames(Map.of(
                "#inExperimentGroup", ATTR_IN_EXPERIMENT_GROUP,
                "#initialState", ATTR_INITIAL_STATE,
                "#finalState", ATTR_FINAL_STATE))
            .expressionAttributeValues(Map.of(
                ":inExperimentGroup", inExperimentGroupAttributeValue,
                ":initialState", initialStateAttributeValue))
            .build())
        .thenApply(ignored -> true)
        .exceptionally(throwable -> {
          if (ExceptionUtils.unwrap(throwable) instanceof ConditionalCheckFailedException) {
            return false;
          }

          throw ExceptionUtils.wrap(throwable);
        });
  }

  /**
   * Writes the final state of a device participating in a push notification experiment.
   *
   * @param accountIdentifier the account identifier for the account to which the target device is linked
   * @param deviceId the identifier for the device within the given account
   * @param experimentName the name of the experiment
   * @param finalState the final state of the object; must be serializable as a JSON text and of the same type as the
   *                   previously-stored initial state

   * @return A future that completes when the final state has been stored; yields a finished sample if an initial sample
   * was found or empty if no initial sample was found for the given account, device, and experiment. The future may
   * with a {@link JsonProcessingException} if the initial state could not be read or the final state could not be
   * written as a JSON text.
   *
   * @param <T> the type of state object for this sample
   */
  public <T> CompletableFuture<PushNotificationExperimentSample<T>> recordFinalState(final UUID accountIdentifier,
      final byte deviceId,
      final String experimentName,
      final T finalState) {

    CompletableFuture<String> finalStateJsonFuture;

    // Process the final state JSON on the calling thread, but inside a CompletionStage so there's just one "channel"
    // for reporting JSON exceptions. The alternative is to `throw JsonProcessingException`, but then callers would have
    // to both catch the exception when calling this method and also watch the returned future for the same exception.
    try {
      finalStateJsonFuture =
          CompletableFuture.completedFuture(SystemMapper.jsonMapper().writeValueAsString(finalState));
    } catch (final JsonProcessingException e) {
      finalStateJsonFuture = CompletableFuture.failedFuture(e);
    }

    final AttributeValue aciAndDeviceIdAttributeValue = buildSortKey(accountIdentifier, deviceId);

    return finalStateJsonFuture.thenCompose(finalStateJson -> {
      return dynamoDbAsyncClient.updateItem(UpdateItemRequest.builder()
              .tableName(tableName)
              .key(Map.of(
                  KEY_EXPERIMENT_NAME, AttributeValue.fromS(experimentName),
                  ATTR_ACI_AND_DEVICE_ID, aciAndDeviceIdAttributeValue))
              // `UpdateItem` will, by default, create a new item if one does not already exist for the given primary key. We
              // want update-only-if-exists behavior, though, and so check that there's already an existing item for this ACI
              // and device ID.
              .conditionExpression("#aciAndDeviceId = :aciAndDeviceId")
              .updateExpression("SET #finalState = if_not_exists(#finalState, :finalState)")
              .expressionAttributeNames(Map.of(
                  "#aciAndDeviceId", ATTR_ACI_AND_DEVICE_ID,
                  "#finalState", ATTR_FINAL_STATE))
              .expressionAttributeValues(Map.of(
                  ":aciAndDeviceId", aciAndDeviceIdAttributeValue,
                  ":finalState", AttributeValue.fromS(finalStateJson)))
              .returnValues(ReturnValue.ALL_NEW)
              .build())
          .thenApply(updateItemResponse -> {
            try {
              final boolean inExperimentGroup = updateItemResponse.attributes().get(ATTR_IN_EXPERIMENT_GROUP).bool();

              @SuppressWarnings("unchecked") final T parsedInitialState =
                  (T) parseState(updateItemResponse.attributes().get(ATTR_INITIAL_STATE).s(), finalState.getClass());

              @SuppressWarnings("unchecked") final T parsedFinalState =
                  (T) parseState(updateItemResponse.attributes().get(ATTR_FINAL_STATE).s(), finalState.getClass());

              return new PushNotificationExperimentSample<>(accountIdentifier, deviceId, inExperimentGroup, parsedInitialState, parsedFinalState);
            } catch (final JsonProcessingException e) {
              throw ExceptionUtils.wrap(e);
            }
          });
    });
  }

  /**
   * Returns a publisher across all samples for a given experiment.
   *
   * @param experimentName the name of the experiment for which to fetch samples
   * @param stateClass the type of state object for sample in the given experiment
   *
   * @return a publisher of tuples of ACI, device ID, and sample for all samples associated with the given experiment
   *
   * @param <T> the type of the sample's state objects
   *
   * @see <a href="https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.ParallelScan">Working with scans - Parallel scan</a>
   */
  public <T> Flux<PushNotificationExperimentSample<T>> getSamples(final String experimentName, final Class<T> stateClass) {

    return Flux.from(dynamoDbAsyncClient.queryPaginator(QueryRequest.builder()
                .tableName(tableName)
                .keyConditionExpression("#experiment = :experiment")
                .expressionAttributeNames(Map.of("#experiment", KEY_EXPERIMENT_NAME))
                .expressionAttributeValues(Map.of(":experiment", AttributeValue.fromS(experimentName)))
                .build())
            .items())
        .handle((item, sink) -> {
          try {
            final Tuple2<UUID, Byte> aciAndDeviceId = parseSortKey(item.get(ATTR_ACI_AND_DEVICE_ID));

            final boolean inExperimentGroup = item.get(ATTR_IN_EXPERIMENT_GROUP).bool();
            final T initialState = parseState(item.get(ATTR_INITIAL_STATE).s(), stateClass);
            final T finalState = item.get(ATTR_FINAL_STATE) != null
                ? parseState(item.get(ATTR_FINAL_STATE).s(), stateClass)
                : null;

            sink.next(new PushNotificationExperimentSample<>(aciAndDeviceId.getT1(), aciAndDeviceId.getT2(), inExperimentGroup, initialState, finalState));
          } catch (final JsonProcessingException e) {
            sink.error(e);
          }
        });
  }

  public CompletableFuture<Void> discardSamples(final String experimentName, final int maxConcurrency) {
    final AttributeValue experimentNameAttributeValue = AttributeValue.fromS(experimentName);

    return Flux.from(dynamoDbAsyncClient.scanPaginator(ScanRequest.builder()
                .tableName(tableName)
                .filterExpression("#experiment = :experiment")
                .expressionAttributeNames(Map.of("#experiment", KEY_EXPERIMENT_NAME))
                .expressionAttributeValues(Map.of(":experiment", experimentNameAttributeValue))
                .projectionExpression(ATTR_ACI_AND_DEVICE_ID)
                .build())
            .items())
        .map(item -> item.get(ATTR_ACI_AND_DEVICE_ID))
        .flatMap(aciAndDeviceId -> Mono.fromFuture(() -> dynamoDbAsyncClient.deleteItem(DeleteItemRequest.builder()
            .tableName(tableName)
            .key(Map.of(
                KEY_EXPERIMENT_NAME, experimentNameAttributeValue,
                ATTR_ACI_AND_DEVICE_ID, aciAndDeviceId))
            .build()))
            .retryWhen(Retry.backoff(5, Duration.ofSeconds(5)))
            .onErrorResume(throwable -> {
              log.warn("Failed to delete sample for experiment {}", experimentName, throwable);
              return Mono.empty();
            }), maxConcurrency)
        .then()
        .toFuture();
  }

  @VisibleForTesting
  static AttributeValue buildSortKey(final UUID accountIdentifier, final byte deviceId) {
    return AttributeValue.fromB(SdkBytes.fromByteBuffer(ByteBuffer.allocate(17)
        .putLong(accountIdentifier.getMostSignificantBits())
        .putLong(accountIdentifier.getLeastSignificantBits())
        .put(deviceId)
        .flip()));
  }

  private static Tuple2<UUID, Byte> parseSortKey(final AttributeValue sortKey) {
    final ByteBuffer byteBuffer = sortKey.b().asByteBuffer();

    return Tuples.of(new UUID(byteBuffer.getLong(), byteBuffer.getLong()), byteBuffer.get());
  }

  private static <T> T parseState(final String state, final Class<T> clazz) throws JsonProcessingException {
    return SystemMapper.jsonMapper().readValue(state, clazz);
  }
}
