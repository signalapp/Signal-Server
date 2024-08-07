package org.whispersystems.textsecuregcm.experiment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.time.Clock;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtension;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.Select;
import javax.annotation.Nullable;

class PushNotificationExperimentSamplesTest {

  private PushNotificationExperimentSamples pushNotificationExperimentSamples;

  @RegisterExtension
  public static final DynamoDbExtension DYNAMO_DB_EXTENSION =
      new DynamoDbExtension(DynamoDbExtensionSchema.Tables.PUSH_NOTIFICATION_EXPERIMENT_SAMPLES);

  private record TestDeviceState(int bounciness) {
  }

  @BeforeEach
  void setUp() {
    pushNotificationExperimentSamples =
        new PushNotificationExperimentSamples(DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
            DynamoDbExtensionSchema.Tables.PUSH_NOTIFICATION_EXPERIMENT_SAMPLES.tableName(),
            Clock.systemUTC());
  }

  @Test
  void recordInitialState() throws JsonProcessingException {
    final String experimentName = "test-experiment";
    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = (byte) ThreadLocalRandom.current().nextInt(Device.MAXIMUM_DEVICE_ID);
    final boolean inExperimentGroup = ThreadLocalRandom.current().nextBoolean();
    final int bounciness = ThreadLocalRandom.current().nextInt();

    assertTrue(pushNotificationExperimentSamples.recordInitialState(accountIdentifier,
            deviceId,
            experimentName,
            inExperimentGroup,
            new TestDeviceState(bounciness))
        .join(),
        "Attempt to record an initial state should succeed for entirely new records");

    assertEquals(new PushNotificationExperimentSample<>(accountIdentifier, deviceId, inExperimentGroup, new TestDeviceState(bounciness), null),
        getSample(accountIdentifier, deviceId, experimentName, TestDeviceState.class));

    assertTrue(pushNotificationExperimentSamples.recordInitialState(accountIdentifier,
                deviceId,
                experimentName,
                inExperimentGroup,
                new TestDeviceState(bounciness))
            .join(),
        "Attempt to re-record an initial state should succeed for existing-but-unchanged records");

    assertEquals(new PushNotificationExperimentSample<>(accountIdentifier, deviceId, inExperimentGroup, new TestDeviceState(bounciness), null),
        getSample(accountIdentifier, deviceId, experimentName, TestDeviceState.class),
        "Recorded initial state should be unchanged after repeated write");

    assertFalse(pushNotificationExperimentSamples.recordInitialState(accountIdentifier,
            deviceId,
            experimentName,
            inExperimentGroup,
            new TestDeviceState(bounciness + 1))
        .join(),
        "Attempt to record a conflicting initial state should fail");

    assertEquals(new PushNotificationExperimentSample<>(accountIdentifier, deviceId, inExperimentGroup, new TestDeviceState(bounciness), null),
        getSample(accountIdentifier, deviceId, experimentName, TestDeviceState.class),
        "Recorded initial state should be unchanged after unsuccessful write");

    assertFalse(pushNotificationExperimentSamples.recordInitialState(accountIdentifier,
            deviceId,
            experimentName,
            !inExperimentGroup,
            new TestDeviceState(bounciness))
        .join(),
        "Attempt to record a new group assignment should fail");

    assertEquals(new PushNotificationExperimentSample<>(accountIdentifier, deviceId, inExperimentGroup, new TestDeviceState(bounciness), null),
        getSample(accountIdentifier, deviceId, experimentName, TestDeviceState.class),
        "Recorded initial state should be unchanged after unsuccessful write");

    final int finalBounciness = bounciness + 17;

    pushNotificationExperimentSamples.recordFinalState(accountIdentifier,
            deviceId,
            experimentName,
            new TestDeviceState(finalBounciness))
        .join();

    assertFalse(pushNotificationExperimentSamples.recordInitialState(accountIdentifier,
                deviceId,
                experimentName,
                inExperimentGroup,
                new TestDeviceState(bounciness))
            .join(),
        "Attempt to record an initial state should fail for samples with final states");

    assertEquals(new PushNotificationExperimentSample<>(accountIdentifier, deviceId, inExperimentGroup, new TestDeviceState(bounciness), new TestDeviceState(finalBounciness)),
        getSample(accountIdentifier, deviceId, experimentName, TestDeviceState.class),
        "Recorded initial state should be unchanged after unsuccessful write");
  }

  @Test
  void recordFinalState() throws JsonProcessingException {
    final String experimentName = "test-experiment";
    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = (byte) ThreadLocalRandom.current().nextInt(Device.MAXIMUM_DEVICE_ID);
    final boolean inExperimentGroup = ThreadLocalRandom.current().nextBoolean();
    final int initialBounciness = ThreadLocalRandom.current().nextInt();
    final int finalBounciness = initialBounciness + 17;

    {
      pushNotificationExperimentSamples.recordInitialState(accountIdentifier,
              deviceId,
              experimentName,
              inExperimentGroup,
              new TestDeviceState(initialBounciness))
          .join();

      final PushNotificationExperimentSample<TestDeviceState> returnedSample =
          pushNotificationExperimentSamples.recordFinalState(accountIdentifier,
                  deviceId,
                  experimentName,
                  new TestDeviceState(finalBounciness))
              .join();

      final PushNotificationExperimentSample<TestDeviceState> expectedSample =
          new PushNotificationExperimentSample<>(accountIdentifier, deviceId, inExperimentGroup,
          new TestDeviceState(initialBounciness),
          new TestDeviceState(finalBounciness));

      assertEquals(expectedSample, returnedSample,
          "Attempt to update existing sample without final state should succeed");

      assertEquals(expectedSample, getSample(accountIdentifier, deviceId, experimentName, TestDeviceState.class),
          "Attempt to update existing sample without final state should be persisted");
    }

    assertThrows(CompletionException.class, () -> pushNotificationExperimentSamples.recordFinalState(accountIdentifier,
            (byte) (deviceId + 1),
            experimentName,
            new TestDeviceState(finalBounciness))
        .join(),
        "Attempts to record a final state without an initial state should fail");
  }

  @SuppressWarnings("SameParameterValue")
  private <T> PushNotificationExperimentSample<T> getSample(final UUID accountIdentifier,
      final byte deviceId,
      final String experimentName,
      final Class<T> stateClass) throws JsonProcessingException {

    final GetItemResponse response = DYNAMO_DB_EXTENSION.getDynamoDbClient().getItem(GetItemRequest.builder()
        .tableName(DynamoDbExtensionSchema.Tables.PUSH_NOTIFICATION_EXPERIMENT_SAMPLES.tableName())
        .key(Map.of(
            PushNotificationExperimentSamples.KEY_EXPERIMENT_NAME, AttributeValue.fromS(experimentName),
            PushNotificationExperimentSamples.ATTR_ACI_AND_DEVICE_ID, PushNotificationExperimentSamples.buildSortKey(accountIdentifier, deviceId)))
        .build());

    final boolean inExperimentGroup =
        response.item().get(PushNotificationExperimentSamples.ATTR_IN_EXPERIMENT_GROUP).bool();

    final T initialState =
        SystemMapper.jsonMapper().readValue(response.item().get(PushNotificationExperimentSamples.ATTR_INITIAL_STATE).s(), stateClass);

    @Nullable final T finalState = response.item().containsKey(PushNotificationExperimentSamples.ATTR_FINAL_STATE)
        ? SystemMapper.jsonMapper().readValue(response.item().get(PushNotificationExperimentSamples.ATTR_FINAL_STATE).s(), stateClass)
        : null;

    return new PushNotificationExperimentSample<>(accountIdentifier, deviceId, inExperimentGroup, initialState, finalState);
  }

  @Test
  void getSamples() throws JsonProcessingException {
    final String experimentName = "test-experiment";
    final UUID initialSampleAccountIdentifier = UUID.randomUUID();
    final byte initialSampleDeviceId = (byte) ThreadLocalRandom.current().nextInt(Device.MAXIMUM_DEVICE_ID);
    final boolean initialSampleInExperimentGroup = ThreadLocalRandom.current().nextBoolean();

    final UUID finalSampleAccountIdentifier = UUID.randomUUID();
    final byte finalSampleDeviceId = (byte) ThreadLocalRandom.current().nextInt(Device.MAXIMUM_DEVICE_ID);
    final boolean finalSampleInExperimentGroup = ThreadLocalRandom.current().nextBoolean();

    final int initialBounciness = ThreadLocalRandom.current().nextInt();
    final int finalBounciness = initialBounciness + 17;

    pushNotificationExperimentSamples.recordInitialState(initialSampleAccountIdentifier,
            initialSampleDeviceId,
            experimentName,
            initialSampleInExperimentGroup,
            new TestDeviceState(initialBounciness))
        .join();

    pushNotificationExperimentSamples.recordInitialState(finalSampleAccountIdentifier,
            finalSampleDeviceId,
            experimentName,
            finalSampleInExperimentGroup,
            new TestDeviceState(initialBounciness))
        .join();

    pushNotificationExperimentSamples.recordFinalState(finalSampleAccountIdentifier,
            finalSampleDeviceId,
            experimentName,
            new TestDeviceState(finalBounciness))
        .join();

    pushNotificationExperimentSamples.recordInitialState(UUID.randomUUID(),
            (byte) ThreadLocalRandom.current().nextInt(Device.MAXIMUM_DEVICE_ID),
            experimentName + "-different",
            ThreadLocalRandom.current().nextBoolean(),
            new TestDeviceState(ThreadLocalRandom.current().nextInt()))
        .join();

    final Set<PushNotificationExperimentSample<TestDeviceState>> expectedSamples = Set.of(
        new PushNotificationExperimentSample<>(initialSampleAccountIdentifier,
            initialSampleDeviceId,
            initialSampleInExperimentGroup,
            new TestDeviceState(initialBounciness),
            null),

        new PushNotificationExperimentSample<>(finalSampleAccountIdentifier,
            finalSampleDeviceId,
            finalSampleInExperimentGroup,
            new TestDeviceState(initialBounciness),
            new TestDeviceState(finalBounciness)));

    assertEquals(expectedSamples,
        pushNotificationExperimentSamples.getSamples(experimentName, TestDeviceState.class).collect(Collectors.toSet()).block());
  }

  @Test
  void discardSamples() throws JsonProcessingException {
    final String discardSamplesExperimentName = "discard-experiment";
    final String retainSamplesExperimentName = "retain-experiment";
    final int sampleCount = 16;

    for (int i = 0; i < sampleCount; i++) {
      pushNotificationExperimentSamples.recordInitialState(UUID.randomUUID(),
              Device.PRIMARY_ID,
              discardSamplesExperimentName,
              ThreadLocalRandom.current().nextBoolean(),
              new TestDeviceState(ThreadLocalRandom.current().nextInt()))
          .join();

      pushNotificationExperimentSamples.recordInitialState(UUID.randomUUID(),
              Device.PRIMARY_ID,
              retainSamplesExperimentName,
              ThreadLocalRandom.current().nextBoolean(),
              new TestDeviceState(ThreadLocalRandom.current().nextInt()))
          .join();
    }

    pushNotificationExperimentSamples.discardSamples(discardSamplesExperimentName, 1).join();

    {
      final QueryResponse queryResponse = DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient().query(QueryRequest.builder()
              .tableName(DynamoDbExtensionSchema.Tables.PUSH_NOTIFICATION_EXPERIMENT_SAMPLES.tableName())
              .keyConditionExpression("#experiment = :experiment")
              .expressionAttributeNames(Map.of("#experiment", PushNotificationExperimentSamples.KEY_EXPERIMENT_NAME))
              .expressionAttributeValues(Map.of(":experiment", AttributeValue.fromS(discardSamplesExperimentName)))
              .select(Select.COUNT)
              .build())
          .join();

      assertEquals(0, queryResponse.count());
    }

    {
      final QueryResponse queryResponse = DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient().query(QueryRequest.builder()
              .tableName(DynamoDbExtensionSchema.Tables.PUSH_NOTIFICATION_EXPERIMENT_SAMPLES.tableName())
              .keyConditionExpression("#experiment = :experiment")
              .expressionAttributeNames(Map.of("#experiment", PushNotificationExperimentSamples.KEY_EXPERIMENT_NAME))
              .expressionAttributeValues(Map.of(":experiment", AttributeValue.fromS(retainSamplesExperimentName)))
              .select(Select.COUNT)
              .build())
          .join();

      assertEquals(sampleCount, queryResponse.count());
    }
  }
}
