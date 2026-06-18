/*
 * Copyright 2021-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.apple.foundationdb.tuple.Versionstamp;
import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.signal.libsignal.protocol.InvalidMessageException;
import org.signal.libsignal.protocol.InvalidVersionException;
import org.signal.libsignal.protocol.SealedSenderMultiRecipientMessage;
import org.signal.libsignal.protocol.ServiceId;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.PniServiceIdentifier;
import org.whispersystems.textsecuregcm.push.RedisMessageAvailabilityManager;
import org.whispersystems.textsecuregcm.storage.foundationdb.FoundationDbMessageStore;
import org.whispersystems.textsecuregcm.tests.util.MultiRecipientMessageHelper;
import org.whispersystems.textsecuregcm.tests.util.TestRecipient;
import org.whispersystems.textsecuregcm.util.TestClock;
import reactor.core.publisher.Mono;

class MessagesManagerTest {

  private MessagesDynamoDb messagesDynamoDb;
  private MessagesCache messagesCache;
  private FoundationDbMessageStore foundationDbMessageStore;
  private ReportMessageManager reportMessageManager;
  private ExperimentEnrollmentManager experimentEnrollmentManager;

  private static final TestClock CLOCK = TestClock.pinned(Instant.now());

  private MessagesManager messagesManager;

  @BeforeEach
  void setUp() {
    messagesDynamoDb = mock(MessagesDynamoDb.class);
    messagesCache = mock(MessagesCache.class);
    foundationDbMessageStore = mock(FoundationDbMessageStore.class);
    reportMessageManager = mock(ReportMessageManager.class);
    experimentEnrollmentManager = mock(ExperimentEnrollmentManager.class);

    when(messagesCache.insert(any(), any(), anyByte(), any())).thenReturn(CompletableFuture.completedFuture(true));

    messagesManager = new MessagesManager(messagesDynamoDb, messagesCache, foundationDbMessageStore,
        mock(RedisMessageAvailabilityManager.class), reportMessageManager, Executors.newSingleThreadExecutor(), CLOCK,
        experimentEnrollmentManager);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void insert(final boolean mirrorInsert) {
    final UUID sourceAci = UUID.randomUUID();
    final Envelope message = Envelope.newBuilder()
        .setSourceServiceId(new AciServiceIdentifier(sourceAci).toCompactByteString())
        .build();

    if (mirrorInsert) {
      when(experimentEnrollmentManager.isEnrolled(any(UUID.class), eq(MessagesManager.MIRROR_INSERTS_EXPERIMENT_NAME)))
          .thenReturn(true);

      when(foundationDbMessageStore.insert(any(), any()))
          .thenAnswer(invocation -> {
            final Map<Byte, Envelope> messagesByDeviceId = invocation.getArgument(1);

            return CompletableFuture.completedFuture(messagesByDeviceId.keySet().stream()
                .collect(Collectors.toMap(deviceId -> deviceId,
                    _ -> new FoundationDbMessageStore.InsertResult(Optional.of(Versionstamp.fromBytes(new byte[12])),
                        Optional.of(MessageGuidUtil.generateRandomV8UUID()),
                        true))));
          });
    }

    final UUID destinationUuid = UUID.randomUUID();

    messagesManager.insert(destinationUuid, Map.of(Device.PRIMARY_ID, message));

    verify(reportMessageManager).store(eq(sourceAci.toString()), any(UUID.class));

    final Envelope syncMessage = Envelope.newBuilder(message)
        .setSourceServiceId(new AciServiceIdentifier(destinationUuid).toCompactByteString())
        .build();

    messagesManager.insert(destinationUuid, Map.of(Device.PRIMARY_ID, syncMessage));

    verifyNoMoreInteractions(reportMessageManager);

    verify(foundationDbMessageStore, times(mirrorInsert ? 2 : 0)).insert(any(), any());
    verify(messagesCache, times(2))
        .insert(argThat(messageGuid -> messageGuid.version() == (mirrorInsert ? 8 : 4)),
            eq(destinationUuid),
            eq(Device.PRIMARY_ID),
            any());
  }

  @Test
  void insertFoundationDbException() {
    final UUID sourceAci = UUID.randomUUID();
    final Envelope message = Envelope.newBuilder()
        .setSourceServiceId(new AciServiceIdentifier(sourceAci).toCompactByteString())
        .build();

    when(experimentEnrollmentManager.isEnrolled(any(UUID.class), eq(MessagesManager.MIRROR_INSERTS_EXPERIMENT_NAME)))
        .thenReturn(true);

    when(foundationDbMessageStore.insert(any(), any()))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException()));

    final UUID destinationUuid = UUID.randomUUID();

    messagesManager.insert(destinationUuid, Map.of(Device.PRIMARY_ID, message));

    verify(foundationDbMessageStore).insert(any(), any());
    verify(messagesCache)
        .insert(argThat(messageGuid -> messageGuid.version() == 4),
            eq(destinationUuid),
            eq(Device.PRIMARY_ID),
            any());
  }

  private static UUID generateRandomV8UUID() {
    final long versionMask = 0x0000_0000_0000_f000L;
    final long v8Version = 0x0000_0000_0000_8000L;

    long mostSignificantBits = ThreadLocalRandom.current().nextLong();

    // Clear any bits in the version field
    mostSignificantBits &= ~versionMask;

    // Set the version to 8
    mostSignificantBits |= v8Version;

    return new UUID(mostSignificantBits, ThreadLocalRandom.current().nextLong());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void insertMultiRecipientMessage(final boolean mirrorInsert) throws InvalidMessageException, InvalidVersionException {
    if (mirrorInsert) {
      when(experimentEnrollmentManager.isEnrolled(any(UUID.class), eq(MessagesManager.MIRROR_INSERTS_EXPERIMENT_NAME)))
          .thenReturn(true);

      when(foundationDbMessageStore.insert(any(), any()))
          .thenAnswer(invocation -> {
            final Map<Byte, Envelope> messagesByDeviceId = invocation.getArgument(1);

            return CompletableFuture.completedFuture(messagesByDeviceId.keySet().stream()
                .collect(Collectors.toMap(deviceId -> deviceId,
                    _ -> new FoundationDbMessageStore.InsertResult(Optional.of(Versionstamp.fromBytes(new byte[12])),
                        Optional.of(generateRandomV8UUID()),
                        true))));
          });
    }

    final AciServiceIdentifier singleDeviceAccountAciServiceIdentifier = new AciServiceIdentifier(UUID.randomUUID());
    final PniServiceIdentifier singleDeviceAccountPniServiceIdentifier = new PniServiceIdentifier(UUID.randomUUID());
    final AciServiceIdentifier multiDeviceAccountAciServiceIdentifier = new AciServiceIdentifier(UUID.randomUUID());
    final AciServiceIdentifier unresolvedAccountAciServiceIdentifier = new AciServiceIdentifier(UUID.randomUUID());

    final Account singleDeviceAccount = mock(Account.class);
    final Account multiDeviceAccount = mock(Account.class);

    when(singleDeviceAccount.getIdentifier(IdentityType.ACI))
        .thenReturn(singleDeviceAccountAciServiceIdentifier.uuid());

    when(multiDeviceAccount.getIdentifier(IdentityType.ACI))
        .thenReturn(multiDeviceAccountAciServiceIdentifier.uuid());

    final byte[] multiRecipientMessageBytes = MultiRecipientMessageHelper.generateMultiRecipientMessage(List.of(
        new TestRecipient(singleDeviceAccountAciServiceIdentifier, Device.PRIMARY_ID, 1, new byte[48]),
        new TestRecipient(multiDeviceAccountAciServiceIdentifier, Device.PRIMARY_ID, 2, new byte[48]),
        new TestRecipient(multiDeviceAccountAciServiceIdentifier, (byte) (Device.PRIMARY_ID + 1), 3, new byte[48]),
        new TestRecipient(unresolvedAccountAciServiceIdentifier, Device.PRIMARY_ID, 4, new byte[48]),
        new TestRecipient(singleDeviceAccountPniServiceIdentifier, Device.PRIMARY_ID, 5, new byte[48])
    ));

    final SealedSenderMultiRecipientMessage multiRecipientMessage =
        SealedSenderMultiRecipientMessage.parse(multiRecipientMessageBytes);

    final Map<SealedSenderMultiRecipientMessage.Recipient, Account> resolvedRecipients = new HashMap<>();

    multiRecipientMessage.getRecipients().forEach(((serviceId, recipient) -> {
      if (serviceId.getRawUUID().equals(singleDeviceAccountAciServiceIdentifier.uuid()) ||
          serviceId.getRawUUID().equals(singleDeviceAccountPniServiceIdentifier.uuid())) {
        resolvedRecipients.put(recipient, singleDeviceAccount);
      } else if (serviceId.getRawUUID().equals(multiDeviceAccountAciServiceIdentifier.uuid())) {
        resolvedRecipients.put(recipient, multiDeviceAccount);
      }
    }));

    final Map<Account, Map<Byte, Boolean>> expectedPresenceByAccountAndDeviceId = Map.of(
        singleDeviceAccount, Map.of(Device.PRIMARY_ID, true),
        multiDeviceAccount, Map.of(Device.PRIMARY_ID, false, (byte) (Device.PRIMARY_ID + 1), true)
    );

    final Map<UUID, Map<Byte, Boolean>> presenceByAccountIdentifierAndDeviceId = Map.of(
        singleDeviceAccountAciServiceIdentifier.uuid(), Map.of(Device.PRIMARY_ID, true),
        multiDeviceAccountAciServiceIdentifier.uuid(), Map.of(Device.PRIMARY_ID, false, (byte) (Device.PRIMARY_ID + 1), true)
    );

    final byte[] sharedMrmKey = "shared-mrm-key".getBytes(StandardCharsets.UTF_8);

    when(messagesCache.insertSharedMultiRecipientMessagePayload(multiRecipientMessage))
        .thenReturn(CompletableFuture.completedFuture(sharedMrmKey));

    when(messagesCache.insert(any(), any(), anyByte(), any()))
        .thenAnswer(invocation -> {
          final UUID accountIdentifier = invocation.getArgument(1);
          final byte deviceId = invocation.getArgument(2);

          return CompletableFuture.completedFuture(
              presenceByAccountIdentifierAndDeviceId.getOrDefault(accountIdentifier, Collections.emptyMap())
                  .getOrDefault(deviceId, false));
        });

    final long clientTimestamp = System.currentTimeMillis();
    final boolean isStory = ThreadLocalRandom.current().nextBoolean();
    final boolean isEphemeral = ThreadLocalRandom.current().nextBoolean();
    final boolean isUrgent = ThreadLocalRandom.current().nextBoolean();

    final Envelope.Builder expectedEnvelopeBuilder = Envelope.newBuilder()
        .setType(Envelope.Type.UNIDENTIFIED_SENDER)
        .setClientTimestamp(clientTimestamp)
        .setServerTimestamp(CLOCK.millis())
        .setEphemeral(isEphemeral)
        .setUrgent(isUrgent)
        .setSharedMrmKey(ByteString.copyFrom(sharedMrmKey));
    if (isStory) {
        expectedEnvelopeBuilder.setStory(true);
    }
    final Envelope prototypeExpectedMessage = expectedEnvelopeBuilder.build();

    assertEquals(expectedPresenceByAccountAndDeviceId,
        messagesManager.insertMultiRecipientMessage(multiRecipientMessage, resolvedRecipients, clientTimestamp, isStory, isEphemeral, isUrgent).join());

    verify(messagesCache).insert(any(),
        eq(singleDeviceAccountAciServiceIdentifier.uuid()),
        eq(Device.PRIMARY_ID),
        eq(prototypeExpectedMessage.toBuilder().setDestinationServiceId(singleDeviceAccountAciServiceIdentifier.toCompactByteString()).build()));

    verify(messagesCache).insert(any(),
        eq(singleDeviceAccountAciServiceIdentifier.uuid()),
        eq(Device.PRIMARY_ID),
        eq(prototypeExpectedMessage.toBuilder().setDestinationServiceId(singleDeviceAccountPniServiceIdentifier.toCompactByteString()).build()));

    verify(messagesCache).insert(any(),
        eq(multiDeviceAccountAciServiceIdentifier.uuid()),
        eq((byte) (Device.PRIMARY_ID + 1)),
        eq(prototypeExpectedMessage.toBuilder().setDestinationServiceId(multiDeviceAccountAciServiceIdentifier.toCompactByteString()).build()));

    verify(messagesCache, never()).insert(any(),
        eq(unresolvedAccountAciServiceIdentifier.uuid()),
        anyByte(),
        any());

    if (mirrorInsert) {
      final Envelope prototypeExpectedFoundationDbMessage = prototypeExpectedMessage.toBuilder().clearSharedMrmKey().build();

      verify(foundationDbMessageStore).insert(singleDeviceAccountAciServiceIdentifier,
          Map.of(Device.PRIMARY_ID, prototypeExpectedFoundationDbMessage.toBuilder()
              .setDestinationServiceId(singleDeviceAccountAciServiceIdentifier.toCompactByteString())
              .setContent(ByteString.copyFrom(multiRecipientMessage.messageForRecipient(multiRecipientMessage.getRecipients().get(new ServiceId.Aci(singleDeviceAccountAciServiceIdentifier.uuid())))))
                  .build()));

      verify(foundationDbMessageStore).insert(singleDeviceAccountAciServiceIdentifier,
          Map.of(Device.PRIMARY_ID, prototypeExpectedFoundationDbMessage.toBuilder()
              .setDestinationServiceId(singleDeviceAccountPniServiceIdentifier.toCompactByteString())
              .setContent(ByteString.copyFrom(multiRecipientMessage.messageForRecipient(multiRecipientMessage.getRecipients().get(new ServiceId.Pni(singleDeviceAccountPniServiceIdentifier.uuid())))))
              .build()));

      verify(foundationDbMessageStore).insert(multiDeviceAccountAciServiceIdentifier,
          Map.of(
              Device.PRIMARY_ID, prototypeExpectedFoundationDbMessage.toBuilder()
                  .setDestinationServiceId(multiDeviceAccountAciServiceIdentifier.toCompactByteString())
                  .setContent(ByteString.copyFrom(multiRecipientMessage.messageForRecipient(multiRecipientMessage.getRecipients().get(new ServiceId.Aci(multiDeviceAccountAciServiceIdentifier.uuid())))))
                  .build(),
              (byte) (Device.PRIMARY_ID + 1), prototypeExpectedFoundationDbMessage.toBuilder()
                  .setDestinationServiceId(multiDeviceAccountAciServiceIdentifier.toCompactByteString())
                  .setContent(ByteString.copyFrom(multiRecipientMessage.messageForRecipient(multiRecipientMessage.getRecipients().get(new ServiceId.Aci(multiDeviceAccountAciServiceIdentifier.uuid())))))
                  .build())
          );

      verify(foundationDbMessageStore, never()).insert(eq(unresolvedAccountAciServiceIdentifier), any());
    } else {
      verifyNoInteractions(foundationDbMessageStore);
    }
  }

  @ParameterizedTest
  @CsvSource({
      "false, false, false",
      "false, true, true",
      "true, false, true",
      "true, true, true"
  })
  void mayHaveMessages(final boolean hasCachedMessages, final boolean hasPersistedMessages, final boolean expectMayHaveMessages) {
    final UUID accountIdentifier = UUID.randomUUID();
    final Device device = mock(Device.class);
    when(device.getId()).thenReturn(Device.PRIMARY_ID);

    when(messagesCache.hasMessagesAsync(accountIdentifier, Device.PRIMARY_ID))
        .thenReturn(CompletableFuture.completedFuture(hasCachedMessages));

    when(messagesDynamoDb.mayHaveMessages(accountIdentifier, device))
        .thenReturn(CompletableFuture.completedFuture(hasPersistedMessages));

    if (hasCachedMessages) {
      verifyNoInteractions(messagesDynamoDb);
    }

    assertEquals(expectMayHaveMessages, messagesManager.mayHaveMessages(accountIdentifier, device).join());
  }

  @ParameterizedTest
  @CsvSource({
      ",,",
      "1,,1",
      ",1,1",
      "2,1,1",
      "1,2,2"
  })
  public void oldestMessageTimestamp(Long oldestCached, Long oldestPersisted, Long expected) {
    final UUID accountIdentifier = UUID.randomUUID();
    final Device device = mock(Device.class);
    when(device.getId()).thenReturn(Device.PRIMARY_ID);

    when(messagesCache.getEarliestUndeliveredTimestamp(accountIdentifier, Device.PRIMARY_ID))
        .thenReturn(oldestCached == null ? Mono.empty() : Mono.just(oldestCached));
    when(messagesDynamoDb.load(accountIdentifier, device, 1))
        .thenReturn(oldestPersisted == null
            ? Mono.empty()
            : Mono.just(Envelope.newBuilder().setServerTimestamp(oldestPersisted).build()));
    final Optional<Instant> earliest =
        messagesManager.getEarliestUndeliveredTimestampForDevice(accountIdentifier, device).join();
    assertEquals(Optional.ofNullable(expected).map(Instant::ofEpochMilli), earliest);
  }
}
