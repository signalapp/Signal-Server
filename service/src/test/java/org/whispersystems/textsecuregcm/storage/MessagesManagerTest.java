/*
 * Copyright 2021-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.signal.libsignal.protocol.InvalidMessageException;
import org.signal.libsignal.protocol.InvalidVersionException;
import org.signal.libsignal.protocol.SealedSenderMultiRecipientMessage;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.PniServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.push.RedisMessageAvailabilityManager;
import org.whispersystems.textsecuregcm.tests.util.MultiRecipientMessageHelper;
import org.whispersystems.textsecuregcm.tests.util.TestRecipient;
import org.whispersystems.textsecuregcm.util.TestClock;
import reactor.core.publisher.Mono;

class MessagesManagerTest {

  private final MessagesDynamoDb messagesDynamoDb = mock(MessagesDynamoDb.class);
  private final MessagesCache messagesCache = mock(MessagesCache.class);
  private final ReportMessageManager reportMessageManager = mock(ReportMessageManager.class);

  private static final TestClock CLOCK = TestClock.pinned(Instant.now());

  private final MessagesManager messagesManager = new MessagesManager(messagesDynamoDb, messagesCache,
      mock(RedisMessageAvailabilityManager.class), reportMessageManager, Executors.newSingleThreadExecutor(), CLOCK);

  @BeforeEach
  void setUp() {
    when(messagesCache.insert(any(), any(), anyByte(), any())).thenReturn(CompletableFuture.completedFuture(true));
  }

  @Test
  void insert() {
    final UUID sourceAci = UUID.randomUUID();
    final Envelope message = Envelope.newBuilder()
        .setSourceServiceId(sourceAci.toString())
        .build();

    final UUID destinationUuid = UUID.randomUUID();

    messagesManager.insert(destinationUuid, Map.of(Device.PRIMARY_ID, message));

    verify(reportMessageManager).store(eq(sourceAci.toString()), any(UUID.class));

    final Envelope syncMessage = Envelope.newBuilder(message)
        .setSourceServiceId(destinationUuid.toString())
        .build();

    messagesManager.insert(destinationUuid, Map.of(Device.PRIMARY_ID, syncMessage));

    verifyNoMoreInteractions(reportMessageManager);
  }

  @Test
  void insertMultiRecipientMessage() throws InvalidMessageException, InvalidVersionException {
    final ServiceIdentifier singleDeviceAccountAciServiceIdentifier = new AciServiceIdentifier(UUID.randomUUID());
    final ServiceIdentifier singleDeviceAccountPniServiceIdentifier = new PniServiceIdentifier(UUID.randomUUID());
    final ServiceIdentifier multiDeviceAccountAciServiceIdentifier = new AciServiceIdentifier(UUID.randomUUID());
    final ServiceIdentifier unresolvedAccountAciServiceIdentifier = new AciServiceIdentifier(UUID.randomUUID());

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

    final Envelope prototypeExpectedMessage = Envelope.newBuilder()
        .setType(Envelope.Type.UNIDENTIFIED_SENDER)
        .setClientTimestamp(clientTimestamp)
        .setServerTimestamp(CLOCK.millis())
        .setStory(isStory)
        .setEphemeral(isEphemeral)
        .setUrgent(isUrgent)
        .setSharedMrmKey(ByteString.copyFrom(sharedMrmKey))
        .build();

    assertEquals(expectedPresenceByAccountAndDeviceId,
        messagesManager.insertMultiRecipientMessage(multiRecipientMessage, resolvedRecipients, clientTimestamp, isStory, isEphemeral, isUrgent).join());

    verify(messagesCache).insert(any(),
        eq(singleDeviceAccountAciServiceIdentifier.uuid()),
        eq(Device.PRIMARY_ID),
        eq(prototypeExpectedMessage.toBuilder().setDestinationServiceId(singleDeviceAccountAciServiceIdentifier.toServiceIdentifierString()).build()));

    verify(messagesCache).insert(any(),
        eq(singleDeviceAccountAciServiceIdentifier.uuid()),
        eq(Device.PRIMARY_ID),
        eq(prototypeExpectedMessage.toBuilder().setDestinationServiceId(singleDeviceAccountPniServiceIdentifier.toServiceIdentifierString()).build()));

    verify(messagesCache).insert(any(),
        eq(multiDeviceAccountAciServiceIdentifier.uuid()),
        eq((byte) (Device.PRIMARY_ID + 1)),
        eq(prototypeExpectedMessage.toBuilder().setDestinationServiceId(multiDeviceAccountAciServiceIdentifier.toServiceIdentifierString()).build()));

    verify(messagesCache, never()).insert(any(),
        eq(unresolvedAccountAciServiceIdentifier.uuid()),
        anyByte(),
        any());
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
