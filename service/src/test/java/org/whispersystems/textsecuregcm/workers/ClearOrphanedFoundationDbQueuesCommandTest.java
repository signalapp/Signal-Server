/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.ByteString;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountLockManager;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.FoundationDbClusterExtension;
import org.whispersystems.textsecuregcm.storage.foundationdb.FoundationDbMessageStore;
import org.whispersystems.textsecuregcm.storage.foundationdb.VersionstampUUIDCipher;
import org.whispersystems.textsecuregcm.util.TestClock;
import org.whispersystems.textsecuregcm.util.ThrowingSupplier;

class ClearOrphanedFoundationDbQueuesCommandTest {

  @RegisterExtension
  static FoundationDbClusterExtension FOUNDATION_DB_EXTENSION = new FoundationDbClusterExtension(2);

  private static final TestClock CLOCK = TestClock.pinned(Instant.ofEpochSecond(500));

  private FoundationDbMessageStore foundationDbMessageStore;

  private AccountsManager accountsManager;
  private AccountLockManager accountLockManager;

  @BeforeEach
  void setup() {
    final byte[] versionstampCipherKey = new byte[16];
    new SecureRandom().nextBytes(versionstampCipherKey);

    final List<Database> databases = Arrays.asList(FOUNDATION_DB_EXTENSION.getDatabases());

    accountsManager = mock(AccountsManager.class);
    accountLockManager = mock(AccountLockManager.class);

    when(accountLockManager.withLock(any(), any())).thenAnswer(invocation -> {
      //noinspection rawtypes
      final ThrowingSupplier supplier = invocation.getArgument(1);
      return supplier.get();
    });

    foundationDbMessageStore = new FoundationDbMessageStore(
        Map.of(0, databases),
        0,
        new VersionstampUUIDCipher(0, versionstampCipherKey),
        Executors.newSingleThreadScheduledExecutor(),
        CLOCK,
        Duration.ofSeconds(5),
        10);
  }

  @Test
  void clearOrphanedQueues() {
    // Create a few accounts and insert messages into their queues
    final List<AciServiceIdentifier> accounts = new ArrayList<>();
    for (int i = 0; i < 16; i++) {
      final AciServiceIdentifier aci = new AciServiceIdentifier(UUID.randomUUID());
      foundationDbMessageStore.insert(aci, Map.of(Device.PRIMARY_ID, generateRandomMessage())).join();
      foundationDbMessageStore.insert(aci, Map.of(Device.PRIMARY_ID, generateRandomMessage())).join();
      accounts.add(aci);
    }

    // Assume that a subset of accounts are deleted
    final Set<AciServiceIdentifier> deletedAccounts = new HashSet<>(accounts.subList(0, 3));

    when(accountsManager.getByAccountIdentifierAsync(any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(mock(Account.class))));
    for (final AciServiceIdentifier deletedAccount : deletedAccounts) {
      when(accountsManager.getByAccountIdentifierAsync(deletedAccount.uuid()))
          .thenReturn(CompletableFuture.completedFuture(Optional.empty()));
    }

    final ClearOrphanedFoundationDbQueuesCommand command = new ClearOrphanedFoundationDbQueuesCommand();
    command.clearOrphanedQueues(
        Arrays.stream(FOUNDATION_DB_EXTENSION.getDatabases()), accountsManager, 16, false, 8,
        3,
        Duration.ofSeconds(2),
        2,
        accountLockManager,
        Executors.newVirtualThreadPerTaskExecutor()
    );

    for (final AciServiceIdentifier aci : accounts) {
      assertEquals(!deletedAccounts.contains(aci), queueExists(FoundationDbMessageStore.getAccountSubspace(aci)));
    }
  }

  @Test
  void getAcisInShard() {
    // create accounts in a test subspace that doesn't conflict with the main messages subspace
    final Subspace testMessagesSubspace = new Subspace(
        Tuple.from("MT"));
    final ClearOrphanedFoundationDbQueuesCommand command = new ClearOrphanedFoundationDbQueuesCommand(
        testMessagesSubspace);
    final List<AciServiceIdentifier> acis = IntStream.range(0, 128)
        .mapToObj(_ -> new AciServiceIdentifier(UUID.randomUUID()))
        .toList();
    final Database database = FOUNDATION_DB_EXTENSION.getDatabases()[0];
    database.run(transaction -> {
      acis.forEach(aci -> {
        transaction.set(FoundationDbMessageStore.getAccountSubspace(testMessagesSubspace, aci).pack(Tuple.from("foo")),
            new byte[]{42});
        transaction.set(FoundationDbMessageStore.getAccountSubspace(testMessagesSubspace, aci).pack(Tuple.from("bar")),
            new byte[]{43});
      });
      return null;
    });
    final List<AciServiceIdentifier> fetchedAcis = command.getAcisInShard(database, 2, 3, Duration.ofSeconds(2), 5)
        .collectList()
        .block();
    assertNotNull(fetchedAcis);
    assertEquals(new HashSet<>(acis), new HashSet<>(fetchedAcis));
  }

  @Test
  void aciReusedAfterExistenceCheck() {
    final AciServiceIdentifier aci = new AciServiceIdentifier(UUID.randomUUID());
    foundationDbMessageStore.insert(aci, Map.of(Device.PRIMARY_ID, generateRandomMessage())).join();
    foundationDbMessageStore.insert(aci, Map.of(Device.PRIMARY_ID, generateRandomMessage())).join();

    // Stub that the initial existence check returns empty, but the second check under the ACI lock returns present i.e the ACI has been re-used
    // since the initial check
    when(accountsManager.getByAccountIdentifierAsync(aci.uuid()))
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));
    when(accountsManager.getByAccountIdentifier(aci.uuid())).thenReturn(Optional.of(mock(Account.class)));

    final ClearOrphanedFoundationDbQueuesCommand command = new ClearOrphanedFoundationDbQueuesCommand();
    command.clearOrphanedQueues(
        Arrays.stream(FOUNDATION_DB_EXTENSION.getDatabases()), accountsManager, 16, false, 8,
        3,
        Duration.ofSeconds(2),
        2,
        accountLockManager,
        Executors.newVirtualThreadPerTaskExecutor()
    );

    assertTrue(queueExists(FoundationDbMessageStore.getAccountSubspace(aci)));
  }

  /// Returns whether any key exists under the account's prefix in any shard
  private boolean queueExists(final Subspace accountSpace) {
    final Range accountRange = accountSpace.range();

    for (final Database database : FOUNDATION_DB_EXTENSION.getDatabases()) {
      final List<KeyValue> keyValues = database.readAsync(transaction ->
          AsyncUtil.collect(transaction.getRange(accountRange, 1))).join();

      if (!keyValues.isEmpty()) {
        return true;
      }
    }

    return false;
  }

  private static MessageProtos.Envelope generateRandomMessage() {
    final byte[] content = new byte[16];
    new SecureRandom().nextBytes(content);

    return MessageProtos.Envelope.newBuilder()
        .setClientTimestamp(CLOCK.millis())
        .setServerTimestamp(CLOCK.millis())
        .setContent(ByteString.copyFrom(content))
        .setEphemeral(false)
        .build();
  }
}
