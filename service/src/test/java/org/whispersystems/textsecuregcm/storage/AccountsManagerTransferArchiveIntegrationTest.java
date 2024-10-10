/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.entities.RemoteAttachment;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.push.ClientPresenceManager;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;
import org.whispersystems.textsecuregcm.redis.RedisServerExtension;
import org.whispersystems.textsecuregcm.securestorage.SecureStorageClient;
import org.whispersystems.textsecuregcm.securevaluerecovery.SecureValueRecovery2Client;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

// ThreadMode.SEPARATE_THREAD protects against hangs in the remote Redis calls, as this mode allows the test code to be
// preempted by the timeout check
@Timeout(value = 5, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
public class AccountsManagerTransferArchiveIntegrationTest {

  @RegisterExtension
  static final RedisServerExtension PUBSUB_SERVER_EXTENSION = RedisServerExtension.builder().build();

  private AccountsManager accountsManager;

  @BeforeEach
  void setUp() {
    PUBSUB_SERVER_EXTENSION.getRedisClient().useConnection(connection -> {
      connection.sync().flushall();
      connection.sync().configSet("notify-keyspace-events", "K$");
    });

    //noinspection unchecked
    accountsManager = new AccountsManager(
        mock(Accounts.class),
        mock(PhoneNumberIdentifiers.class),
        mock(FaultTolerantRedisClusterClient.class),
        PUBSUB_SERVER_EXTENSION.getRedisClient(),
        mock(AccountLockManager.class),
        mock(KeysManager.class),
        mock(MessagesManager.class),
        mock(ProfilesManager.class),
        mock(SecureStorageClient.class),
        mock(SecureValueRecovery2Client.class),
        mock(ClientPresenceManager.class),
        mock(RegistrationRecoveryPasswordsManager.class),
        mock(ClientPublicKeysManager.class),
        mock(ExecutorService.class),
        mock(ExecutorService.class),
        Clock.systemUTC(),
        "link-device-secret".getBytes(StandardCharsets.UTF_8),
        mock(DynamicConfigurationManager.class));

    accountsManager.start();
  }

  @AfterEach
  void tearDown() {
    accountsManager.stop();
  }

  @Test
  void waitForTransferArchive() {
    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = Device.PRIMARY_ID;
    final long deviceCreated = System.currentTimeMillis();

    final RemoteAttachment transferArchive =
        new RemoteAttachment(3, Base64.getUrlEncoder().encodeToString("transfer-archive".getBytes(StandardCharsets.UTF_8)));

    final Device device = mock(Device.class);
    when(device.getId()).thenReturn(deviceId);
    when(device.getCreated()).thenReturn(deviceCreated);

    final Account account = mock(Account.class);
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(accountIdentifier);

    final CompletableFuture<Optional<RemoteAttachment>> displacedFuture =
        accountsManager.waitForTransferArchive(account, device, Duration.ofSeconds(5));

    final CompletableFuture<Optional<RemoteAttachment>> activeFuture =
        accountsManager.waitForTransferArchive(account, device, Duration.ofSeconds(5));

    assertEquals(Optional.empty(), displacedFuture.join());

    accountsManager.recordTransferArchiveUpload(account, deviceId, Instant.ofEpochMilli(deviceCreated), transferArchive).join();

    assertEquals(Optional.of(transferArchive), activeFuture.join());
  }

  @Test
  void waitForTransferArchiveAlreadyAdded() {
    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = Device.PRIMARY_ID;
    final long deviceCreated = System.currentTimeMillis();

    final RemoteAttachment transferArchive =
        new RemoteAttachment(3, Base64.getUrlEncoder().encodeToString("transfer-archive".getBytes(StandardCharsets.UTF_8)));

    final Device device = mock(Device.class);
    when(device.getId()).thenReturn(deviceId);
    when(device.getCreated()).thenReturn(deviceCreated);

    final Account account = mock(Account.class);
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(accountIdentifier);

    accountsManager.recordTransferArchiveUpload(account, deviceId, Instant.ofEpochMilli(deviceCreated), transferArchive).join();

    assertEquals(Optional.of(transferArchive),
        accountsManager.waitForTransferArchive(account, device, Duration.ofSeconds(5)).join());
  }

  @Test
  void waitForTransferArchiveTimeout() {
    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = Device.PRIMARY_ID;
    final long deviceCreated = System.currentTimeMillis();

    final Device device = mock(Device.class);
    when(device.getId()).thenReturn(deviceId);
    when(device.getCreated()).thenReturn(deviceCreated);

    final Account account = mock(Account.class);
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(accountIdentifier);

    assertEquals(Optional.empty(),
        accountsManager.waitForTransferArchive(account, device, Duration.ofMillis(1)).join());
  }
}
