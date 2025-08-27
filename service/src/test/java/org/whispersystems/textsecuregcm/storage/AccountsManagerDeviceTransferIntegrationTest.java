/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.auth.DisconnectionRequestManager;
import org.whispersystems.textsecuregcm.entities.RemoteAttachmentError;
import org.whispersystems.textsecuregcm.entities.RestoreAccountRequest;
import org.whispersystems.textsecuregcm.entities.RemoteAttachment;
import org.whispersystems.textsecuregcm.entities.TransferArchiveResult;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;
import org.whispersystems.textsecuregcm.redis.RedisServerExtension;
import org.whispersystems.textsecuregcm.securestorage.SecureStorageClient;
import org.whispersystems.textsecuregcm.securevaluerecovery.SecureValueRecoveryClient;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

// ThreadMode.SEPARATE_THREAD protects against hangs in the remote Redis calls, as this mode allows the test code to be
// preempted by the timeout check
@Timeout(value = 5, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
public class AccountsManagerDeviceTransferIntegrationTest {

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
        mock(SecureValueRecoveryClient.class),
        mock(DisconnectionRequestManager.class),
        mock(RegistrationRecoveryPasswordsManager.class),
        mock(ClientPublicKeysManager.class),
        mock(ExecutorService.class),
        mock(ScheduledExecutorService.class),
        mock(ScheduledExecutorService.class),
        Clock.systemUTC(),
        "link-device-secret".getBytes(StandardCharsets.UTF_8),
        mock(DynamicConfigurationManager.class));

    accountsManager.start();
  }

  @AfterEach
  void tearDown() {
    accountsManager.stop();
  }

  @ParameterizedTest
  @MethodSource
  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  void waitForTransferArchive(
      final Optional<Long> recordUploadDeviceCreated,
      final Optional<Integer> recordUploadRegistrationId) {
    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = Device.PRIMARY_ID;

    final RemoteAttachment transferArchive =
        new RemoteAttachment(3, Base64.getUrlEncoder().encodeToString("transfer-archive".getBytes(StandardCharsets.UTF_8)));

    final Device device = mock(Device.class);
    when(device.getId()).thenReturn(deviceId);
    when(device.getCreated()).thenReturn(recordUploadDeviceCreated.orElse(System.currentTimeMillis()));
    when(device.getRegistrationId(IdentityType.ACI)).thenReturn(recordUploadRegistrationId.orElse(1));

    final Account account = mock(Account.class);
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(accountIdentifier);

    final CompletableFuture<Optional<TransferArchiveResult>> displacedFuture =
        accountsManager.waitForTransferArchive(account, device, Duration.ofSeconds(5));

    final CompletableFuture<Optional<TransferArchiveResult>> activeFuture =
        accountsManager.waitForTransferArchive(account, device, Duration.ofSeconds(5));

    assertEquals(Optional.empty(), displacedFuture.join());

    accountsManager.recordTransferArchiveUpload(account, deviceId, recordUploadDeviceCreated.map(Instant::ofEpochMilli), recordUploadRegistrationId, transferArchive).join();

    assertEquals(Optional.of(transferArchive), activeFuture.join());
  }

  private static List<Arguments> waitForTransferArchive() {
    final long deviceCreated = System.currentTimeMillis();
    final int registrationId = 123;

    return List.of(
        Arguments.of(Optional.empty(), Optional.of(registrationId)),
        Arguments.of(Optional.of(deviceCreated), Optional.empty())
    );
  }

  @ParameterizedTest
  @MethodSource
  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  void waitForTransferArchiveAlreadyAdded(
      final Optional<Long> recordUploadDeviceCreated,
      final Optional<Integer> recordUploadRegistrationId) {
    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = Device.PRIMARY_ID;


    final RemoteAttachment transferArchive =
        new RemoteAttachment(3, Base64.getUrlEncoder().encodeToString("transfer-archive".getBytes(StandardCharsets.UTF_8)));

    final Device device = mock(Device.class);
    when(device.getId()).thenReturn(deviceId);
    when(device.getCreated()).thenReturn(recordUploadDeviceCreated.orElse(System.currentTimeMillis()));
    when(device.getRegistrationId(IdentityType.ACI)).thenReturn(recordUploadRegistrationId.orElse(1));

    final Account account = mock(Account.class);
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(accountIdentifier);

    accountsManager.recordTransferArchiveUpload(account, deviceId, recordUploadDeviceCreated.map(Instant::ofEpochMilli), recordUploadRegistrationId, transferArchive).join();

    assertEquals(Optional.of(transferArchive),
        accountsManager.waitForTransferArchive(account, device, Duration.ofSeconds(5)).join());
  }

  private static List<Arguments> waitForTransferArchiveAlreadyAdded() {
    final long deviceCreated = System.currentTimeMillis();
    final int registrationId = 123;

    return List.of(
        Arguments.of(Optional.empty(), Optional.of(registrationId)),
        Arguments.of(Optional.of(deviceCreated), Optional.empty())
    );
  }

  @ParameterizedTest
  @MethodSource
  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  void waitForErrorTransferArchive(
      final Optional<Long> recordUploadDeviceCreated,
      final Optional<Integer> recordUploadRegistrationId) {
    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = Device.PRIMARY_ID;

    final RemoteAttachmentError transferArchiveError =
        new RemoteAttachmentError(RemoteAttachmentError.ErrorType.CONTINUE_WITHOUT_UPLOAD);

    final Device device = mock(Device.class);
    when(device.getId()).thenReturn(deviceId);
    when(device.getCreated()).thenReturn(recordUploadDeviceCreated.orElse(System.currentTimeMillis()));
    when(device.getRegistrationId(IdentityType.ACI)).thenReturn(recordUploadRegistrationId.orElse(1));

    final Account account = mock(Account.class);
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(accountIdentifier);

    accountsManager.recordTransferArchiveUpload(account, deviceId, recordUploadDeviceCreated.map(Instant::ofEpochMilli),
        recordUploadRegistrationId, transferArchiveError).join();

    assertEquals(Optional.of(transferArchiveError),
        accountsManager.waitForTransferArchive(account, device, Duration.ofSeconds(5)).join());
  }

  private static List<Arguments> waitForErrorTransferArchive() {
    final long deviceCreated = System.currentTimeMillis();
    final int registrationId = 123;

    return List.of(
        Arguments.of(Optional.empty(), Optional.of(registrationId)),
        Arguments.of(Optional.of(deviceCreated), Optional.empty())
    );
  }

  @ParameterizedTest
  @MethodSource
  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  void waitForTransferArchiveTimeout(
      final Optional<Long> recordUploadDeviceCreated,
      final Optional<Integer> recordUploadRegistrationId) {
    final UUID accountIdentifier = UUID.randomUUID();

    final Device device = mock(Device.class);
    when(device.getCreated()).thenReturn(recordUploadDeviceCreated.orElse(System.currentTimeMillis()));
    when(device.getRegistrationId(IdentityType.ACI)).thenReturn(recordUploadRegistrationId.orElse(1));

    final Account account = mock(Account.class);
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(accountIdentifier);

    assertEquals(Optional.empty(),
        accountsManager.waitForTransferArchive(account, device, Duration.ofMillis(1)).join());
  }

  private static List<Arguments> waitForTransferArchiveTimeout() {
    final long deviceCreated = System.currentTimeMillis();
    final int registrationId = 123;

    return List.of(
        Arguments.of(Optional.empty(), Optional.of(registrationId)),
        Arguments.of(Optional.of(deviceCreated), Optional.empty())
    );
  }

  @Test
  void waitForRestoreAccountRequest() {
    final String token = RandomStringUtils.secure().nextAlphanumeric(16);
    final byte[] deviceTransferBootstrap = TestRandomUtil.nextBytes(100);
    final RestoreAccountRequest restoreAccountRequest =
        new RestoreAccountRequest(RestoreAccountRequest.Method.DEVICE_TRANSFER, deviceTransferBootstrap);

    final CompletableFuture<Optional<RestoreAccountRequest>> displacedFuture =
        accountsManager.waitForRestoreAccountRequest(token, Duration.ofSeconds(5));

    final CompletableFuture<Optional<RestoreAccountRequest>> activeFuture =
        accountsManager.waitForRestoreAccountRequest(token, Duration.ofSeconds(5));

    assertEquals(Optional.empty(), displacedFuture.join());

    accountsManager.recordRestoreAccountRequest(token, restoreAccountRequest).join();

    final Optional<RestoreAccountRequest> result = activeFuture.join();
    assertTrue(result.isPresent());
    assertEquals(restoreAccountRequest.method(), result.get().method());
    assertArrayEquals(restoreAccountRequest.deviceTransferBootstrap(), result.get().deviceTransferBootstrap());
  }

  @Test
  void waitForRestoreAccountRequestAlreadyRequested() {
    final String token = RandomStringUtils.secure().nextAlphanumeric(16);
    final RestoreAccountRequest restoreAccountRequest =
        new RestoreAccountRequest(RestoreAccountRequest.Method.DEVICE_TRANSFER, null);

    accountsManager.recordRestoreAccountRequest(token, restoreAccountRequest).join();

    assertEquals(Optional.of(restoreAccountRequest),
        accountsManager.waitForRestoreAccountRequest(token, Duration.ofSeconds(5)).join());
  }

  @Test
  void waitForRestoreAccountRequestTimeout() {
    assertEquals(Optional.empty(),
        accountsManager.waitForRestoreAccountRequest(RandomStringUtils.secure().nextAlphanumeric(16),
            Duration.ofMillis(1)).join());
  }
}
