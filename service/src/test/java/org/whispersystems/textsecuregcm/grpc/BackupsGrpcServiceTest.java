/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.signal.chat.backup.BackupsGrpc;
import org.signal.chat.backup.GetBackupAuthCredentialsRequest;
import org.signal.chat.backup.GetBackupAuthCredentialsResponse;
import org.signal.chat.backup.RedeemReceiptRequest;
import org.signal.chat.backup.RedeemReceiptResponse;
import org.signal.chat.backup.SetBackupIdRequest;
import org.signal.chat.common.ZkCredential;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.ServerSecretParams;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.backups.BackupAuthCredentialRequest;
import org.signal.libsignal.zkgroup.backups.BackupCredentialType;
import org.signal.libsignal.zkgroup.backups.BackupLevel;
import org.signal.libsignal.zkgroup.receipts.ClientZkReceiptOperations;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredential;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialPresentation;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialRequestContext;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialResponse;
import org.signal.libsignal.zkgroup.receipts.ReceiptSerial;
import org.signal.libsignal.zkgroup.receipts.ServerZkReceiptOperations;
import org.whispersystems.textsecuregcm.auth.RedemptionRange;
import org.whispersystems.textsecuregcm.backup.BackupAuthManager;
import org.whispersystems.textsecuregcm.backup.BackupAuthTestUtil;
import org.whispersystems.textsecuregcm.backup.BackupBadReceiptException;
import org.whispersystems.textsecuregcm.backup.BackupException;
import org.whispersystems.textsecuregcm.backup.BackupInvalidArgumentException;
import org.whispersystems.textsecuregcm.backup.BackupMissingIdCommitmentException;
import org.whispersystems.textsecuregcm.backup.BackupNotFoundException;
import org.whispersystems.textsecuregcm.backup.BackupPermissionException;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.metrics.BackupMetrics;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.EnumMapUtil;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import javax.annotation.Nullable;

class BackupsGrpcServiceTest extends SimpleBaseGrpcTest<BackupsGrpcService, BackupsGrpc.BackupsBlockingStub> {

  private final byte[] messagesBackupKey = TestRandomUtil.nextBytes(32);
  private final byte[] mediaBackupKey = TestRandomUtil.nextBytes(32);
  private final BackupAuthTestUtil backupAuthTestUtil = new BackupAuthTestUtil(Clock.systemUTC());
  final BackupAuthCredentialRequest mediaAuthCredRequest =
      backupAuthTestUtil.getRequest(mediaBackupKey, AUTHENTICATED_ACI);
  final BackupAuthCredentialRequest messagesAuthCredRequest =
      backupAuthTestUtil.getRequest(messagesBackupKey, AUTHENTICATED_ACI);
  private Account account;
  private Device device;

  @Mock
  private BackupAuthManager backupAuthManager;
  @Mock
  private AccountsManager accountsManager;

  @Override
  protected BackupsGrpcService createServiceBeforeEachTest() {
    return new BackupsGrpcService(accountsManager, backupAuthManager, new BackupMetrics());
  }

  @BeforeEach
  void setup() {
    account = mock(Account.class);
    device = mock(Device.class);
    when(device.isPrimary()).thenReturn(true);
    when(accountsManager.getByAccountIdentifierAsync(AUTHENTICATED_ACI))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));
    when(accountsManager.getByAccountIdentifier(AUTHENTICATED_ACI))
        .thenReturn(Optional.of(account));
    when(account.getDevice(AUTHENTICATED_DEVICE_ID)).thenReturn(Optional.of(device));
  }


  @Test
  void setBackupId() throws RateLimitExceededException, BackupInvalidArgumentException, BackupPermissionException {
    authenticatedServiceStub().setBackupId(
        SetBackupIdRequest.newBuilder()
            .setMediaBackupAuthCredentialRequest(ByteString.copyFrom(mediaAuthCredRequest.serialize()))
            .setMessagesBackupAuthCredentialRequest(ByteString.copyFrom(messagesAuthCredRequest.serialize()))
            .build());

    verify(backupAuthManager)
        .commitBackupId(account, device, Optional.of(messagesAuthCredRequest), Optional.of(mediaAuthCredRequest));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void setBackupIdPartial(boolean media)
      throws RateLimitExceededException, BackupInvalidArgumentException, BackupPermissionException {
    final SetBackupIdRequest.Builder builder = SetBackupIdRequest.newBuilder();
    if (media) {
      builder.setMediaBackupAuthCredentialRequest(ByteString.copyFrom(mediaAuthCredRequest.serialize()));
    } else {
      builder.setMessagesBackupAuthCredentialRequest(ByteString.copyFrom(messagesAuthCredRequest.serialize()));
    }
    authenticatedServiceStub().setBackupId(builder.build());
    verify(backupAuthManager)
        .commitBackupId(account, device,
            Optional.ofNullable(media ? null : messagesAuthCredRequest),
            Optional.ofNullable(media ? mediaAuthCredRequest: null));
  }

  @Test
  void setBackupIdInvalid() {
    // invalid serialization
    GrpcTestUtils.assertStatusException(
        Status.INVALID_ARGUMENT, () -> authenticatedServiceStub().setBackupId(
            SetBackupIdRequest.newBuilder()
                .setMessagesBackupAuthCredentialRequest(ByteString.fromHex("FF"))
                .setMediaBackupAuthCredentialRequest(ByteString.fromHex("FF"))
                .build())
    );

  }

  public static Stream<Arguments> setBackupIdException() {
    return Stream.of(
        Arguments.of(new RateLimitExceededException(null), Status.RESOURCE_EXHAUSTED),
        Arguments.of(new BackupPermissionException("test"), Status.INVALID_ARGUMENT),
        Arguments.of(new BackupInvalidArgumentException("test"), Status.INVALID_ARGUMENT));
  }

  @ParameterizedTest
  @MethodSource
  void setBackupIdException(final Exception ex, final Status expected)
      throws RateLimitExceededException, BackupInvalidArgumentException, BackupPermissionException {
    doThrow(ex).when(backupAuthManager).commitBackupId(any(), any(), any(), any());

    GrpcTestUtils.assertStatusException(
        expected, () -> authenticatedServiceStub().setBackupId(SetBackupIdRequest.newBuilder()
            .setMediaBackupAuthCredentialRequest(ByteString.copyFrom(mediaAuthCredRequest.serialize()))
            .setMessagesBackupAuthCredentialRequest(ByteString.copyFrom(messagesAuthCredRequest.serialize()))
            .build())
    );
  }

  public static Stream<Arguments> redeemReceipt() {
    return Stream.of(
        Arguments.of(null, RedeemReceiptResponse.OutcomeCase.SUCCESS),
        Arguments.of(new BackupBadReceiptException("test"), RedeemReceiptResponse.OutcomeCase.INVALID_RECEIPT),
        Arguments.of(new BackupMissingIdCommitmentException(), RedeemReceiptResponse.OutcomeCase.ACCOUNT_MISSING_COMMITMENT));
  }

  @ParameterizedTest
  @MethodSource
  void redeemReceipt(@Nullable final BackupException exception, final RedeemReceiptResponse.OutcomeCase expectedOutcome)
      throws InvalidInputException, VerificationFailedException, BackupInvalidArgumentException, BackupMissingIdCommitmentException, BackupBadReceiptException {

    final ServerSecretParams params = ServerSecretParams.generate();
    final ServerZkReceiptOperations serverOps = new ServerZkReceiptOperations(params);
    final ClientZkReceiptOperations clientOps = new ClientZkReceiptOperations(params.getPublicParams());
    final ReceiptCredentialRequestContext rcrc = clientOps
        .createReceiptCredentialRequestContext(new ReceiptSerial(TestRandomUtil.nextBytes(ReceiptSerial.SIZE)));
    final ReceiptCredentialResponse rcr = serverOps.issueReceiptCredential(rcrc.getRequest(), 0L, 3L);
    final ReceiptCredential receiptCredential = clientOps.receiveReceiptCredential(rcrc, rcr);
    final ReceiptCredentialPresentation presentation = clientOps.createReceiptCredentialPresentation(receiptCredential);

    if (exception != null) {
      doThrow(exception).when(backupAuthManager).redeemReceipt(any(), any());
    }

    final RedeemReceiptResponse redeemReceiptResponse = authenticatedServiceStub().redeemReceipt(
        RedeemReceiptRequest.newBuilder()
            .setPresentation(ByteString.copyFrom(presentation.serialize()))
            .build());
    assertThat(redeemReceiptResponse.getOutcomeCase()).isEqualTo(expectedOutcome);

    verify(backupAuthManager).redeemReceipt(account, presentation);
  }


  @Test
  void getCredentials() throws BackupNotFoundException {
    final Instant start = Instant.now().truncatedTo(ChronoUnit.DAYS);
    final Instant end = start.plus(Duration.ofDays(1));
    final RedemptionRange expectedRange = RedemptionRange.inclusive(Clock.systemUTC(), start, end);

    final Map<BackupCredentialType, List<BackupAuthManager.Credential>> expectedCredentialsByType =
        EnumMapUtil.toEnumMap(BackupCredentialType.class, credentialType -> backupAuthTestUtil.getCredentials(
            BackupLevel.PAID, backupAuthTestUtil.getRequest(messagesBackupKey, AUTHENTICATED_ACI), credentialType,
            start, end));

    for (Map.Entry<BackupCredentialType, List<BackupAuthManager.Credential>> entry : expectedCredentialsByType.entrySet()) {
      final BackupCredentialType credentialType = entry.getKey();
      final List<BackupAuthManager.Credential> expectedCredentials = entry.getValue();
      when(backupAuthManager.getBackupAuthCredentials(any(), eq(credentialType), eq(expectedRange)))
          .thenReturn(expectedCredentials);
    }

    final GetBackupAuthCredentialsResponse credentialResponse = authenticatedServiceStub().getBackupAuthCredentials(
        GetBackupAuthCredentialsRequest.newBuilder()
            .setRedemptionStart(start.getEpochSecond()).setRedemptionStop(end.getEpochSecond())
            .build());

    expectedCredentialsByType.forEach((credentialType, expectedCredentials) -> {

      final Map<Long, ZkCredential> creds = switch (credentialType) {
        case MESSAGES -> credentialResponse.getCredentials().getMessageCredentialsMap();
        case MEDIA -> credentialResponse.getCredentials().getMediaCredentialsMap();
      };
      assertThat(creds).hasSize(expectedCredentials.size()).containsKey(start.getEpochSecond());

      for (BackupAuthManager.Credential expectedCred : expectedCredentials) {
        assertThat(creds)
            .extractingByKey(expectedCred.redemptionTime().getEpochSecond())
            .isNotNull()
            .extracting(ZkCredential::getCredential)
            .extracting(ByteString::toByteArray)
            .isEqualTo(expectedCred.credential().serialize());
      }
    });
  }

  @ParameterizedTest
  @CsvSource({
      "true, false",
      "false, true",
      "true, true"
  })
  void getCredentialsBadInput(final boolean missingStart, final boolean missingEnd) {
    final Instant start = Instant.now().truncatedTo(ChronoUnit.DAYS);
    final Instant end = start.plus(Duration.ofDays(1));

    final GetBackupAuthCredentialsRequest.Builder builder = GetBackupAuthCredentialsRequest.newBuilder();
    if (!missingStart) {
      builder.setRedemptionStart(start.getEpochSecond());
    }
    if (!missingEnd) {
      builder.setRedemptionStop(end.getEpochSecond());
    }

    GrpcTestUtils.assertStatusException(Status.INVALID_ARGUMENT,
        () -> authenticatedServiceStub().getBackupAuthCredentials(builder.build()));
  }

}
