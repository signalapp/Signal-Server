/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.micrometer.core.instrument.Tag;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.signal.chat.backup.GetBackupAuthCredentialsRequest;
import org.signal.chat.backup.GetBackupAuthCredentialsResponse;
import org.signal.chat.backup.RedeemReceiptRequest;
import org.signal.chat.backup.RedeemReceiptResponse;
import org.signal.chat.backup.SetBackupIdRequest;
import org.signal.chat.backup.SetBackupIdResponse;
import org.signal.chat.backup.SimpleBackupsGrpc;
import org.signal.chat.common.ZkCredential;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.backups.BackupAuthCredentialRequest;
import org.signal.libsignal.zkgroup.backups.BackupCredentialType;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialPresentation;
import org.whispersystems.textsecuregcm.auth.RedemptionRange;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticationUtil;
import org.whispersystems.textsecuregcm.backup.BackupAuthManager;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.metrics.BackupMetrics;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;

public class BackupsGrpcService extends SimpleBackupsGrpc.BackupsImplBase {

  private final AccountsManager accountManager;
  private final BackupAuthManager backupAuthManager;
  private final BackupMetrics backupMetrics;

  public BackupsGrpcService(final AccountsManager accountManager, final BackupAuthManager backupAuthManager, final BackupMetrics backupMetrics) {
    this.accountManager = accountManager;
    this.backupAuthManager = backupAuthManager;
    this.backupMetrics = backupMetrics;
  }

  @Override
  public SetBackupIdResponse setBackupId(SetBackupIdRequest request) throws RateLimitExceededException {

    final Optional<BackupAuthCredentialRequest> messagesCredentialRequest = deserializeWithEmptyPresenceCheck(
        BackupAuthCredentialRequest::new,
        request.getMessagesBackupAuthCredentialRequest());

    final Optional<BackupAuthCredentialRequest> mediaCredentialRequest = deserializeWithEmptyPresenceCheck(
        BackupAuthCredentialRequest::new,
        request.getMediaBackupAuthCredentialRequest());

    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();
    final Account account = authenticatedAccount();
    final Device device = account
        .getDevice(authenticatedDevice.deviceId())
        .orElseThrow(Status.UNAUTHENTICATED::asRuntimeException);
    backupAuthManager.commitBackupId(account, device, messagesCredentialRequest, mediaCredentialRequest);
    return SetBackupIdResponse.getDefaultInstance();
  }

  public RedeemReceiptResponse redeemReceipt(RedeemReceiptRequest request) {
    final ReceiptCredentialPresentation receiptCredentialPresentation = deserialize(
        ReceiptCredentialPresentation::new,
        request.getPresentation().toByteArray());
    final Account account = authenticatedAccount();
    backupAuthManager.redeemReceipt(account, receiptCredentialPresentation);
    return RedeemReceiptResponse.getDefaultInstance();
  }

  @Override
  public GetBackupAuthCredentialsResponse getBackupAuthCredentials(GetBackupAuthCredentialsRequest request) {
    final Tag platformTag = UserAgentTagUtil.getPlatformTag(RequestAttributesUtil.getUserAgent().orElse(null));
    final RedemptionRange redemptionRange;
    try {
      redemptionRange = RedemptionRange.inclusive(Clock.systemUTC(),
          Instant.ofEpochSecond(request.getRedemptionStart()),
          Instant.ofEpochSecond(request.getRedemptionStop()));
    } catch (IllegalArgumentException e) {
      throw Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException();
    }
    final Account account = authenticatedAccount();
    final List<BackupAuthManager.Credential> messageCredentials =
        backupAuthManager.getBackupAuthCredentials(
            account,
            BackupCredentialType.MESSAGES,
            redemptionRange);
    backupMetrics.updateGetCredentialCounter(platformTag, BackupCredentialType.MESSAGES, messageCredentials.size());

    final List<BackupAuthManager.Credential> mediaCredentials =
        backupAuthManager.getBackupAuthCredentials(
            account,
            BackupCredentialType.MEDIA,
            redemptionRange);
    backupMetrics.updateGetCredentialCounter(platformTag, BackupCredentialType.MEDIA, mediaCredentials.size());

    return GetBackupAuthCredentialsResponse.newBuilder()
        .putAllMessageCredentials(messageCredentials.stream().collect(Collectors.toMap(
            c -> c.redemptionTime().getEpochSecond(),
            c -> ZkCredential.newBuilder()
                .setCredential(ByteString.copyFrom(c.credential().serialize()))
                .setRedemptionTime(c.redemptionTime().getEpochSecond())
                .build())))
        .putAllMediaCredentials(mediaCredentials.stream().collect(Collectors.toMap(
            c -> c.redemptionTime().getEpochSecond(),
            c -> ZkCredential.newBuilder()
                .setCredential(ByteString.copyFrom(c.credential().serialize()))
                .setRedemptionTime(c.redemptionTime().getEpochSecond())
                .build())))
        .build();
  }

  private Account authenticatedAccount() {
    return accountManager
        .getByAccountIdentifier(AuthenticationUtil.requireAuthenticatedDevice().accountIdentifier())
        .orElseThrow(Status.UNAUTHENTICATED::asRuntimeException);
  }

  private interface Deserializer<T> {

    T deserialize(byte[] bytes) throws InvalidInputException;
  }

  private <T> Optional<T> deserializeWithEmptyPresenceCheck(Deserializer<T> deserializer, ByteString byteString) {
    if (byteString.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(deserialize(deserializer, byteString.toByteArray()));
  }

  private <T> T deserialize(Deserializer<T> deserializer, byte[] bytes) {
    try {
      return deserializer.deserialize(bytes);
    } catch (InvalidInputException e) {
      throw Status.INVALID_ARGUMENT.withDescription("Invalid serialization").asRuntimeException();
    }
  }

}
