/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import org.signal.chat.backup.GetBackupAuthCredentialsRequest;
import org.signal.chat.backup.GetBackupAuthCredentialsResponse;
import org.signal.chat.backup.ReactorBackupsGrpc;
import org.signal.chat.backup.RedeemReceiptRequest;
import org.signal.chat.backup.RedeemReceiptResponse;
import org.signal.chat.backup.SetBackupIdRequest;
import org.signal.chat.backup.SetBackupIdResponse;
import org.signal.chat.common.ZkCredential;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.backups.BackupAuthCredentialRequest;
import org.signal.libsignal.zkgroup.backups.BackupCredentialType;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialPresentation;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticationUtil;
import org.whispersystems.textsecuregcm.backup.BackupAuthManager;
import org.whispersystems.textsecuregcm.controllers.ArchiveController;
import org.whispersystems.textsecuregcm.metrics.BackupMetrics;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import reactor.core.publisher.Mono;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

public class BackupsGrpcService extends ReactorBackupsGrpc.BackupsImplBase {

  private final AccountsManager accountManager;
  private final BackupAuthManager backupAuthManager;
  private final BackupMetrics backupMetrics;

  public BackupsGrpcService(final AccountsManager accountManager, final BackupAuthManager backupAuthManager, final BackupMetrics backupMetrics) {
    this.accountManager = accountManager;
    this.backupAuthManager = backupAuthManager;
    this.backupMetrics = backupMetrics;
  }


  @Override
  public Mono<SetBackupIdResponse> setBackupId(SetBackupIdRequest request) {

    final BackupAuthCredentialRequest messagesCredentialRequest = deserialize(
        BackupAuthCredentialRequest::new,
        request.getMessagesBackupAuthCredentialRequest().toByteArray());

    final BackupAuthCredentialRequest mediaCredentialRequest = deserialize(
        BackupAuthCredentialRequest::new,
        request.getMediaBackupAuthCredentialRequest().toByteArray());

    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();
    return authenticatedAccount()
        .flatMap(account -> {
          final Device device = account
              .getDevice(authenticatedDevice.deviceId())
              .orElseThrow(Status.UNAUTHENTICATED::asRuntimeException);
          return Mono.fromFuture(
              backupAuthManager.commitBackupId(account, device, messagesCredentialRequest, mediaCredentialRequest));
        })
        .thenReturn(SetBackupIdResponse.getDefaultInstance());
  }

  public Mono<RedeemReceiptResponse> redeemReceipt(RedeemReceiptRequest request) {
    final ReceiptCredentialPresentation receiptCredentialPresentation = deserialize(
        ReceiptCredentialPresentation::new,
        request.getPresentation().toByteArray());
    return authenticatedAccount()
        .flatMap(account -> Mono.fromFuture(backupAuthManager.redeemReceipt(account, receiptCredentialPresentation)))
        .thenReturn(RedeemReceiptResponse.getDefaultInstance());
  }

  @Override
  public Mono<GetBackupAuthCredentialsResponse> getBackupAuthCredentials(GetBackupAuthCredentialsRequest request) {
    final Tag platformTag = UserAgentTagUtil.getPlatformTag(RequestAttributesUtil.getUserAgent().orElse(null));
    return authenticatedAccount().flatMap(account -> {

      final Mono<List<BackupAuthManager.Credential>> messageCredentials = Mono.fromCompletionStage(() ->
          backupAuthManager.getBackupAuthCredentials(
              account,
              BackupCredentialType.MESSAGES,
              Instant.ofEpochSecond(request.getRedemptionStart()),
              Instant.ofEpochSecond(request.getRedemptionStop())))
          .doOnSuccess(credentials ->
              backupMetrics.updateGetCredentialCounter(platformTag, BackupCredentialType.MESSAGES, credentials.size()));

      final Mono<List<BackupAuthManager.Credential>> mediaCredentials = Mono.fromCompletionStage(() ->
          backupAuthManager.getBackupAuthCredentials(
              account,
              BackupCredentialType.MEDIA,
              Instant.ofEpochSecond(request.getRedemptionStart()),
              Instant.ofEpochSecond(request.getRedemptionStop())))
          .doOnSuccess(credentials ->
              backupMetrics.updateGetCredentialCounter(platformTag, BackupCredentialType.MEDIA, credentials.size()));

      return messageCredentials.zipWith(mediaCredentials, (messageCreds, mediaCreds) ->
          GetBackupAuthCredentialsResponse.newBuilder()
              .putAllMessageCredentials(messageCreds.stream().collect(Collectors.toMap(
                  c -> c.redemptionTime().getEpochSecond(),
                  c -> ZkCredential.newBuilder()
                      .setCredential(ByteString.copyFrom(c.credential().serialize()))
                      .setRedemptionTime(c.redemptionTime().getEpochSecond())
                      .build())))
              .putAllMediaCredentials(mediaCreds.stream().collect(Collectors.toMap(
                  c -> c.redemptionTime().getEpochSecond(),
                  c -> ZkCredential.newBuilder()
                      .setCredential(ByteString.copyFrom(c.credential().serialize()))
                      .setRedemptionTime(c.redemptionTime().getEpochSecond())
                      .build())))
              .build());
    });
  }

  private Mono<Account> authenticatedAccount() {
    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();
    return Mono
        .fromFuture(() -> accountManager.getByAccountIdentifierAsync(authenticatedDevice.accountIdentifier()))
        .map(maybeAccount -> maybeAccount.orElseThrow(Status.UNAUTHENTICATED::asRuntimeException));
  }

  private interface Deserializer<T> {

    T deserialize(byte[] bytes) throws InvalidInputException;
  }

  private <T> T deserialize(Deserializer<T> deserializer, byte[] bytes) {
    try {
      return deserializer.deserialize(bytes);
    } catch (InvalidInputException e) {
      throw Status.INVALID_ARGUMENT.withDescription("Invalid serialization").asRuntimeException();
    }
  }

}
