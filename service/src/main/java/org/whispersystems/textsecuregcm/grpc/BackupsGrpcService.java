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
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import reactor.core.publisher.Mono;

public class BackupsGrpcService extends ReactorBackupsGrpc.BackupsImplBase {

  private final AccountsManager accountManager;
  private final BackupAuthManager backupAuthManager;

  public BackupsGrpcService(final AccountsManager accountManager, final BackupAuthManager backupAuthManager) {
    this.accountManager = accountManager;
    this.backupAuthManager = backupAuthManager;
  }


  @Override
  public Mono<SetBackupIdResponse> setBackupId(SetBackupIdRequest request) {

    final BackupAuthCredentialRequest messagesCredentialRequest = deserialize(
        BackupAuthCredentialRequest::new,
        request.getMessagesBackupAuthCredentialRequest().toByteArray());

    final BackupAuthCredentialRequest mediaCredentialRequest = deserialize(
        BackupAuthCredentialRequest::new,
        request.getMediaBackupAuthCredentialRequest().toByteArray());

    return authenticatedAccount()
        .flatMap(account -> Mono.fromFuture(
            backupAuthManager.commitBackupId(account, messagesCredentialRequest, mediaCredentialRequest)))
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
    return authenticatedAccount().flatMap(account -> {
      final Mono<List<BackupAuthManager.Credential>> messageCredentials = Mono.fromCompletionStage(() ->
          backupAuthManager.getBackupAuthCredentials(
              account,
              BackupCredentialType.MESSAGES,
              Instant.ofEpochSecond(request.getRedemptionStart()),
              Instant.ofEpochSecond(request.getRedemptionStop())));
      final Mono<List<BackupAuthManager.Credential>> mediaCredentials = Mono.fromCompletionStage(() ->
          backupAuthManager.getBackupAuthCredentials(
              account,
              BackupCredentialType.MEDIA,
              Instant.ofEpochSecond(request.getRedemptionStart()),
              Instant.ofEpochSecond(request.getRedemptionStop())));

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
