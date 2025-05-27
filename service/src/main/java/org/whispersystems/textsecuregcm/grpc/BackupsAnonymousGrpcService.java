/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import org.signal.chat.backup.CopyMediaRequest;
import org.signal.chat.backup.CopyMediaResponse;
import org.signal.chat.backup.DeleteAllRequest;
import org.signal.chat.backup.DeleteAllResponse;
import org.signal.chat.backup.DeleteMediaRequest;
import org.signal.chat.backup.DeleteMediaResponse;
import org.signal.chat.backup.GetBackupInfoRequest;
import org.signal.chat.backup.GetBackupInfoResponse;
import org.signal.chat.backup.GetCdnCredentialsRequest;
import org.signal.chat.backup.GetCdnCredentialsResponse;
import org.signal.chat.backup.GetUploadFormRequest;
import org.signal.chat.backup.GetUploadFormResponse;
import org.signal.chat.backup.ListMediaRequest;
import org.signal.chat.backup.ListMediaResponse;
import org.signal.chat.backup.ReactorBackupsAnonymousGrpc;
import org.signal.chat.backup.RefreshRequest;
import org.signal.chat.backup.RefreshResponse;
import org.signal.chat.backup.SetPublicKeyRequest;
import org.signal.chat.backup.SetPublicKeyResponse;
import org.signal.chat.backup.SignedPresentation;
import org.signal.libsignal.protocol.InvalidKeyException;
import org.signal.libsignal.protocol.ecc.ECPublicKey;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.backups.BackupAuthCredentialPresentation;
import org.whispersystems.textsecuregcm.auth.AuthenticatedBackupUser;
import org.whispersystems.textsecuregcm.backup.BackupManager;
import org.whispersystems.textsecuregcm.backup.CopyParameters;
import org.whispersystems.textsecuregcm.backup.MediaEncryptionParameters;
import org.whispersystems.textsecuregcm.controllers.ArchiveController;
import org.whispersystems.textsecuregcm.metrics.BackupMetrics;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

public class BackupsAnonymousGrpcService extends ReactorBackupsAnonymousGrpc.BackupsAnonymousImplBase {

  private final BackupManager backupManager;
  private final BackupMetrics backupMetrics;

  public BackupsAnonymousGrpcService(final BackupManager backupManager, final BackupMetrics backupMetrics) {
    this.backupManager = backupManager;
    this.backupMetrics = backupMetrics;
  }

  @Override
  public Mono<GetCdnCredentialsResponse> getCdnCredentials(final GetCdnCredentialsRequest request) {
    return authenticateBackupUserMono(request.getSignedPresentation())
        .map(user -> backupManager.generateReadAuth(user, request.getCdn()))
        .map(credentials -> GetCdnCredentialsResponse.newBuilder().putAllHeaders(credentials).build());
  }

  @Override
  public Mono<GetBackupInfoResponse> getBackupInfo(final GetBackupInfoRequest request) {
    return Mono.fromFuture(() ->
            authenticateBackupUser(request.getSignedPresentation()).thenCompose(backupManager::backupInfo))
        .map(info -> GetBackupInfoResponse.newBuilder()
            .setBackupName(info.messageBackupKey())
            .setCdn(info.cdn())
            .setBackupDir(info.backupSubdir())
            .setMediaDir(info.mediaSubdir())
            .setUsedSpace(info.mediaUsedSpace().orElse(0L))
            .build());
  }

  @Override
  public Mono<RefreshResponse> refresh(final RefreshRequest request) {
    return Mono.fromFuture(() -> authenticateBackupUser(request.getSignedPresentation())
            .thenCompose(backupManager::ttlRefresh))
        .thenReturn(RefreshResponse.getDefaultInstance());
  }

  @Override
  public Mono<SetPublicKeyResponse> setPublicKey(final SetPublicKeyRequest request) {
    final ECPublicKey publicKey = deserialize(ECPublicKey::new, request.getPublicKey().toByteArray());
    final BackupAuthCredentialPresentation presentation = deserialize(
        BackupAuthCredentialPresentation::new,
        request.getSignedPresentation().getPresentation().toByteArray());
    final byte[] signature = request.getSignedPresentation().getPresentationSignature().toByteArray();

    return Mono.fromFuture(() -> backupManager.setPublicKey(presentation, signature, publicKey))
        .thenReturn(SetPublicKeyResponse.getDefaultInstance());
  }


  @Override
  public Mono<GetUploadFormResponse> getUploadForm(final GetUploadFormRequest request) {
    return authenticateBackupUserMono(request.getSignedPresentation())
        .flatMap(backupUser -> switch (request.getUploadTypeCase()) {
          case MESSAGES -> Mono.fromFuture(backupManager.createMessageBackupUploadDescriptor(backupUser));
          case MEDIA -> Mono.fromCompletionStage(backupManager.createTemporaryAttachmentUploadDescriptor(backupUser));
          case UPLOADTYPE_NOT_SET -> Mono.error(Status.INVALID_ARGUMENT
              .withDescription("Must set upload_type")
              .asRuntimeException());
        })
        .map(uploadDescriptor -> GetUploadFormResponse.newBuilder()
            .setCdn(uploadDescriptor.cdn())
            .setKey(uploadDescriptor.key())
            .setSignedUploadLocation(uploadDescriptor.signedUploadLocation())
            .putAllHeaders(uploadDescriptor.headers())
            .build());
  }

  @Override
  public Flux<CopyMediaResponse> copyMedia(final CopyMediaRequest request) {
    return authenticateBackupUserMono(request.getSignedPresentation())
        .flatMapMany(backupUser -> backupManager.copyToBackup(backupUser,
            request.getItemsList().stream().map(item -> new CopyParameters(
                item.getSourceAttachmentCdn(), item.getSourceKey(),
                // uint32 in proto, make sure it fits in a signed int
                fromUnsignedExact(item.getObjectLength()),
                new MediaEncryptionParameters(item.getEncryptionKey().toByteArray(), item.getHmacKey().toByteArray()),
                item.getMediaId().toByteArray())).toList()))
        .doOnNext(result -> backupMetrics.updateCopyCounter(
            result,
            UserAgentTagUtil.getPlatformTag(RequestAttributesUtil.getUserAgent().orElse(null))))
        .map(copyResult -> {
          CopyMediaResponse.Builder builder = CopyMediaResponse
              .newBuilder()
              .setMediaId(ByteString.copyFrom(copyResult.mediaId()));
          builder = switch (copyResult.outcome()) {
            case SUCCESS -> builder
                .setSuccess(CopyMediaResponse.CopySuccess.newBuilder().setCdn(copyResult.cdn()).build());
            case OUT_OF_QUOTA -> builder
                .setOutOfSpace(CopyMediaResponse.OutOfSpace.getDefaultInstance());
            case SOURCE_WRONG_LENGTH -> builder
                .setWrongSourceLength(CopyMediaResponse.WrongSourceLength.getDefaultInstance());
            case SOURCE_NOT_FOUND -> builder
                .setSourceNotFound(CopyMediaResponse.SourceNotFound.getDefaultInstance());
          };
          return builder.build();
        });

  }

  @Override
  public Mono<ListMediaResponse> listMedia(final ListMediaRequest request) {
    return authenticateBackupUserMono(request.getSignedPresentation()).zipWhen(
        backupUser -> Mono.fromFuture(backupManager.list(
            backupUser,
            request.hasCursor() ? Optional.of(request.getCursor()) : Optional.empty(),
            request.getLimit()).toCompletableFuture()),

        (backupUser, listResult) -> {
          final ListMediaResponse.Builder builder = ListMediaResponse.newBuilder();
          for (BackupManager.StorageDescriptorWithLength sd : listResult.media()) {
            builder.addPage(ListMediaResponse.ListEntry.newBuilder()
                .setMediaId(ByteString.copyFrom(sd.key()))
                .setCdn(sd.cdn())
                .setLength(sd.length())
                .build());
          }
          builder
              .setBackupDir(backupUser.backupDir())
              .setMediaDir(backupUser.mediaDir());
          listResult.cursor().ifPresent(builder::setCursor);
          return builder.build();
        });

  }

  @Override
  public Mono<DeleteAllResponse> deleteAll(final DeleteAllRequest request) {
    return Mono.fromFuture(() -> authenticateBackupUser(request.getSignedPresentation())
            .thenCompose(backupManager::deleteEntireBackup))
        .thenReturn(DeleteAllResponse.getDefaultInstance());
  }

  @Override
  public Flux<DeleteMediaResponse> deleteMedia(final DeleteMediaRequest request) {
    return Mono
        .fromFuture(() -> authenticateBackupUser(request.getSignedPresentation()))
        .flatMapMany(backupUser -> backupManager.deleteMedia(backupUser, request
            .getItemsList()
            .stream()
            .map(item -> new BackupManager.StorageDescriptor(item.getCdn(), item.getMediaId().toByteArray()))
            .toList()))
        .map(storageDescriptor -> DeleteMediaResponse.newBuilder()
            .setMediaId(ByteString.copyFrom(storageDescriptor.key()))
            .setCdn(storageDescriptor.cdn()).build());
  }

  private Mono<AuthenticatedBackupUser> authenticateBackupUserMono(final SignedPresentation signedPresentation) {
    return Mono.fromFuture(() -> authenticateBackupUser(signedPresentation));
  }

  private CompletableFuture<AuthenticatedBackupUser> authenticateBackupUser(
      final SignedPresentation signedPresentation) {
    if (signedPresentation == null) {
      throw Status.UNAUTHENTICATED.asRuntimeException();
    }
    try {
      return backupManager.authenticateBackupUser(
          new BackupAuthCredentialPresentation(signedPresentation.getPresentation().toByteArray()),
          signedPresentation.getPresentationSignature().toByteArray(),
          RequestAttributesUtil.getUserAgent().orElse(null));
    } catch (InvalidInputException e) {
      throw Status.UNAUTHENTICATED.withDescription("Could not deserialize presentation").asRuntimeException();
    }
  }

  /**
   * Convert an int from a proto uint32 to a signed positive integer, throwing if the value exceeds
   * {@link Integer#MAX_VALUE}. To convert to a long, see {@link Integer#toUnsignedLong(int)}
   */
  private static int fromUnsignedExact(final int i) {
    if (i < 0) {
      throw Status.INVALID_ARGUMENT.withDescription("Invalid size").asRuntimeException();
    }
    return i;
  }

  private interface Deserializer<T> {

    T deserialize(byte[] bytes) throws InvalidInputException, InvalidKeyException;
  }

  private static <T> T deserialize(Deserializer<T> deserializer, byte[] bytes) {
    try {
      return deserializer.deserialize(bytes);
    } catch (InvalidInputException | InvalidKeyException e) {
      throw Status.INVALID_ARGUMENT.withDescription("Invalid serialization").asRuntimeException();
    }
  }

}
