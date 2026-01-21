/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
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
import org.signal.chat.backup.GetSvrBCredentialsRequest;
import org.signal.chat.backup.GetSvrBCredentialsResponse;
import org.signal.chat.backup.GetUploadFormRequest;
import org.signal.chat.backup.GetUploadFormResponse;
import org.signal.chat.backup.ListMediaRequest;
import org.signal.chat.backup.ListMediaResponse;
import org.signal.chat.backup.RefreshRequest;
import org.signal.chat.backup.RefreshResponse;
import org.signal.chat.backup.SetPublicKeyRequest;
import org.signal.chat.backup.SetPublicKeyResponse;
import org.signal.chat.backup.SignedPresentation;
import org.signal.chat.backup.SimpleBackupsAnonymousGrpc;
import org.signal.libsignal.protocol.InvalidKeyException;
import org.signal.libsignal.protocol.ecc.ECPublicKey;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.backups.BackupAuthCredentialPresentation;
import org.whispersystems.textsecuregcm.auth.AuthenticatedBackupUser;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.backup.BackupManager;
import org.whispersystems.textsecuregcm.backup.BackupUploadDescriptor;
import org.whispersystems.textsecuregcm.backup.CopyParameters;
import org.whispersystems.textsecuregcm.backup.MediaEncryptionParameters;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.metrics.BackupMetrics;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import reactor.adapter.JdkFlowAdapter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class BackupsAnonymousGrpcService extends SimpleBackupsAnonymousGrpc.BackupsAnonymousImplBase {

  private final BackupManager backupManager;
  private final BackupMetrics backupMetrics;

  public BackupsAnonymousGrpcService(final BackupManager backupManager, final BackupMetrics backupMetrics) {
    this.backupManager = backupManager;
    this.backupMetrics = backupMetrics;
  }

  @Override
  public GetCdnCredentialsResponse getCdnCredentials(final GetCdnCredentialsRequest request) {
    final AuthenticatedBackupUser backupUser = authenticateBackupUser(request.getSignedPresentation());
    return GetCdnCredentialsResponse.newBuilder()
        .putAllHeaders(backupManager.generateReadAuth(backupUser, request.getCdn()))
        .build();
  }

  @Override
  public GetSvrBCredentialsResponse getSvrBCredentials(final GetSvrBCredentialsRequest request) {
    final AuthenticatedBackupUser backupUser = authenticateBackupUser(request.getSignedPresentation());
    final ExternalServiceCredentials credentials = backupManager.generateSvrbAuth(backupUser);
    return GetSvrBCredentialsResponse.newBuilder()
        .setUsername(credentials.username())
        .setPassword(credentials.password())
        .build();
  }

  @Override
  public GetBackupInfoResponse getBackupInfo(final GetBackupInfoRequest request) {
    final AuthenticatedBackupUser backupUser = authenticateBackupUser(request.getSignedPresentation());
    final BackupManager.BackupInfo info = backupManager.backupInfo(backupUser);
    return GetBackupInfoResponse.newBuilder()
        .setBackupName(info.messageBackupKey())
        .setCdn(info.cdn())
        .setBackupDir(info.backupSubdir())
        .setMediaDir(info.mediaSubdir())
        .setUsedSpace(info.mediaUsedSpace().orElse(0L))
        .build();
  }

  @Override
  public RefreshResponse refresh(final RefreshRequest request) {
    final AuthenticatedBackupUser backupUser = authenticateBackupUser(request.getSignedPresentation());
    backupManager.ttlRefresh(backupUser);
    return RefreshResponse.getDefaultInstance();
  }

  @Override
  public SetPublicKeyResponse setPublicKey(final SetPublicKeyRequest request) {
    final ECPublicKey publicKey = deserialize(ECPublicKey::new, request.getPublicKey().toByteArray());
    final BackupAuthCredentialPresentation presentation = deserialize(
        BackupAuthCredentialPresentation::new,
        request.getSignedPresentation().getPresentation().toByteArray());
    final byte[] signature = request.getSignedPresentation().getPresentationSignature().toByteArray();

    backupManager.setPublicKey(presentation, signature, publicKey);
    return SetPublicKeyResponse.getDefaultInstance();
  }


  @Override
  public GetUploadFormResponse getUploadForm(final GetUploadFormRequest request) throws RateLimitExceededException {
    final AuthenticatedBackupUser backupUser = authenticateBackupUser(request.getSignedPresentation());
    final BackupUploadDescriptor uploadDescriptor = switch (request.getUploadTypeCase()) {
      case MESSAGES -> {
        final long uploadLength = request.getMessages().getUploadLength();
        final boolean oversize = uploadLength > BackupManager.MAX_MESSAGE_BACKUP_OBJECT_SIZE;
        backupMetrics.updateMessageBackupSizeDistribution(backupUser, oversize, Optional.of(uploadLength));
        if (oversize) {
          throw Status.FAILED_PRECONDITION
              .withDescription("Exceeds max upload length")
              .asRuntimeException();
        }

        yield backupManager.createMessageBackupUploadDescriptor(backupUser);
      }
      case MEDIA -> backupManager.createTemporaryAttachmentUploadDescriptor(backupUser);
      case UPLOADTYPE_NOT_SET -> throw Status.INVALID_ARGUMENT
          .withDescription("Must set upload_type")
          .asRuntimeException();
    };
    return GetUploadFormResponse.newBuilder()
        .setCdn(uploadDescriptor.cdn())
        .setKey(uploadDescriptor.key())
        .setSignedUploadLocation(uploadDescriptor.signedUploadLocation())
        .putAllHeaders(uploadDescriptor.headers())
        .build();
  }

  @Override
  public Flow.Publisher<CopyMediaResponse> copyMedia(final CopyMediaRequest request) {
    final Flux<CopyMediaResponse> flux = Mono
        .fromFuture(() -> authenticateBackupUserAsync(request.getSignedPresentation()))
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
    return JdkFlowAdapter.publisherToFlowPublisher(flux);
  }

  @Override
  public ListMediaResponse listMedia(final ListMediaRequest request) {
    final AuthenticatedBackupUser backupUser = authenticateBackupUser(request.getSignedPresentation());
    final BackupManager.ListMediaResult listResult = backupManager.list(
        backupUser,
        request.hasCursor() ? Optional.of(request.getCursor()) : Optional.empty(),
        request.getLimit());

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
  }

  @Override
  public DeleteAllResponse deleteAll(final DeleteAllRequest request) {
    final AuthenticatedBackupUser backupUser = authenticateBackupUser(request.getSignedPresentation());
    backupManager.deleteEntireBackup(backupUser);
    return DeleteAllResponse.getDefaultInstance();
  }

  @Override
  public Flow.Publisher<DeleteMediaResponse> deleteMedia(final DeleteMediaRequest request) {
    return JdkFlowAdapter.publisherToFlowPublisher(Mono
        .fromFuture(() -> authenticateBackupUserAsync(request.getSignedPresentation()))
        .flatMapMany(backupUser -> backupManager.deleteMedia(backupUser, request
            .getItemsList()
            .stream()
            .map(item -> new BackupManager.StorageDescriptor(item.getCdn(), item.getMediaId().toByteArray()))
            .toList()))
        .map(storageDescriptor -> DeleteMediaResponse.newBuilder()
            .setMediaId(ByteString.copyFrom(storageDescriptor.key()))
            .setCdn(storageDescriptor.cdn()).build()));
  }

  private CompletableFuture<AuthenticatedBackupUser> authenticateBackupUserAsync(final SignedPresentation signedPresentation) {
    if (signedPresentation == null) {
      throw Status.UNAUTHENTICATED.asRuntimeException();
    }
    try {
      return backupManager.authenticateBackupUserAsync(
          new BackupAuthCredentialPresentation(signedPresentation.getPresentation().toByteArray()),
          signedPresentation.getPresentationSignature().toByteArray(),
          RequestAttributesUtil.getUserAgent().orElse(null));
    } catch (InvalidInputException e) {
      throw Status.UNAUTHENTICATED.withDescription("Could not deserialize presentation").asRuntimeException();
    }
  }

  private AuthenticatedBackupUser authenticateBackupUser(final SignedPresentation signedPresentation) {
    return authenticateBackupUserAsync(signedPresentation).join();
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
