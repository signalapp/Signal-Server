/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc;

import com.google.protobuf.ByteString;
import java.util.Optional;
import java.util.concurrent.Flow;
import org.signal.chat.backup.CopyMediaRequest;
import org.signal.chat.backup.CopyMediaResponse;
import org.signal.chat.backup.DeleteAllRequest;
import org.signal.chat.backup.DeleteAllResponse;
import org.signal.chat.backup.DeleteMediaItem;
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
import org.signal.chat.errors.FailedPrecondition;
import org.signal.chat.errors.FailedZkAuthentication;
import org.signal.libsignal.protocol.InvalidKeyException;
import org.signal.libsignal.protocol.ecc.ECPublicKey;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.backups.BackupAuthCredentialPresentation;
import org.whispersystems.textsecuregcm.auth.AuthenticatedBackupUser;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.backup.BackupFailedZkAuthenticationException;
import org.whispersystems.textsecuregcm.backup.BackupInvalidArgumentException;
import org.whispersystems.textsecuregcm.backup.BackupManager;
import org.whispersystems.textsecuregcm.backup.BackupNotFoundException;
import org.whispersystems.textsecuregcm.backup.BackupPermissionException;
import org.whispersystems.textsecuregcm.backup.BackupUploadDescriptor;
import org.whispersystems.textsecuregcm.backup.BackupWrongCredentialTypeException;
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
  public GetCdnCredentialsResponse getCdnCredentials(final GetCdnCredentialsRequest request)
      throws BackupInvalidArgumentException, BackupPermissionException {
    try {
      final AuthenticatedBackupUser backupUser = authenticateBackupUser(request.getSignedPresentation());
      return GetCdnCredentialsResponse.newBuilder()
          .setCdnCredentials(GetCdnCredentialsResponse.CdnCredentials.newBuilder()
              .putAllHeaders(backupManager.generateReadAuth(backupUser, request.getCdn())))
          .build();
    } catch (BackupFailedZkAuthenticationException e) {
      return GetCdnCredentialsResponse.newBuilder()
          .setFailedAuthentication(FailedZkAuthentication.newBuilder().setDescription(e.getMessage()).build())
          .build();
    }
  }

  @Override
  public GetSvrBCredentialsResponse getSvrBCredentials(final GetSvrBCredentialsRequest request)
      throws BackupWrongCredentialTypeException, BackupPermissionException {
    try {
      final AuthenticatedBackupUser backupUser = authenticateBackupUser(request.getSignedPresentation());
      final ExternalServiceCredentials credentials = backupManager.generateSvrbAuth(backupUser);
      return GetSvrBCredentialsResponse.newBuilder()
          .setSvrbCredentials(GetSvrBCredentialsResponse.SvrBCredentials.newBuilder()
              .setUsername(credentials.username())
              .setPassword(credentials.password()))
          .build();
    } catch (BackupFailedZkAuthenticationException e) {
      return GetSvrBCredentialsResponse.newBuilder()
          .setFailedAuthentication(FailedZkAuthentication.newBuilder().setDescription(e.getMessage()).build())
          .build();
    }
  }

  @Override
  public GetBackupInfoResponse getBackupInfo(final GetBackupInfoRequest request)
      throws BackupNotFoundException, BackupPermissionException {
    try {
      final AuthenticatedBackupUser backupUser = authenticateBackupUser(request.getSignedPresentation());
      final BackupManager.BackupInfo info = backupManager.backupInfo(backupUser);
      return GetBackupInfoResponse.newBuilder().setBackupInfo(GetBackupInfoResponse.BackupInfo.newBuilder()
              .setBackupName(info.messageBackupKey())
              .setCdn(info.cdn())
              .setBackupDir(info.backupSubdir())
              .setMediaDir(info.mediaSubdir())
              .setUsedSpace(info.mediaUsedSpace().orElse(0L)))
          .build();
    } catch (BackupFailedZkAuthenticationException e) {
      return GetBackupInfoResponse.newBuilder()
          .setFailedAuthentication(FailedZkAuthentication.newBuilder().setDescription(e.getMessage()).build())
          .build();
    }
  }

  @Override
  public RefreshResponse refresh(final RefreshRequest request) throws BackupPermissionException {
    try {
      final AuthenticatedBackupUser backupUser = authenticateBackupUser(request.getSignedPresentation());
      backupManager.ttlRefresh(backupUser);
      return RefreshResponse.getDefaultInstance();
    } catch (BackupFailedZkAuthenticationException e) {
      return RefreshResponse.newBuilder()
          .setFailedAuthentication(FailedZkAuthentication.newBuilder().setDescription(e.getMessage()).build())
          .build();
    }
  }

  @Override
  public SetPublicKeyResponse setPublicKey(final SetPublicKeyRequest request)
      throws BackupFailedZkAuthenticationException {
    final ECPublicKey publicKey = deserialize(ECPublicKey::new, request.getPublicKey().toByteArray());
    final BackupAuthCredentialPresentation presentation = deserialize(
        BackupAuthCredentialPresentation::new,
        request.getSignedPresentation().getPresentation().toByteArray());
    final byte[] signature = request.getSignedPresentation().getPresentationSignature().toByteArray();

    backupManager.setPublicKey(presentation, signature, publicKey);
    return SetPublicKeyResponse.getDefaultInstance();
  }


  @Override
  public GetUploadFormResponse getUploadForm(final GetUploadFormRequest request)
      throws RateLimitExceededException, BackupWrongCredentialTypeException, BackupPermissionException {
    final AuthenticatedBackupUser backupUser;
    try {
      backupUser = authenticateBackupUser(request.getSignedPresentation());
    } catch (BackupFailedZkAuthenticationException e) {
      return GetUploadFormResponse.newBuilder()
          .setFailedAuthentication(FailedZkAuthentication.newBuilder().setDescription(e.getMessage()).build())
          .build();
    }
    final GetUploadFormResponse.Builder builder = GetUploadFormResponse.newBuilder();
    switch (request.getUploadTypeCase()) {
      case MESSAGES -> {
        final long uploadLength = request.getMessages().getUploadLength();
        final boolean oversize = uploadLength > BackupManager.MAX_MESSAGE_BACKUP_OBJECT_SIZE;
        backupMetrics.updateMessageBackupSizeDistribution(backupUser, oversize, Optional.of(uploadLength));
        if (oversize) {
          builder.setExceedsMaxUploadLength(FailedPrecondition.getDefaultInstance());
        } else {
          final BackupUploadDescriptor uploadDescriptor = backupManager.createMessageBackupUploadDescriptor(backupUser);
          builder.setUploadForm(builder.getUploadFormBuilder()
              .setCdn(uploadDescriptor.cdn())
              .setKey(uploadDescriptor.key())
              .setSignedUploadLocation(uploadDescriptor.signedUploadLocation())
              .putAllHeaders(uploadDescriptor.headers())).build();
        }
      }
      case MEDIA -> {
        final BackupUploadDescriptor uploadDescriptor = backupManager.createTemporaryAttachmentUploadDescriptor(
            backupUser);
        builder.setUploadForm(builder.getUploadFormBuilder()
            .setCdn(uploadDescriptor.cdn())
            .setKey(uploadDescriptor.key())
            .setSignedUploadLocation(uploadDescriptor.signedUploadLocation())
            .putAllHeaders(uploadDescriptor.headers())).build();
      }
      case UPLOADTYPE_NOT_SET -> throw GrpcExceptions.fieldViolation("upload_type", "Must set upload_type");
    }
    return builder.build();
  }

  @Override
  public Flow.Publisher<CopyMediaResponse> copyMedia(final CopyMediaRequest request)
      throws BackupWrongCredentialTypeException, BackupPermissionException, BackupInvalidArgumentException {
    final BackupManager.CopyQuota copyQuota;
    try {
      final AuthenticatedBackupUser backupUser = authenticateBackupUser(request.getSignedPresentation());
      copyQuota = backupManager.getCopyQuota(backupUser,
          request.getItemsList().stream().map(item -> new CopyParameters(
              item.getSourceAttachmentCdn(), item.getSourceKey(),
              // uint32 in proto, make sure it fits in a signed int
              fromUnsignedExact(item.getObjectLength()),
              new MediaEncryptionParameters(item.getEncryptionKey().toByteArray(), item.getHmacKey().toByteArray()),
              item.getMediaId().toByteArray())).toList());
    } catch (BackupFailedZkAuthenticationException e) {
      return JdkFlowAdapter.publisherToFlowPublisher(Mono.just(CopyMediaResponse
          .newBuilder()
          .setFailedAuthentication(FailedZkAuthentication.newBuilder().setDescription(e.getMessage()))
          .build()));
    }
    return JdkFlowAdapter.publisherToFlowPublisher(backupManager.copyToBackup(copyQuota)
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
        }));
  }

  @Override
  public ListMediaResponse listMedia(final ListMediaRequest request) throws BackupPermissionException {
    try {
      final AuthenticatedBackupUser backupUser = authenticateBackupUser(request.getSignedPresentation());
      final BackupManager.ListMediaResult listResult = backupManager.list(
          backupUser,
          request.hasCursor() ? Optional.of(request.getCursor()) : Optional.empty(),
          request.getLimit());
      final ListMediaResponse.ListResult.Builder builder = ListMediaResponse.ListResult.newBuilder();
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
      return ListMediaResponse.newBuilder().setListResult(builder).build();
    } catch (BackupFailedZkAuthenticationException e) {
      return ListMediaResponse.newBuilder()
          .setFailedAuthentication(FailedZkAuthentication.newBuilder().setDescription(e.getMessage()))
          .build();
    }
  }

  @Override
  public DeleteAllResponse deleteAll(final DeleteAllRequest request) throws BackupPermissionException {
    try {
      final AuthenticatedBackupUser backupUser = authenticateBackupUser(request.getSignedPresentation());
      backupManager.deleteEntireBackup(backupUser);
      return DeleteAllResponse.getDefaultInstance();
    } catch (BackupFailedZkAuthenticationException e) {
      return DeleteAllResponse.newBuilder()
          .setFailedAuthentication(FailedZkAuthentication.newBuilder().setDescription(e.getMessage()))
          .build();
    }
  }

  @Override
  public Flow.Publisher<DeleteMediaResponse> deleteMedia(final DeleteMediaRequest request)
      throws BackupWrongCredentialTypeException, BackupPermissionException {
    final Flux<BackupManager.StorageDescriptor> deleteItems;
    try {
      final AuthenticatedBackupUser backupUser = authenticateBackupUser(request.getSignedPresentation());
      deleteItems = backupManager.deleteMedia(backupUser, request
          .getItemsList()
          .stream()
          .map(item -> new BackupManager.StorageDescriptor(item.getCdn(), item.getMediaId().toByteArray()))
          .toList());
    } catch (BackupFailedZkAuthenticationException e) {
      return JdkFlowAdapter.publisherToFlowPublisher(Mono.just(DeleteMediaResponse
          .newBuilder()
          .setFailedAuthentication(FailedZkAuthentication.newBuilder().setDescription(e.getMessage()))
          .build()));
    }
    return JdkFlowAdapter.publisherToFlowPublisher(deleteItems
        .map(storageDescriptor -> DeleteMediaResponse.newBuilder()
            .setDeletedItem(DeleteMediaItem.newBuilder()
                .setMediaId(ByteString.copyFrom(storageDescriptor.key()))
                .setCdn(storageDescriptor.cdn()))
            .build()));
  }

  @Override
  public Throwable mapException(final Throwable throwable) {
    return switch (throwable) {
      case BackupInvalidArgumentException e -> GrpcExceptions.invalidArguments(e.getMessage());
      case BackupPermissionException e -> GrpcExceptions.badAuthentication(e.getMessage());
      case BackupWrongCredentialTypeException e -> GrpcExceptions.badAuthentication(e.getMessage());
      default -> throwable;
    };
  }

  private AuthenticatedBackupUser authenticateBackupUser(final SignedPresentation signedPresentation)
      throws BackupFailedZkAuthenticationException {
    if (signedPresentation == null) {
      throw GrpcExceptions.badAuthentication("Missing required signedPresentation");
    }
    try {
      return backupManager.authenticateBackupUser(
          new BackupAuthCredentialPresentation(signedPresentation.getPresentation().toByteArray()),
          signedPresentation.getPresentationSignature().toByteArray(),
          RequestAttributesUtil.getUserAgent().orElse(null));
    } catch (InvalidInputException e) {
      throw GrpcExceptions.badAuthentication("Could not deserialize presentation");
    }
  }

  /**
   * Convert an int from a proto uint32 to a signed positive integer, throwing if the value exceeds
   * {@link Integer#MAX_VALUE}. To convert to a long, see {@link Integer#toUnsignedLong(int)}
   */
  private static int fromUnsignedExact(final int i) {
    if (i < 0) {
      throw GrpcExceptions.invalidArguments("integer length too large");
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
      throw GrpcExceptions.invalidArguments("invalid serialization");
    }
  }

}
