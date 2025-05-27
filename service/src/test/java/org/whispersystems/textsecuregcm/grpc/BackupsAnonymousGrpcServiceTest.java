/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.time.Clock;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.mockito.Mock;
import org.signal.chat.backup.BackupsAnonymousGrpc;
import org.signal.chat.backup.CopyMediaItem;
import org.signal.chat.backup.CopyMediaRequest;
import org.signal.chat.backup.CopyMediaResponse;
import org.signal.chat.backup.DeleteMediaItem;
import org.signal.chat.backup.DeleteMediaRequest;
import org.signal.chat.backup.GetBackupInfoRequest;
import org.signal.chat.backup.GetBackupInfoResponse;
import org.signal.chat.backup.GetCdnCredentialsRequest;
import org.signal.chat.backup.GetCdnCredentialsResponse;
import org.signal.chat.backup.GetUploadFormRequest;
import org.signal.chat.backup.GetUploadFormResponse;
import org.signal.chat.backup.ListMediaRequest;
import org.signal.chat.backup.ListMediaResponse;
import org.signal.chat.backup.SetPublicKeyRequest;
import org.signal.chat.backup.SignedPresentation;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.backups.BackupAuthCredentialPresentation;
import org.signal.libsignal.zkgroup.backups.BackupCredentialType;
import org.signal.libsignal.zkgroup.backups.BackupLevel;
import org.whispersystems.textsecuregcm.auth.AuthenticatedBackupUser;
import org.whispersystems.textsecuregcm.backup.BackupAuthTestUtil;
import org.whispersystems.textsecuregcm.backup.BackupManager;
import org.whispersystems.textsecuregcm.backup.BackupUploadDescriptor;
import org.whispersystems.textsecuregcm.backup.CopyResult;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.metrics.BackupMetrics;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import reactor.core.publisher.Flux;

class BackupsAnonymousGrpcServiceTest extends
    SimpleBaseGrpcTest<BackupsAnonymousGrpcService, BackupsAnonymousGrpc.BackupsAnonymousBlockingStub> {

  private final UUID aci = UUID.randomUUID();
  private final byte[] messagesBackupKey = TestRandomUtil.nextBytes(32);
  private final BackupAuthTestUtil backupAuthTestUtil = new BackupAuthTestUtil(Clock.systemUTC());
  private final BackupAuthCredentialPresentation presentation =
      presentation(backupAuthTestUtil, messagesBackupKey, aci);

  @Mock
  private BackupManager backupManager;

  @Override
  protected BackupsAnonymousGrpcService createServiceBeforeEachTest() {
    return new BackupsAnonymousGrpcService(backupManager, new BackupMetrics());
  }

  @BeforeEach
  void setup() {
    when(backupManager.authenticateBackupUser(any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(
            backupUser(presentation.getBackupId(), BackupCredentialType.MESSAGES, BackupLevel.PAID)));
  }

  @Test
  void setPublicKey() {
    when(backupManager.setPublicKey(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(null));
    assertThatNoException().isThrownBy(() -> unauthenticatedServiceStub().setPublicKey(SetPublicKeyRequest.newBuilder()
        .setPublicKey(ByteString.copyFrom(Curve.generateKeyPair().getPublicKey().serialize()))
        .setSignedPresentation(signedPresentation(presentation))
        .build()));
  }

  @Test
  void setBadPublicKey() {
    when(backupManager.setPublicKey(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(null));
    assertThatExceptionOfType(StatusRuntimeException.class).isThrownBy(() ->
            unauthenticatedServiceStub().setPublicKey(SetPublicKeyRequest.newBuilder()
                .setPublicKey(ByteString.copyFromUtf8("aaaaa")) // Invalid public key
                .setSignedPresentation(signedPresentation(presentation))
                .build()))
        .extracting(ex -> ex.getStatus().getCode())
        .isEqualTo(Status.Code.INVALID_ARGUMENT);
  }

  @Test
  void setMissingPublicKey() {
    assertThatExceptionOfType(StatusRuntimeException.class).isThrownBy(() ->
            unauthenticatedServiceStub().setPublicKey(SetPublicKeyRequest.newBuilder()
                // Missing public key
                .setSignedPresentation(signedPresentation(presentation))
                .build()))
        .extracting(ex -> ex.getStatus().getCode())
        .isEqualTo(Status.Code.INVALID_ARGUMENT);
  }


  @Test
  void putMediaBatchSuccess() {
    final byte[][] mediaIds = {TestRandomUtil.nextBytes(15), TestRandomUtil.nextBytes(15)};
    when(backupManager.copyToBackup(any(), any()))
        .thenReturn(Flux.just(
            new CopyResult(CopyResult.Outcome.SUCCESS, mediaIds[0], 1),
            new CopyResult(CopyResult.Outcome.SUCCESS, mediaIds[1], 1)));

    final CopyMediaRequest request = CopyMediaRequest.newBuilder()
        .setSignedPresentation(signedPresentation(presentation))
        .addItems(CopyMediaItem.newBuilder()
            .setSourceAttachmentCdn(3)
            .setSourceKey("abc")
            .setObjectLength(100)
            .setMediaId(ByteString.copyFrom(mediaIds[0]))
            .setHmacKey(ByteString.copyFrom(TestRandomUtil.nextBytes(32)))
            .setEncryptionKey(ByteString.copyFrom(TestRandomUtil.nextBytes(32)))
            .build())
        .addItems(CopyMediaItem.newBuilder()
            .setSourceAttachmentCdn(3)
            .setSourceKey("def")
            .setObjectLength(200)
            .setMediaId(ByteString.copyFrom(mediaIds[1]))
            .setHmacKey(ByteString.copyFrom(TestRandomUtil.nextBytes(32)))
            .setEncryptionKey(ByteString.copyFrom(TestRandomUtil.nextBytes(32)))
            .build())
        .build();

    final Iterator<CopyMediaResponse> it = unauthenticatedServiceStub().copyMedia(request);

    for (int i = 0; i < 2; i++) {
      final CopyMediaResponse response = it.next();
      assertThat(response.getSuccess().getCdn()).isEqualTo(1);
      assertThat(response.getMediaId().toByteArray()).isEqualTo(mediaIds[i]);
    }
    assertThat(it.hasNext()).isFalse();
  }

  @Test
  void putMediaBatchPartialFailure() {
    // Copy four different mediaIds, with a variety of success/failure outcomes
    final byte[][] mediaIds = IntStream.range(0, 4).mapToObj(i -> TestRandomUtil.nextBytes(15)).toArray(byte[][]::new);
    final CopyResult.Outcome[] outcomes = new CopyResult.Outcome[]{
        CopyResult.Outcome.SUCCESS,
        CopyResult.Outcome.SOURCE_NOT_FOUND,
        CopyResult.Outcome.SOURCE_WRONG_LENGTH,
        CopyResult.Outcome.OUT_OF_QUOTA
    };
    when(backupManager.copyToBackup(any(), any()))
        .thenReturn(Flux.fromStream(IntStream.range(0, 4)
            .mapToObj(i -> new CopyResult(
                outcomes[i],
                mediaIds[i],
                outcomes[i] == CopyResult.Outcome.SUCCESS ? 1 : null))));

    final CopyMediaRequest request = CopyMediaRequest.newBuilder()
        .setSignedPresentation(signedPresentation(presentation))
        .addAllItems(Arrays.stream(mediaIds)
            .map(mediaId -> CopyMediaItem.newBuilder()
                .setSourceAttachmentCdn(3)
                .setSourceKey("abc")
                .setObjectLength(100)
                .setMediaId(ByteString.copyFrom(mediaId))
                .setHmacKey(ByteString.copyFrom(TestRandomUtil.nextBytes(32)))
                .setEncryptionKey(ByteString.copyFrom(TestRandomUtil.nextBytes(32)))
                .build())
            .collect(Collectors.toList()))
        .build();

    final Iterator<CopyMediaResponse> responses = unauthenticatedServiceStub().copyMedia(request);

    // Verify that we get the expected response for each mediaId
    for (int i = 0; i < mediaIds.length; i++) {
      final CopyMediaResponse response = responses.next();
      switch (outcomes[i]) {
        case SUCCESS -> assertThat(response.getSuccess().getCdn()).isEqualTo(1);
        case SOURCE_WRONG_LENGTH -> assertThat(response.getWrongSourceLength()).isNotNull();
        case OUT_OF_QUOTA -> assertThat(response.getOutOfSpace()).isNotNull();
        case SOURCE_NOT_FOUND -> assertThat(response.getSourceNotFound()).isNotNull();
      }
      assertThat(response.getMediaId().toByteArray()).isEqualTo(mediaIds[i]);
    }
  }

  @Test
  void getBackupInfo() {
    when(backupManager.backupInfo(any())).thenReturn(CompletableFuture.completedFuture(new BackupManager.BackupInfo(
        1, "myBackupDir", "myMediaDir", "filename", Optional.empty())));

    final GetBackupInfoResponse response = unauthenticatedServiceStub().getBackupInfo(GetBackupInfoRequest.newBuilder()
        .setSignedPresentation(signedPresentation(presentation))
        .build());
    assertThat(response.getBackupDir()).isEqualTo("myBackupDir");
    assertThat(response.getBackupName()).isEqualTo("filename");
    assertThat(response.getCdn()).isEqualTo(1);
    assertThat(response.getUsedSpace()).isEqualTo(0L);
  }


  @CartesianTest
  void list(
      @CartesianTest.Values(booleans = {true, false}) final boolean cursorProvided,
      @CartesianTest.Values(booleans = {true, false}) final boolean cursorReturned)
      throws VerificationFailedException {

    final byte[] mediaId = TestRandomUtil.nextBytes(15);
    final Optional<String> expectedCursor = cursorProvided ? Optional.of("myCursor") : Optional.empty();
    final Optional<String> returnedCursor = cursorReturned ? Optional.of("newCursor") : Optional.empty();

    final int limit = 17;

    when(backupManager.list(any(), eq(expectedCursor), eq(limit)))
        .thenReturn(CompletableFuture.completedFuture(new BackupManager.ListMediaResult(
            List.of(new BackupManager.StorageDescriptorWithLength(1, mediaId, 100)),
            returnedCursor)));

    final ListMediaRequest.Builder request = ListMediaRequest.newBuilder()
        .setSignedPresentation(signedPresentation(presentation))
        .setLimit(limit);
    if (cursorProvided) {
      request.setCursor("myCursor");
    }

    final ListMediaResponse response = unauthenticatedServiceStub().listMedia(request.build());
    assertThat(response.getPageCount()).isEqualTo(1);
    assertThat(response.getPage(0).getLength()).isEqualTo(100);
    assertThat(response.getPage(0).getMediaId().toByteArray()).isEqualTo(mediaId);
    assertThat(response.hasCursor() ? response.getCursor() : null).isEqualTo(returnedCursor.orElse(null));

  }

  @Test
  void delete() {
    final DeleteMediaRequest request = DeleteMediaRequest.newBuilder()
        .setSignedPresentation(signedPresentation(presentation))
        .addAllItems(IntStream.range(0, 100).mapToObj(i ->
                DeleteMediaItem.newBuilder()
                    .setCdn(3)
                    .setMediaId(ByteString.copyFrom(TestRandomUtil.nextBytes(15)))
                    .build())
            .toList()).build();

    when(backupManager.deleteMedia(any(), any()))
        .thenReturn(Flux.fromStream(request.getItemsList().stream()
            .map(m -> new BackupManager.StorageDescriptor(m.getCdn(), m.getMediaId().toByteArray()))));

    final AtomicInteger count = new AtomicInteger(0);
    unauthenticatedServiceStub().deleteMedia(request).forEachRemaining(i -> count.getAndIncrement());
    assertThat(count.get()).isEqualTo(100);
  }

  @Test
  void mediaUploadForm() {
    when(backupManager.createTemporaryAttachmentUploadDescriptor(any()))
        .thenReturn(CompletableFuture.completedFuture(
            new BackupUploadDescriptor(3, "abc", Map.of("k", "v"), "example.org")));
    final GetUploadFormRequest request = GetUploadFormRequest.newBuilder()
        .setMedia(GetUploadFormRequest.MediaUploadType.getDefaultInstance())
        .setSignedPresentation(signedPresentation(presentation))
        .build();

    final GetUploadFormResponse uploadForm = unauthenticatedServiceStub().getUploadForm(request);
    assertThat(uploadForm.getCdn()).isEqualTo(3);
    assertThat(uploadForm.getKey()).isEqualTo("abc");
    assertThat(uploadForm.getHeadersMap()).containsExactlyEntriesOf(Map.of("k", "v"));
    assertThat(uploadForm.getSignedUploadLocation()).isEqualTo("example.org");

    // rate limit
    when(backupManager.createTemporaryAttachmentUploadDescriptor(any()))
        .thenReturn(CompletableFuture.failedFuture(new RateLimitExceededException(null)));
    assertThatExceptionOfType(StatusRuntimeException.class)
        .isThrownBy(() -> unauthenticatedServiceStub().getUploadForm(request))
        .extracting(StatusRuntimeException::getStatus)
        .isEqualTo(Status.RESOURCE_EXHAUSTED);
  }

  @Test
  void readAuth() {
    when(backupManager.generateReadAuth(any(), eq(3))).thenReturn(Map.of("key", "value"));

    final GetCdnCredentialsResponse response = unauthenticatedServiceStub().getCdnCredentials(
        GetCdnCredentialsRequest.newBuilder()
            .setCdn(3)
            .setSignedPresentation(signedPresentation(presentation))
            .build());
    assertThat(response.getHeadersMap()).containsExactlyEntriesOf(Map.of("key", "value"));
  }

  private static AuthenticatedBackupUser backupUser(final byte[] backupId, final BackupCredentialType credentialType,
      final BackupLevel backupLevel) {
    return new AuthenticatedBackupUser(backupId, credentialType, backupLevel, "myBackupDir", "myMediaDir", null);
  }

  private static BackupAuthCredentialPresentation presentation(BackupAuthTestUtil backupAuthTestUtil,
      byte[] messagesBackupKey, UUID aci) {
    try {
      return backupAuthTestUtil.getPresentation(BackupLevel.PAID, messagesBackupKey, aci);
    } catch (VerificationFailedException e) {
      throw new RuntimeException(e);
    }
  }

  private static SignedPresentation signedPresentation(BackupAuthCredentialPresentation presentation) {
    return SignedPresentation.newBuilder()
        .setPresentation(ByteString.copyFrom(presentation.serialize()))
        .setPresentationSignature(ByteString.copyFromUtf8("aaa")).build();
  }

}
