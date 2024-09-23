/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.backup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.signal.libsignal.protocol.InvalidKeyException;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.signal.libsignal.zkgroup.GenericServerSecretParams;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.backups.BackupAuthCredentialPresentation;
import org.signal.libsignal.zkgroup.backups.BackupLevel;
import org.whispersystems.textsecuregcm.attachments.TusAttachmentGenerator;
import org.whispersystems.textsecuregcm.auth.AuthenticatedBackupUser;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtension;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.CompletableFutureTestUtil;
import org.whispersystems.textsecuregcm.util.TestClock;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import reactor.core.scheduler.Schedulers;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;

public class BackupManagerTest {

  @RegisterExtension
  public static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(
      DynamoDbExtensionSchema.Tables.BACKUPS);

  private static final MediaEncryptionParameters COPY_ENCRYPTION_PARAM = new MediaEncryptionParameters(
      TestRandomUtil.nextBytes(32),
      TestRandomUtil.nextBytes(32),
      TestRandomUtil.nextBytes(16));
  private static final CopyParameters COPY_PARAM = new CopyParameters(
      3, "abc", 100,
      COPY_ENCRYPTION_PARAM, TestRandomUtil.nextBytes(15));
  private static final String COPY_DEST_STRING = Base64.getEncoder().encodeToString(COPY_PARAM.destinationMediaId());

  private final TestClock testClock = TestClock.now();
  private final BackupAuthTestUtil backupAuthTestUtil = new BackupAuthTestUtil(testClock);
  private final RateLimiter mediaUploadLimiter = mock(RateLimiter.class);
  private final TusAttachmentGenerator tusAttachmentGenerator = mock(TusAttachmentGenerator.class);
  private final Cdn3BackupCredentialGenerator tusCredentialGenerator = mock(Cdn3BackupCredentialGenerator.class);
  private final RemoteStorageManager remoteStorageManager = mock(RemoteStorageManager.class);
  private final byte[] backupKey = TestRandomUtil.nextBytes(32);
  private final UUID aci = UUID.randomUUID();

  private BackupManager backupManager;
  private BackupsDb backupsDb;

  @BeforeEach
  public void setup() {
    reset(tusCredentialGenerator, mediaUploadLimiter);
    testClock.unpin();

    final RateLimiters rateLimiters = mock(RateLimiters.class);
    when(rateLimiters.forDescriptor(RateLimiters.For.BACKUP_ATTACHMENT)).thenReturn(mediaUploadLimiter);

    when(remoteStorageManager.cdnNumber()).thenReturn(3);

    this.backupsDb = new BackupsDb(
        DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
        DynamoDbExtensionSchema.Tables.BACKUPS.tableName(),
        testClock);
    this.backupManager = new BackupManager(
        backupsDb,
        backupAuthTestUtil.params,
        rateLimiters,
        tusAttachmentGenerator,
        tusCredentialGenerator,
        remoteStorageManager,
        testClock);
  }

  @ParameterizedTest
  @EnumSource
  public void createBackup(final BackupLevel backupLevel) {

    final Instant now = Instant.ofEpochSecond(Duration.ofDays(1).getSeconds());
    testClock.pin(now);

    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), backupLevel);

    backupManager.createMessageBackupUploadDescriptor(backupUser).join();
    verify(tusCredentialGenerator, times(1))
        .generateUpload("%s/%s".formatted(backupUser.backupDir(), BackupManager.MESSAGE_BACKUP_NAME));

    final BackupManager.BackupInfo info = backupManager.backupInfo(backupUser).join();
    assertThat(info.backupSubdir()).isEqualTo(backupUser.backupDir()).isNotBlank();
    assertThat(info.messageBackupKey()).isEqualTo(BackupManager.MESSAGE_BACKUP_NAME);
    assertThat(info.mediaUsedSpace()).isEqualTo(Optional.empty());

    // Check that the initial expiration times are the initial write times
    checkExpectedExpirations(now, backupLevel == BackupLevel.MEDIA ? now : null, backupUser);
  }

  @Test
  public void createTemporaryMediaAttachmentRateLimited() {
    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupLevel.MEDIA);
    when(mediaUploadLimiter.validateAsync(eq(BackupManager.rateLimitKey(backupUser))))
        .thenReturn(CompletableFuture.failedFuture(new RateLimitExceededException(null)));
    final RateLimitExceededException e = CompletableFutureTestUtil.assertFailsWithCause(
        RateLimitExceededException.class,
        backupManager.createTemporaryAttachmentUploadDescriptor(backupUser).toCompletableFuture());
  }

  @Test
  public void createTemporaryMediaAttachmentWrongTier() {
    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupLevel.MESSAGES);
    assertThatExceptionOfType(StatusRuntimeException.class)
        .isThrownBy(() -> backupManager.createTemporaryAttachmentUploadDescriptor(backupUser))
        .extracting(StatusRuntimeException::getStatus)
        .extracting(Status::getCode)
        .isEqualTo(Status.Code.PERMISSION_DENIED);
  }

  @ParameterizedTest
  @EnumSource
  public void ttlRefresh(final BackupLevel backupLevel) {
    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), backupLevel);

    final Instant tstart = Instant.ofEpochSecond(1).plus(Duration.ofDays(1));
    final Instant tnext = tstart.plus(Duration.ofSeconds(1));

    // create backup at t=tstart
    testClock.pin(tstart);
    backupManager.createMessageBackupUploadDescriptor(backupUser).join();

    // refresh at t=tnext
    testClock.pin(tnext);
    backupManager.ttlRefresh(backupUser).join();

    checkExpectedExpirations(
        tnext,
        backupLevel == BackupLevel.MEDIA ? tnext : null,
        backupUser);
  }

  @ParameterizedTest
  @EnumSource
  public void createBackupRefreshesTtl(final BackupLevel backupLevel) {
    final Instant tstart = Instant.ofEpochSecond(1).plus(Duration.ofDays(1));
    final Instant tnext = tstart.plus(Duration.ofSeconds(1));

    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), backupLevel);

    // create backup at t=tstart
    testClock.pin(tstart);
    backupManager.createMessageBackupUploadDescriptor(backupUser).join();

    // create again at t=tnext
    testClock.pin(tnext);
    backupManager.createMessageBackupUploadDescriptor(backupUser).join();

    checkExpectedExpirations(
        tnext,
        backupLevel == BackupLevel.MEDIA ? tnext : null,
        backupUser);
  }

  @Test
  public void invalidPresentationNoPublicKey() throws VerificationFailedException {
    final BackupAuthCredentialPresentation invalidPresentation = backupAuthTestUtil.getPresentation(
        GenericServerSecretParams.generate(),
        BackupLevel.MESSAGES, backupKey, aci);

    final ECKeyPair keyPair = Curve.generateKeyPair();

    // haven't set a public key yet, but should fail before hitting the database anyway
    assertThatExceptionOfType(StatusRuntimeException.class)
        .isThrownBy(() -> backupManager.authenticateBackupUser(
            invalidPresentation,
            keyPair.getPrivateKey().calculateSignature(invalidPresentation.serialize())))
        .extracting(StatusRuntimeException::getStatus)
        .extracting(Status::getCode)
        .isEqualTo(Status.UNAUTHENTICATED.getCode());
  }


  @Test
  public void invalidPresentationCorrectSignature() throws VerificationFailedException {
    final BackupAuthCredentialPresentation presentation = backupAuthTestUtil.getPresentation(
        BackupLevel.MESSAGES, backupKey, aci);
    final BackupAuthCredentialPresentation invalidPresentation = backupAuthTestUtil.getPresentation(
        GenericServerSecretParams.generate(),
        BackupLevel.MESSAGES, backupKey, aci);

    final ECKeyPair keyPair = Curve.generateKeyPair();
    backupManager.setPublicKey(
        presentation,
        keyPair.getPrivateKey().calculateSignature(presentation.serialize()),
        keyPair.getPublicKey()).join();

    assertThatExceptionOfType(StatusRuntimeException.class)
        .isThrownBy(() -> backupManager.authenticateBackupUser(
            invalidPresentation,
            keyPair.getPrivateKey().calculateSignature(invalidPresentation.serialize())))
        .extracting(StatusRuntimeException::getStatus)
        .extracting(Status::getCode)
        .isEqualTo(Status.UNAUTHENTICATED.getCode());
  }

  @Test
  public void unknownPublicKey() throws VerificationFailedException {
    final BackupAuthCredentialPresentation presentation = backupAuthTestUtil.getPresentation(
        BackupLevel.MESSAGES, backupKey, aci);

    final ECKeyPair keyPair = Curve.generateKeyPair();
    final byte[] signature = keyPair.getPrivateKey().calculateSignature(presentation.serialize());

    // haven't set a public key yet
    assertThat(CompletableFutureTestUtil.assertFailsWithCause(
            StatusRuntimeException.class,
            backupManager.authenticateBackupUser(presentation, signature))
        .getStatus().getCode())
        .isEqualTo(Status.UNAUTHENTICATED.getCode());
  }

  @Test
  public void mismatchedPublicKey() throws VerificationFailedException {
    final BackupAuthCredentialPresentation presentation = backupAuthTestUtil.getPresentation(
        BackupLevel.MESSAGES, backupKey, aci);

    final ECKeyPair keyPair1 = Curve.generateKeyPair();
    final ECKeyPair keyPair2 = Curve.generateKeyPair();
    final byte[] signature1 = keyPair1.getPrivateKey().calculateSignature(presentation.serialize());
    final byte[] signature2 = keyPair2.getPrivateKey().calculateSignature(presentation.serialize());

    backupManager.setPublicKey(presentation, signature1, keyPair1.getPublicKey()).join();

    // shouldn't be able to set a different public key
    assertThat(CompletableFutureTestUtil.assertFailsWithCause(
            StatusRuntimeException.class,
            backupManager.setPublicKey(presentation, signature2, keyPair2.getPublicKey()))
        .getStatus().getCode())
        .isEqualTo(Status.UNAUTHENTICATED.getCode());

    // should be able to set the same public key again (noop)
    backupManager.setPublicKey(presentation, signature1, keyPair1.getPublicKey()).join();
  }

  @Test
  public void signatureValidation() throws VerificationFailedException {
    final BackupAuthCredentialPresentation presentation = backupAuthTestUtil.getPresentation(
        BackupLevel.MESSAGES, backupKey, aci);

    final ECKeyPair keyPair = Curve.generateKeyPair();
    final byte[] signature = keyPair.getPrivateKey().calculateSignature(presentation.serialize());

    // an invalid signature
    final byte[] wrongSignature = Arrays.copyOf(signature, signature.length);
    wrongSignature[1] = (byte) (wrongSignature[1] + 1);

    // shouldn't be able to set a public key with an invalid signature
    assertThatExceptionOfType(StatusRuntimeException.class)
        .isThrownBy(() -> backupManager.setPublicKey(presentation, wrongSignature, keyPair.getPublicKey()))
        .extracting(ex -> ex.getStatus().getCode())
        .isEqualTo(Status.UNAUTHENTICATED.getCode());

    backupManager.setPublicKey(presentation, signature, keyPair.getPublicKey()).join();

    // shouldn't be able to authenticate with an invalid signature
    assertThat(CompletableFutureTestUtil.assertFailsWithCause(
            StatusRuntimeException.class,
            backupManager.authenticateBackupUser(presentation, wrongSignature))
        .getStatus().getCode())
        .isEqualTo(Status.UNAUTHENTICATED.getCode());

    // correct signature
    final AuthenticatedBackupUser user = backupManager.authenticateBackupUser(presentation, signature).join();
    assertThat(user.backupId()).isEqualTo(presentation.getBackupId());
    assertThat(user.backupLevel()).isEqualTo(BackupLevel.MESSAGES);
  }

  @Test
  public void credentialExpiration() throws VerificationFailedException {

    // credential for 1 day after epoch
    testClock.pin(Instant.ofEpochSecond(1).plus(Duration.ofDays(1)));
    final BackupAuthCredentialPresentation oldCredential = backupAuthTestUtil.getPresentation(BackupLevel.MESSAGES,
        backupKey, aci);
    final ECKeyPair keyPair = Curve.generateKeyPair();
    final byte[] signature = keyPair.getPrivateKey().calculateSignature(oldCredential.serialize());
    backupManager.setPublicKey(oldCredential, signature, keyPair.getPublicKey()).join();

    // should be accepted the day before to forgive clock skew
    testClock.pin(Instant.ofEpochSecond(1));
    assertThatNoException().isThrownBy(() -> backupManager.authenticateBackupUser(oldCredential, signature).join());

    // should be accepted the day after to forgive clock skew
    testClock.pin(Instant.ofEpochSecond(1).plus(Duration.ofDays(2)));
    assertThatNoException().isThrownBy(() -> backupManager.authenticateBackupUser(oldCredential, signature).join());

    // should be rejected the day after that
    testClock.pin(Instant.ofEpochSecond(1).plus(Duration.ofDays(3)));
    assertThatExceptionOfType(StatusRuntimeException.class)
        .isThrownBy(() -> backupManager.authenticateBackupUser(oldCredential, signature))
        .extracting(StatusRuntimeException::getStatus)
        .extracting(Status::getCode)
        .isEqualTo(Status.UNAUTHENTICATED.getCode());
  }

  @Test
  public void copySuccess() {
    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupLevel.MEDIA);
    final CopyResult copied = copy(backupUser);

    assertThat(copied.cdn()).isEqualTo(3);
    assertThat(copied.mediaId()).isEqualTo(COPY_PARAM.destinationMediaId());
    assertThat(copied.outcome()).isEqualTo(CopyResult.Outcome.SUCCESS);

    final Map<String, AttributeValue> backup = getBackupItem(backupUser);
    final long bytesUsed = AttributeValues.getLong(backup, BackupsDb.ATTR_MEDIA_BYTES_USED, 0L);
    assertThat(bytesUsed).isEqualTo(COPY_PARAM.destinationObjectSize());

    final long mediaCount = AttributeValues.getLong(backup, BackupsDb.ATTR_MEDIA_COUNT, 0L);
    assertThat(mediaCount).isEqualTo(1);
  }

  @Test
  public void copyFailure() {
    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupLevel.MEDIA);
    assertThat(copyError(backupUser, new SourceObjectNotFoundException()).outcome())
        .isEqualTo(CopyResult.Outcome.SOURCE_NOT_FOUND);

    // usage should be rolled back after a known copy failure
    final Map<String, AttributeValue> backup = getBackupItem(backupUser);
    assertThat(AttributeValues.getLong(backup, BackupsDb.ATTR_MEDIA_BYTES_USED, -1L)).isEqualTo(0L);
    assertThat(AttributeValues.getLong(backup, BackupsDb.ATTR_MEDIA_COUNT, -1L)).isEqualTo(0L);
  }

  @Test
  public void copyPartialSuccess() {
    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupLevel.MEDIA);
    final List<CopyParameters> toCopy = List.of(
        new CopyParameters(3, "success", 100, COPY_ENCRYPTION_PARAM, TestRandomUtil.nextBytes(15)),
        new CopyParameters(3, "missing", 200, COPY_ENCRYPTION_PARAM, TestRandomUtil.nextBytes(15)),
        new CopyParameters(3, "badlength", 300, COPY_ENCRYPTION_PARAM, TestRandomUtil.nextBytes(15)));

    when(tusCredentialGenerator.generateUpload(any()))
        .thenReturn(new BackupUploadDescriptor(3, "", Collections.emptyMap(), ""));
    when(remoteStorageManager.copy(eq(3), eq("success"), eq(100), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));
    when(remoteStorageManager.copy(eq(3), eq("missing"), eq(200), any(), any()))
        .thenReturn(CompletableFuture.failedFuture(new SourceObjectNotFoundException()));
    when(remoteStorageManager.copy(eq(3), eq("badlength"), eq(300), any(), any()))
        .thenReturn(CompletableFuture.failedFuture(new InvalidLengthException("")));

    final List<CopyResult> results = backupManager.copyToBackup(backupUser, toCopy)
        .collectList().block();

    assertThat(results.get(0).outcome()).isEqualTo(CopyResult.Outcome.SUCCESS);
    assertThat(results.get(1).outcome()).isEqualTo(CopyResult.Outcome.SOURCE_NOT_FOUND);
    assertThat(results.get(2).outcome()).isEqualTo(CopyResult.Outcome.SOURCE_WRONG_LENGTH);

    // usage should be rolled back after a known copy failure
    final Map<String, AttributeValue> backup = getBackupItem(backupUser);
    assertThat(AttributeValues.getLong(backup, BackupsDb.ATTR_MEDIA_BYTES_USED, -1L))
        .isEqualTo(toCopy.get(0).destinationObjectSize());
    assertThat(AttributeValues.getLong(backup, BackupsDb.ATTR_MEDIA_COUNT, -1L)).isEqualTo(1L);
  }

  @Test
  public void quotaEnforcementNoRecalculation() {
    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupLevel.MEDIA);
    verifyNoInteractions(remoteStorageManager);

    // set the backupsDb to be out of quota at t=0
    testClock.pin(Instant.ofEpochSecond(1));
    backupsDb.setMediaUsage(backupUser, new UsageInfo(BackupManager.MAX_TOTAL_BACKUP_MEDIA_BYTES, 1000)).join();
    // check still within staleness bound (t=0 + 1 day - 1 sec)
    testClock.pin(Instant.ofEpochSecond(0)
        .plus(BackupManager.MAX_QUOTA_STALENESS)
        .minus(Duration.ofSeconds(1)));

    // Try to copy
    assertThat(copy(backupUser).outcome()).isEqualTo(CopyResult.Outcome.OUT_OF_QUOTA);
  }

  @Test
  public void quotaEnforcementRecalculation() {
    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupLevel.MEDIA);
    final String backupMediaPrefix = "%s/%s/".formatted(backupUser.backupDir(), backupUser.mediaDir());

    final long remainingAfterRecalc = BackupManager.MAX_TOTAL_BACKUP_MEDIA_BYTES - COPY_PARAM.destinationObjectSize();

    // on recalculation, say there's actually enough left to do the copy
    when(remoteStorageManager.calculateBytesUsed(eq(backupMediaPrefix)))
        .thenReturn(CompletableFuture.completedFuture(new UsageInfo(remainingAfterRecalc, 1000)));

    // set the backupsDb to be totally out of quota at t=0
    testClock.pin(Instant.ofEpochSecond(0));
    backupsDb.setMediaUsage(backupUser, new UsageInfo(BackupManager.MAX_TOTAL_BACKUP_MEDIA_BYTES, 1000)).join();
    testClock.pin(Instant.ofEpochSecond(0).plus(BackupManager.MAX_QUOTA_STALENESS));

    // Should recalculate quota and copy can succeed
    assertThat(copy(backupUser).outcome()).isEqualTo(CopyResult.Outcome.SUCCESS);

    // backupsDb should have the new value
    final BackupsDb.TimestampedUsageInfo info = backupsDb.getMediaUsage(backupUser).join();
    assertThat(info.lastRecalculationTime())
        .isEqualTo(Instant.ofEpochSecond(0).plus(BackupManager.MAX_QUOTA_STALENESS));
    assertThat(info.usageInfo().bytesUsed()).isEqualTo(BackupManager.MAX_TOTAL_BACKUP_MEDIA_BYTES);
    assertThat(info.usageInfo().numObjects()).isEqualTo(1001);
  }

  @CartesianTest()
  public void quotaEnforcement(
      @CartesianTest.Values(booleans = {true, false}) boolean hasSpaceBeforeRecalc,
      @CartesianTest.Values(booleans = {true, false}) boolean hasSpaceAfterRecalc,
      @CartesianTest.Values(booleans = {true, false}) boolean doesReaclc) {
    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupLevel.MEDIA);
    final String backupMediaPrefix = "%s/%s/".formatted(backupUser.backupDir(), backupUser.mediaDir());

    final long destSize = COPY_PARAM.destinationObjectSize();
    final long originalRemainingSpace =
        BackupManager.MAX_TOTAL_BACKUP_MEDIA_BYTES - (hasSpaceBeforeRecalc ? destSize : (destSize - 1));
    final long afterRecalcRemainingSpace =
        BackupManager.MAX_TOTAL_BACKUP_MEDIA_BYTES - (hasSpaceAfterRecalc ? destSize : (destSize - 1));

    // set the backupsDb to be out of quota at t=0
    testClock.pin(Instant.ofEpochSecond(0));
    backupsDb.setMediaUsage(backupUser, new UsageInfo(originalRemainingSpace, 1000)).join();

    if (doesReaclc) {
      testClock.pin(Instant.ofEpochSecond(0).plus(BackupManager.MAX_QUOTA_STALENESS).plus(Duration.ofSeconds(1)));
      when(remoteStorageManager.calculateBytesUsed(eq(backupMediaPrefix)))
          .thenReturn(CompletableFuture.completedFuture(new UsageInfo(afterRecalcRemainingSpace, 1000)));
    }
    final CopyResult copyResult = copy(backupUser);
    if (hasSpaceBeforeRecalc || (hasSpaceAfterRecalc && doesReaclc)) {
      assertThat(copyResult.outcome()).isEqualTo(CopyResult.Outcome.SUCCESS);
    } else {
      assertThat(copyResult.outcome()).isEqualTo(CopyResult.Outcome.OUT_OF_QUOTA);
    }
    if (doesReaclc && !hasSpaceBeforeRecalc) {
      // should have recalculated if we exceeded quota
      verify(remoteStorageManager, times(1)).calculateBytesUsed(anyString());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "cursor"})
  public void list(final String cursorVal) {
    final Optional<String> cursor = Optional.of(cursorVal).filter(StringUtils::isNotBlank);
    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupLevel.MEDIA);
    final String backupMediaPrefix = "%s/%s/".formatted(backupUser.backupDir(), backupUser.mediaDir());

    when(remoteStorageManager.cdnNumber()).thenReturn(13);
    when(remoteStorageManager.list(eq(backupMediaPrefix), eq(cursor), eq(17L)))
        .thenReturn(CompletableFuture.completedFuture(new RemoteStorageManager.ListResult(
            List.of(new RemoteStorageManager.ListResult.Entry("aaa", 123)),
            Optional.of("newCursor")
        )));

    final BackupManager.ListMediaResult result = backupManager.list(backupUser, cursor, 17)
        .toCompletableFuture().join();
    assertThat(result.media()).hasSize(1);
    assertThat(result.media().getFirst().cdn()).isEqualTo(13);
    assertThat(result.media().getFirst().key()).isEqualTo(
        Base64.getDecoder().decode("aaa".getBytes(StandardCharsets.UTF_8)));
    assertThat(result.media().getFirst().length()).isEqualTo(123);
    assertThat(result.cursor().orElseThrow()).isEqualTo("newCursor");

  }

  @Test
  public void deleteEntireBackup() {
    final AuthenticatedBackupUser original = backupUser(TestRandomUtil.nextBytes(16), BackupLevel.MEDIA);

    testClock.pin(Instant.ofEpochSecond(10));

    // Deleting should swap the backupDir for the user
    backupManager.deleteEntireBackup(original).join();
    verifyNoInteractions(remoteStorageManager);
    final AuthenticatedBackupUser after = retrieveBackupUser(original.backupId(), BackupLevel.MEDIA);
    assertThat(original.backupDir()).isNotEqualTo(after.backupDir());
    assertThat(original.mediaDir()).isNotEqualTo(after.mediaDir());

    // Trying again should do the deletion inline
    when(remoteStorageManager.list(anyString(), any(), anyLong()))
        .thenReturn(CompletableFuture.completedFuture(new RemoteStorageManager.ListResult(
            Collections.emptyList(),
            Optional.empty()
        )));
    backupManager.deleteEntireBackup(after).join();
    verify(remoteStorageManager, times(1))
        .list(eq(after.backupDir() + "/"), eq(Optional.empty()), anyLong());

    // The original prefix to expire should be flagged as requiring expiration
    final ExpiredBackup expiredBackup = backupManager
        .getExpiredBackups(1, Schedulers.immediate(), Instant.ofEpochSecond(1L))
        .collectList().block()
        .getFirst();
    assertThat(expiredBackup.hashedBackupId()).isEqualTo(hashedBackupId(original.backupId()));
    assertThat(expiredBackup.prefixToDelete()).isEqualTo(original.backupDir());
    assertThat(expiredBackup.expirationType()).isEqualTo(ExpiredBackup.ExpirationType.GARBAGE_COLLECTION);
  }

  @Test
  public void delete() {
    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupLevel.MEDIA);
    final byte[] mediaId = TestRandomUtil.nextBytes(16);
    final String backupMediaKey = "%s/%s/%s".formatted(
        backupUser.backupDir(),
        backupUser.mediaDir(),
        BackupManager.encodeMediaIdForCdn(mediaId));

    backupsDb.setMediaUsage(backupUser, new UsageInfo(100, 1000)).join();

    when(remoteStorageManager.delete(backupMediaKey))
        .thenReturn(CompletableFuture.completedFuture(7L));
    when(remoteStorageManager.cdnNumber()).thenReturn(5);
    backupManager.deleteMedia(backupUser, List.of(new BackupManager.StorageDescriptor(5, mediaId)))
        .collectList().block();

    assertThat(backupsDb.getMediaUsage(backupUser).join().usageInfo())
        .isEqualTo(new UsageInfo(93, 999));
  }

  @Test
  public void deleteUnknownCdn() {
    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupLevel.MEDIA);
    final BackupManager.StorageDescriptor sd = new BackupManager.StorageDescriptor(4, TestRandomUtil.nextBytes(15));
    when(remoteStorageManager.cdnNumber()).thenReturn(5);
    assertThatThrownBy(() ->
        backupManager.deleteMedia(backupUser, List.of(sd)).then().block())
        .isInstanceOf(StatusRuntimeException.class)
        .matches(e -> ((StatusRuntimeException) e).getStatus().getCode() == Status.INVALID_ARGUMENT.getCode());
  }

  @Test
  public void deletePartialFailure() {
    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupLevel.MEDIA);

    final List<BackupManager.StorageDescriptor> descriptors = new ArrayList<>();
    long initialBytes = 0;
    for (int i = 1; i <= 5; i++) {
      final BackupManager.StorageDescriptor descriptor = new BackupManager.StorageDescriptor(5,
          TestRandomUtil.nextBytes(15));
      descriptors.add(descriptor);
      final String backupMediaKey = "%s/%s/%s".formatted(
          backupUser.backupDir(),
          backupUser.mediaDir(),
          BackupManager.encodeMediaIdForCdn(descriptor.key()));

      initialBytes += i;
      // fail deletion 3, otherwise return the corresponding object's size as i
      final CompletableFuture<Long> deleteResult = i == 3
          ? CompletableFuture.failedFuture(new IOException("oh no"))
          : CompletableFuture.completedFuture(Long.valueOf(i));

      when(remoteStorageManager.delete(backupMediaKey)).thenReturn(deleteResult);
    }
    when(remoteStorageManager.cdnNumber()).thenReturn(5);
    backupsDb.setMediaUsage(backupUser, new UsageInfo(initialBytes, 5)).join();

    final List<BackupManager.StorageDescriptor> deleted = backupManager
        .deleteMedia(backupUser, descriptors)
        .onErrorComplete()
        .collectList().block();
    // first two objects should be deleted
    assertThat(deleted.size()).isEqualTo(2);
    assertThat(backupsDb.getMediaUsage(backupUser).join().usageInfo())
        .isEqualTo(new UsageInfo(initialBytes - 1 - 2, 3));

  }

  @Test
  public void alreadyDeleted() {
    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupLevel.MEDIA);
    final byte[] mediaId = TestRandomUtil.nextBytes(16);
    final String backupMediaKey = "%s/%s/%s".formatted(
        backupUser.backupDir(),
        backupUser.mediaDir(),
        BackupManager.encodeMediaIdForCdn(mediaId));

    backupsDb.setMediaUsage(backupUser, new UsageInfo(100, 5)).join();

    // Deletion doesn't remove anything
    when(remoteStorageManager.delete(backupMediaKey)).thenReturn(CompletableFuture.completedFuture(0L));
    when(remoteStorageManager.cdnNumber()).thenReturn(5);
    backupManager.deleteMedia(backupUser, List.of(new BackupManager.StorageDescriptor(5, mediaId))).then().block();

    assertThat(backupsDb.getMediaUsage(backupUser).join().usageInfo())
        .isEqualTo(new UsageInfo(100, 5));
  }

  @Test
  public void listExpiredBackups() {
    final List<AuthenticatedBackupUser> backupUsers = IntStream.range(0, 10)
        .mapToObj(i -> backupUser(TestRandomUtil.nextBytes(16), BackupLevel.MEDIA))
        .toList();
    for (int i = 0; i < backupUsers.size(); i++) {
      testClock.pin(Instant.ofEpochSecond(i));
      backupManager.createMessageBackupUploadDescriptor(backupUsers.get(i)).join();
    }

    // set of backup-id hashes that should be expired (initially t=0)
    final Set<ByteBuffer> expectedHashes = new HashSet<>();

    for (int i = 0; i < backupUsers.size(); i++) {
      testClock.pin(Instant.ofEpochSecond(i));

      // get backups expired at t=i
      final List<ExpiredBackup> expired = backupManager
          .getExpiredBackups(1, Schedulers.immediate(), Instant.ofEpochSecond(i))
          .collectList()
          .block();

      // all the backups tht should be expired at t=i should be returned (ones with expiration time 0,1,...i-1)
      assertThat(expired.size()).isEqualTo(expectedHashes.size());
      assertThat(expired.stream()
          .map(ExpiredBackup::hashedBackupId)
          .map(ByteBuffer::wrap)
          .allMatch(expectedHashes::contains)).isTrue();
      assertThat(expired.stream().allMatch(eb -> eb.expirationType() == ExpiredBackup.ExpirationType.ALL)).isTrue();

      // on next iteration, backup i should be expired
      expectedHashes.add(ByteBuffer.wrap(hashedBackupId(backupUsers.get(i).backupId())));
    }
  }

  @Test
  public void listExpiredBackupsByTier() {
    final byte[] backupId = TestRandomUtil.nextBytes(16);

    // refreshed media timestamp at t=5
    testClock.pin(Instant.ofEpochSecond(5));
    backupManager.createMessageBackupUploadDescriptor(backupUser(backupId, BackupLevel.MEDIA)).join();

    // refreshed messages timestamp at t=6
    testClock.pin(Instant.ofEpochSecond(6));
    backupManager.createMessageBackupUploadDescriptor(backupUser(backupId, BackupLevel.MESSAGES)).join();

    Function<Instant, List<ExpiredBackup>> getExpired = time -> backupManager
        .getExpiredBackups(1, Schedulers.immediate(), time)
        .collectList().block();

    assertThat(getExpired.apply(Instant.ofEpochSecond(5))).isEmpty();

    assertThat(getExpired.apply(Instant.ofEpochSecond(6)))
        .hasSize(1).first()
        .matches(eb -> eb.expirationType() == ExpiredBackup.ExpirationType.MEDIA, "is media tier");

    assertThat(getExpired.apply(Instant.ofEpochSecond(7)))
        .hasSize(1).first()
        .matches(eb -> eb.expirationType() == ExpiredBackup.ExpirationType.ALL, "is messages tier");
  }

  @ParameterizedTest
  @EnumSource(mode = EnumSource.Mode.INCLUDE, names = {"MEDIA", "ALL"})
  public void expireBackup(ExpiredBackup.ExpirationType expirationType) {
    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupLevel.MEDIA);
    backupManager.createMessageBackupUploadDescriptor(backupUser).join();

    final String expectedPrefixToDelete = switch (expirationType) {
      case ALL -> backupUser.backupDir();
      case MEDIA -> backupUser.backupDir() + "/" + backupUser.mediaDir();
      case GARBAGE_COLLECTION -> throw new IllegalArgumentException();
    } + "/";

    when(remoteStorageManager.list(eq(expectedPrefixToDelete), eq(Optional.empty()), anyLong()))
        .thenReturn(CompletableFuture.completedFuture(new RemoteStorageManager.ListResult(List.of(
            new RemoteStorageManager.ListResult.Entry("abc", 1),
            new RemoteStorageManager.ListResult.Entry("def", 1),
            new RemoteStorageManager.ListResult.Entry("ghi", 1)), Optional.empty())));
    when(remoteStorageManager.delete(anyString())).thenReturn(CompletableFuture.completedFuture(1L));

    backupManager.expireBackup(expiredBackup(expirationType, backupUser)).join();
    verify(remoteStorageManager, times(1)).list(anyString(), any(), anyLong());
    verify(remoteStorageManager, times(1)).delete(expectedPrefixToDelete + "abc");
    verify(remoteStorageManager, times(1)).delete(expectedPrefixToDelete + "def");
    verify(remoteStorageManager, times(1)).delete(expectedPrefixToDelete + "ghi");
    verifyNoMoreInteractions(remoteStorageManager);

    final BackupsDb.TimestampedUsageInfo usage = backupsDb.getMediaUsage(backupUser).join();
    assertThat(usage.usageInfo().bytesUsed()).isEqualTo(0L);
    assertThat(usage.usageInfo().numObjects()).isEqualTo(0L);

    if (expirationType == ExpiredBackup.ExpirationType.ALL) {
      // should have deleted the db row for the backup
      assertThat(CompletableFutureTestUtil.assertFailsWithCause(
              StatusRuntimeException.class,
              backupsDb.describeBackup(backupUser))
          .getStatus().getCode())
          .isEqualTo(Status.NOT_FOUND.getCode());
    } else {
      // should have deleted all the media, but left the backup descriptor in place
      assertThatNoException().isThrownBy(() -> backupsDb.describeBackup(backupUser).join());
    }
  }

  @Test
  public void deleteBackupPaginated() {
    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupLevel.MEDIA);
    backupManager.createMessageBackupUploadDescriptor(backupUser).join();

    final ExpiredBackup expiredBackup = expiredBackup(ExpiredBackup.ExpirationType.MEDIA, backupUser);
    final String mediaPrefix = expiredBackup.prefixToDelete() + "/";

    // Return 1 item per page. Initially the provided cursor is empty and we'll return the cursor string "1".
    // When we get the cursor "1", we'll return "2", when "2" we'll return empty indicating listing
    // is complete
    when(remoteStorageManager.list(eq(mediaPrefix), any(), anyLong())).thenAnswer(a -> {
      Optional<String> cursor = a.getArgument(1);
      return CompletableFuture.completedFuture(
          new RemoteStorageManager.ListResult(List.of(new RemoteStorageManager.ListResult.Entry(
              switch (cursor.orElse("0")) {
                case "0" -> "abc";
                case "1" -> "def";
                case "2" -> "ghi";
                default -> throw new IllegalArgumentException();
              }, 1L)),
              switch (cursor.orElse("0")) {
                case "0" -> Optional.of("1");
                case "1" -> Optional.of("2");
                case "2" -> Optional.empty();
                default -> throw new IllegalArgumentException();
              }));
    });
    when(remoteStorageManager.delete(anyString())).thenReturn(CompletableFuture.completedFuture(1L));
    backupManager.expireBackup(expiredBackup).join();
    verify(remoteStorageManager, times(3)).list(anyString(), any(), anyLong());
    verify(remoteStorageManager, times(1)).delete(mediaPrefix + "abc");
    verify(remoteStorageManager, times(1)).delete(mediaPrefix + "def");
    verify(remoteStorageManager, times(1)).delete(mediaPrefix + "ghi");
    verifyNoMoreInteractions(remoteStorageManager);
  }

  private CopyResult copyError(final AuthenticatedBackupUser backupUser, Throwable copyException) {
    when(tusCredentialGenerator.generateUpload(any()))
        .thenReturn(new BackupUploadDescriptor(3, "def", Collections.emptyMap(), ""));
    when(remoteStorageManager.copy(eq(3), eq(COPY_PARAM.sourceKey()), eq(COPY_PARAM.sourceLength()), any(), any()))
        .thenReturn(CompletableFuture.failedFuture(copyException));
    return backupManager.copyToBackup(backupUser, List.of(COPY_PARAM)).single().block();
  }

  private CopyResult copy(final AuthenticatedBackupUser backupUser) {
    when(tusCredentialGenerator.generateUpload(any()))
        .thenReturn(new BackupUploadDescriptor(3, "def", Collections.emptyMap(), ""));
    when(tusCredentialGenerator.generateUpload(any()))
        .thenReturn(new BackupUploadDescriptor(3, "def", Collections.emptyMap(), ""));
    when(remoteStorageManager.copy(eq(3), eq(COPY_PARAM.sourceKey()), eq(COPY_PARAM.sourceLength()), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));
    return backupManager.copyToBackup(backupUser, List.of(COPY_PARAM)).single().block();
  }

  private static ExpiredBackup expiredBackup(final ExpiredBackup.ExpirationType expirationType,
      final AuthenticatedBackupUser backupUser) {
    return new ExpiredBackup(
        hashedBackupId(backupUser.backupId()),
        expirationType,
        Instant.now(),
        switch (expirationType) {
          case ALL -> backupUser.backupDir();
          case MEDIA -> backupUser.backupDir() + "/" + backupUser.mediaDir();
          case GARBAGE_COLLECTION -> null;
        });
  }

  private Map<String, AttributeValue> getBackupItem(final AuthenticatedBackupUser backupUser) {
    return DYNAMO_DB_EXTENSION.getDynamoDbClient().getItem(GetItemRequest.builder()
            .tableName(DynamoDbExtensionSchema.Tables.BACKUPS.tableName())
            .key(Map.of(BackupsDb.KEY_BACKUP_ID_HASH, AttributeValues.b(hashedBackupId(backupUser.backupId()))))
            .build())
        .item();
  }

  private void checkExpectedExpirations(
      final Instant expectedExpiration,
      final @Nullable Instant expectedMediaExpiration,
      final AuthenticatedBackupUser backupUser) {
    final Map<String, AttributeValue> item = getBackupItem(backupUser);
    final Instant refresh = Instant.ofEpochSecond(Long.parseLong(item.get(BackupsDb.ATTR_LAST_REFRESH).n()));
    assertThat(refresh).isEqualTo(expectedExpiration);

    if (expectedMediaExpiration == null) {
      assertThat(item).doesNotContainKey(BackupsDb.ATTR_LAST_MEDIA_REFRESH);
    } else {
      assertThat(Instant.ofEpochSecond(Long.parseLong(item.get(BackupsDb.ATTR_LAST_MEDIA_REFRESH).n())))
          .isEqualTo(expectedMediaExpiration);
    }
  }

  private static byte[] hashedBackupId(final byte[] backupId) {
    try {
      return Arrays.copyOf(MessageDigest.getInstance("SHA-256").digest(backupId), 16);
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    }
  }

  /**
   * Create BackupUser with the provided backupId and tier
   */
  private AuthenticatedBackupUser backupUser(final byte[] backupId, final BackupLevel backupLevel) {
    // Won't actually validate the public key, but need to have a public key to perform BackupsDB operations
    byte[] privateKey = new byte[32];
    ByteBuffer.wrap(privateKey).put(backupId);
    try {
      backupsDb.setPublicKey(backupId, backupLevel, Curve.decodePrivatePoint(privateKey).publicKey()).join();
    } catch (InvalidKeyException e) {
      throw new RuntimeException(e);
    }
    return retrieveBackupUser(backupId, backupLevel);
  }

  /**
   * Retrieve an existing BackupUser from the database
   */
  private AuthenticatedBackupUser retrieveBackupUser(final byte[] backupId, final BackupLevel backupLevel) {
    final BackupsDb.AuthenticationData authData = backupsDb.retrieveAuthenticationData(backupId).join().get();
    return new AuthenticatedBackupUser(backupId, backupLevel, authData.backupDir(), authData.mediaDir());
  }
}
