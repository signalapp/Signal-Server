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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.whispersystems.textsecuregcm.util.MockUtils.randomSecretBytes;

import io.dropwizard.util.DataSize;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.assertj.core.api.ThrowableAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.signal.libsignal.protocol.InvalidKeyException;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.signal.libsignal.protocol.ecc.ECPrivateKey;
import org.signal.libsignal.zkgroup.GenericServerSecretParams;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.backups.BackupAuthCredentialPresentation;
import org.signal.libsignal.zkgroup.backups.BackupCredentialType;
import org.signal.libsignal.zkgroup.backups.BackupLevel;
import org.whispersystems.textsecuregcm.attachments.TusAttachmentGenerator;
import org.whispersystems.textsecuregcm.auth.AuthenticatedBackupUser;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.configuration.SecureValueRecoveryConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicBackupConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.securevaluerecovery.SecureValueRecoveryClient;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtension;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.CompletableFutureTestUtil;
import org.whispersystems.textsecuregcm.util.TestClock;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;

public class BackupManagerTest {

  @RegisterExtension
  public static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(
      DynamoDbExtensionSchema.Tables.BACKUPS);

  private static final MediaEncryptionParameters COPY_ENCRYPTION_PARAM = new MediaEncryptionParameters(
      TestRandomUtil.nextBytes(32),
      TestRandomUtil.nextBytes(32));
  private static final CopyParameters COPY_PARAM = new CopyParameters(
      3, "abc", 100,
      COPY_ENCRYPTION_PARAM, TestRandomUtil.nextBytes(15));
  private static final long MAX_TOTAL_MEDIA_BYTES = DataSize.mebibytes(1).toBytes();

  private final TestClock testClock = TestClock.now();
  private final BackupAuthTestUtil backupAuthTestUtil = new BackupAuthTestUtil(testClock);
  private final RateLimiter mediaUploadLimiter = mock(RateLimiter.class);
  private final TusAttachmentGenerator tusAttachmentGenerator = mock(TusAttachmentGenerator.class);
  private final Cdn3BackupCredentialGenerator tusCredentialGenerator = mock(Cdn3BackupCredentialGenerator.class);
  private final RemoteStorageManager remoteStorageManager = mock(RemoteStorageManager.class);
  private final byte[] backupKey = TestRandomUtil.nextBytes(32);
  private final UUID aci = UUID.randomUUID();
  private final DynamicBackupConfiguration backupConfiguration = new DynamicBackupConfiguration(
    3, 4, 5, Duration.ofSeconds(30), MAX_TOTAL_MEDIA_BYTES);


  private static final SecureValueRecoveryConfiguration CFG = new SecureValueRecoveryConfiguration(
      "",
      randomSecretBytes(32),
      randomSecretBytes(32),
      null,
      null,
      null);
  private final ExternalServiceCredentialsGenerator svrbCredentialGenerator =
      SecureValueRecoveryBCredentialsGeneratorFactory.svrbCredentialsGenerator(CFG, testClock);
  private final SecureValueRecoveryClient svrbClient = mock(SecureValueRecoveryClient.class);

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
    @SuppressWarnings("unchecked") final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager =
        mock(DynamicConfigurationManager.class);
    final DynamicConfiguration dynamicConfiguration = mock(DynamicConfiguration.class);
    when(dynamicConfiguration.getBackupConfiguration()).thenReturn(backupConfiguration);
    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);

    this.backupManager = new BackupManager(
        backupsDb,
        backupAuthTestUtil.params,
        rateLimiters,
        tusAttachmentGenerator,
        tusCredentialGenerator,
        remoteStorageManager,
        svrbCredentialGenerator,
        svrbClient,
        testClock,
        dynamicConfigurationManager);
  }

  @ParameterizedTest
  @CsvSource({
      "FREE, FREE, false",
      "FREE, PAID, true",
      "PAID, FREE, false",
      "PAID, PAID, false"
  })
  void checkBackupLevel(final BackupLevel authenticateBackupLevel,
      final BackupLevel requiredLevel,
      final boolean expectException) {

    final AuthenticatedBackupUser backupUser =
        backupUser(TestRandomUtil.nextBytes(16), BackupCredentialType.MESSAGES, authenticateBackupLevel);

    final ThrowableAssert.ThrowingCallable checkBackupLevel =
        () -> BackupManager.checkBackupLevel(backupUser, requiredLevel);

    if (expectException) {
      assertThatExceptionOfType(StatusRuntimeException.class)
          .isThrownBy(checkBackupLevel)
          .extracting(StatusRuntimeException::getStatus)
          .extracting(Status::getCode)
          .isEqualTo(Status.Code.PERMISSION_DENIED);
    } else {
      assertThatNoException().isThrownBy(checkBackupLevel);
    }
  }

  @ParameterizedTest
  @CsvSource({
      "MESSAGES, MESSAGES, false",
      "MESSAGES, MEDIA, true",
      "MEDIA, MESSAGES, true",
      "MEDIA, MEDIA, false"
  })
  void checkBackupCredentialType(final BackupCredentialType authenticateCredentialType,
      final BackupCredentialType requiredCredentialType,
      final boolean expectException) {

    final AuthenticatedBackupUser backupUser =
        backupUser(TestRandomUtil.nextBytes(16), authenticateCredentialType, BackupLevel.FREE);

    final ThrowableAssert.ThrowingCallable checkCredentialType =
        () -> BackupManager.checkBackupCredentialType(backupUser, requiredCredentialType);

    if (expectException) {
      assertThatExceptionOfType(StatusRuntimeException.class)
          .isThrownBy(checkCredentialType)
          .extracting(StatusRuntimeException::getStatus)
          .extracting(Status::getCode)
          .isEqualTo(Status.Code.UNAUTHENTICATED);
    } else {
      assertThatNoException().isThrownBy(checkCredentialType);
    }
  }

  @ParameterizedTest
  @EnumSource
  public void createBackup(final BackupLevel backupLevel) {

    final Instant now = Instant.ofEpochSecond(Duration.ofDays(1).getSeconds());
    testClock.pin(now);

    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupCredentialType.MESSAGES, backupLevel);

    backupManager.createMessageBackupUploadDescriptor(backupUser);
    verify(tusCredentialGenerator, times(1))
        .generateUpload("%s/%s".formatted(backupUser.backupDir(), BackupManager.MESSAGE_BACKUP_NAME));

    final BackupManager.BackupInfo info = backupManager.backupInfo(backupUser);
    assertThat(info.backupSubdir()).isEqualTo(backupUser.backupDir()).isNotBlank();
    assertThat(info.messageBackupKey()).isEqualTo(BackupManager.MESSAGE_BACKUP_NAME);
    assertThat(info.mediaUsedSpace()).isEqualTo(Optional.empty());

    // Check that the initial expiration times are the initial write times
    checkExpectedExpirations(now, backupLevel == BackupLevel.PAID ? now : null, backupUser);
  }

  @ParameterizedTest
  @EnumSource
  public void createBackupWrongCredentialType(final BackupLevel backupLevel) {

    final Instant now = Instant.ofEpochSecond(Duration.ofDays(1).getSeconds());
    testClock.pin(now);

    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupCredentialType.MEDIA, backupLevel);

    assertThatExceptionOfType(StatusRuntimeException.class)
        .isThrownBy(() -> backupManager.createMessageBackupUploadDescriptor(backupUser))
        .matches(exception -> exception.getStatus().getCode() == Status.UNAUTHENTICATED.getCode());
  }

  @Test
  public void createTemporaryMediaAttachmentRateLimited() throws RateLimitExceededException {
    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupCredentialType.MEDIA, BackupLevel.PAID);
    doThrow(new RateLimitExceededException(null))
        .when(mediaUploadLimiter).validate(eq(BackupManager.rateLimitKey(backupUser)));
    assertThatExceptionOfType(RateLimitExceededException.class)
        .isThrownBy(() -> backupManager.createTemporaryAttachmentUploadDescriptor(backupUser));
  }

  @Test
  public void createTemporaryMediaAttachmentWrongTier() {
    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupCredentialType.MEDIA, BackupLevel.FREE);
    assertThatExceptionOfType(StatusRuntimeException.class)
        .isThrownBy(() -> backupManager.createTemporaryAttachmentUploadDescriptor(backupUser))
        .extracting(StatusRuntimeException::getStatus)
        .extracting(Status::getCode)
        .isEqualTo(Status.Code.PERMISSION_DENIED);
  }

  @Test
  public void createTemporaryMediaAttachmentWrongCredentialType() {
    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupCredentialType.MESSAGES, BackupLevel.PAID);
    assertThatExceptionOfType(StatusRuntimeException.class)
        .isThrownBy(() -> backupManager.createTemporaryAttachmentUploadDescriptor(backupUser))
        .extracting(StatusRuntimeException::getStatus)
        .extracting(Status::getCode)
        .isEqualTo(Status.Code.UNAUTHENTICATED);
  }

  @ParameterizedTest
  @EnumSource
  public void ttlRefresh(final BackupLevel backupLevel) {
    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupCredentialType.MESSAGES, backupLevel);

    final Instant tstart = Instant.ofEpochSecond(1).plus(Duration.ofDays(1));
    final Instant tnext = tstart.plus(Duration.ofDays(1));

    // create backup at t=tstart
    testClock.pin(tstart);
    backupManager.createMessageBackupUploadDescriptor(backupUser);

    // refresh at t=tnext
    testClock.pin(tnext);
    backupManager.ttlRefresh(backupUser);

    checkExpectedExpirations(
        tnext.truncatedTo(ChronoUnit.DAYS),
        backupLevel == BackupLevel.PAID ? tnext.truncatedTo(ChronoUnit.DAYS) : null,
        backupUser);
  }

  @ParameterizedTest
  @EnumSource
  public void createBackupRefreshesTtl(final BackupLevel backupLevel) {
    final Instant tstart = Instant.ofEpochSecond(1).plus(Duration.ofDays(1));
    final Instant tnext = tstart.plus(Duration.ofDays(1));

    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupCredentialType.MESSAGES, backupLevel);

    // create backup at t=tstart
    testClock.pin(tstart);
    backupManager.createMessageBackupUploadDescriptor(backupUser);

    // create again at t=tnext
    testClock.pin(tnext);
    backupManager.createMessageBackupUploadDescriptor(backupUser);

    checkExpectedExpirations(
        tnext.truncatedTo(ChronoUnit.DAYS),
        backupLevel == BackupLevel.PAID ? tnext.truncatedTo(ChronoUnit.DAYS) : null,
        backupUser);
  }

  @Test
  public void invalidPresentationNoPublicKey() throws VerificationFailedException {
    final BackupAuthCredentialPresentation invalidPresentation = backupAuthTestUtil.getPresentation(
        GenericServerSecretParams.generate(),
        BackupLevel.FREE, backupKey, aci);

    final ECKeyPair keyPair = ECKeyPair.generate();

    // haven't set a public key yet, but should fail before hitting the database anyway
    assertThatExceptionOfType(StatusRuntimeException.class)
        .isThrownBy(() -> backupManager.authenticateBackupUser(
            invalidPresentation,
            keyPair.getPrivateKey().calculateSignature(invalidPresentation.serialize()),
            null))
        .extracting(StatusRuntimeException::getStatus)
        .extracting(Status::getCode)
        .isEqualTo(Status.UNAUTHENTICATED.getCode());
  }


  @Test
  public void invalidPresentationCorrectSignature() throws VerificationFailedException {
    final BackupAuthCredentialPresentation presentation = backupAuthTestUtil.getPresentation(
        BackupLevel.FREE, backupKey, aci);
    final BackupAuthCredentialPresentation invalidPresentation = backupAuthTestUtil.getPresentation(
        GenericServerSecretParams.generate(),
        BackupLevel.FREE, backupKey, aci);

    final ECKeyPair keyPair = ECKeyPair.generate();
    backupManager.setPublicKey(
        presentation,
        keyPair.getPrivateKey().calculateSignature(presentation.serialize()),
        keyPair.getPublicKey());

    assertThatExceptionOfType(StatusRuntimeException.class)
        .isThrownBy(() -> backupManager.authenticateBackupUser(
            invalidPresentation,
            keyPair.getPrivateKey().calculateSignature(invalidPresentation.serialize()),
            null))
        .extracting(StatusRuntimeException::getStatus)
        .extracting(Status::getCode)
        .isEqualTo(Status.UNAUTHENTICATED.getCode());
  }

  @Test
  public void unknownPublicKey() throws VerificationFailedException {
    final BackupAuthCredentialPresentation presentation = backupAuthTestUtil.getPresentation(
        BackupLevel.FREE, backupKey, aci);

    final ECKeyPair keyPair = ECKeyPair.generate();
    final byte[] signature = keyPair.getPrivateKey().calculateSignature(presentation.serialize());

    // haven't set a public key yet
    assertThatExceptionOfType(StatusRuntimeException.class)
        .isThrownBy(() -> backupManager.authenticateBackupUser(presentation, signature, null))
        .extracting(StatusRuntimeException::getStatus)
        .extracting(Status::getCode)
        .isEqualTo(Status.UNAUTHENTICATED.getCode());
  }

  @Test
  public void mismatchedPublicKey() throws VerificationFailedException {
    final BackupAuthCredentialPresentation presentation = backupAuthTestUtil.getPresentation(
        BackupLevel.FREE, backupKey, aci);

    final ECKeyPair keyPair1 = ECKeyPair.generate();
    final ECKeyPair keyPair2 = ECKeyPair.generate();
    final byte[] signature1 = keyPair1.getPrivateKey().calculateSignature(presentation.serialize());
    final byte[] signature2 = keyPair2.getPrivateKey().calculateSignature(presentation.serialize());

    backupManager.setPublicKey(presentation, signature1, keyPair1.getPublicKey());

    // shouldn't be able to set a different public key
    assertThatExceptionOfType(StatusRuntimeException.class).isThrownBy(() ->
            backupManager.setPublicKey(presentation, signature2, keyPair2.getPublicKey()))
        .extracting(StatusRuntimeException::getStatus)
        .extracting(Status::getCode)
        .isEqualTo(Status.UNAUTHENTICATED.getCode());

    // should be able to set the same public key again (noop)
    backupManager.setPublicKey(presentation, signature1, keyPair1.getPublicKey());
  }

  @Test
  public void signatureValidation() throws VerificationFailedException {
    final BackupAuthCredentialPresentation presentation = backupAuthTestUtil.getPresentation(
        BackupLevel.FREE, backupKey, aci);

    final ECKeyPair keyPair = ECKeyPair.generate();
    final byte[] signature = keyPair.getPrivateKey().calculateSignature(presentation.serialize());

    // an invalid signature
    final byte[] wrongSignature = Arrays.copyOf(signature, signature.length);
    wrongSignature[1] += 1;

    // shouldn't be able to set a public key with an invalid signature
    assertThatExceptionOfType(StatusRuntimeException.class)
        .isThrownBy(() -> backupManager.setPublicKey(presentation, wrongSignature, keyPair.getPublicKey()))
        .extracting(ex -> ex.getStatus().getCode())
        .isEqualTo(Status.UNAUTHENTICATED.getCode());

    backupManager.setPublicKey(presentation, signature, keyPair.getPublicKey());

    // shouldn't be able to authenticate with an invalid signature
    assertThatExceptionOfType(StatusRuntimeException.class)
        .isThrownBy(() -> backupManager.authenticateBackupUser(presentation, wrongSignature, null))
        .extracting(StatusRuntimeException::getStatus)
        .extracting(Status::getCode)
        .isEqualTo(Status.UNAUTHENTICATED.getCode());

    // correct signature
    final AuthenticatedBackupUser user = backupManager.authenticateBackupUser(presentation, signature, null);
    assertThat(user.backupId()).isEqualTo(presentation.getBackupId());
    assertThat(user.backupLevel()).isEqualTo(BackupLevel.FREE);
  }

  @Test
  public void credentialExpiration() throws VerificationFailedException {

    // credential for 1 day after epoch
    testClock.pin(Instant.ofEpochSecond(1).plus(Duration.ofDays(1)));
    final BackupAuthCredentialPresentation oldCredential = backupAuthTestUtil.getPresentation(BackupLevel.FREE,
        backupKey, aci);
    final ECKeyPair keyPair = ECKeyPair.generate();
    final byte[] signature = keyPair.getPrivateKey().calculateSignature(oldCredential.serialize());
    backupManager.setPublicKey(oldCredential, signature, keyPair.getPublicKey());

    // should be accepted the day before to forgive clock skew
    testClock.pin(Instant.ofEpochSecond(1));
    assertThatNoException().isThrownBy(() -> backupManager.authenticateBackupUser(oldCredential, signature, null));

    // should be accepted the day after to forgive clock skew
    testClock.pin(Instant.ofEpochSecond(1).plus(Duration.ofDays(2)));
    assertThatNoException().isThrownBy(() -> backupManager.authenticateBackupUser(oldCredential, signature, null));

    // should be rejected the day after that
    testClock.pin(Instant.ofEpochSecond(1).plus(Duration.ofDays(3)));
    assertThatExceptionOfType(StatusRuntimeException.class)
        .isThrownBy(() -> backupManager.authenticateBackupUser(oldCredential, signature, null))
        .extracting(StatusRuntimeException::getStatus)
        .extracting(Status::getCode)
        .isEqualTo(Status.UNAUTHENTICATED.getCode());
  }

  @Test
  public void copySuccess() {
    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupCredentialType.MEDIA, BackupLevel.PAID);
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
  public void copyUsageCheckpoints() throws InterruptedException {
    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupCredentialType.MEDIA, BackupLevel.PAID);
    backupsDb.setMediaUsage(backupUser, new UsageInfo(0, 0)).join();

    final List<String> sourceKeys = IntStream.range(0, 50)
        .mapToObj(ignore -> RandomStringUtils.insecure().nextAlphanumeric(10))
        .toList();
    final List<CopyParameters> toCopy = sourceKeys.stream()
        .map(source -> new CopyParameters(3, source, 100, COPY_ENCRYPTION_PARAM, TestRandomUtil.nextBytes(15)))
        .toList();

    final int slowIndex = backupConfiguration.usageCheckpointCount() - 1;
    final CompletableFuture<Void> slow = new CompletableFuture<>();
    when(remoteStorageManager.copy(eq(3), anyString(), eq(100), any(), anyString()))
        .thenReturn(CompletableFuture.completedFuture(null));
    when(remoteStorageManager.copy(eq(3), eq(sourceKeys.get(slowIndex)), eq(100), any(), anyString()))
        .thenReturn(slow);
    final ArrayBlockingQueue<CopyResult> copyResults = new ArrayBlockingQueue<>(100);
    final CompletableFuture<Void> future = backupManager
        .copyToBackup(backupUser, toCopy)
        .doOnNext(copyResults::add).then().toFuture();

    for (int i = 0; i < slowIndex; i++) {
      assertThat(copyResults.poll(1, TimeUnit.SECONDS)).isNotNull();
    }

    // Copying can start on the next batch of USAGE_CHECKPOINT_COUNT before the current one is done, so we should see
    // at least one usage update, and at most 2
    final long bytesPerObject = COPY_ENCRYPTION_PARAM.outputSize(100);
    assertThat(backupsDb.getMediaUsage(backupUser).join().usageInfo()).isIn(
        new UsageInfo(
            bytesPerObject * backupConfiguration.usageCheckpointCount(),
            backupConfiguration.usageCheckpointCount()),
        new UsageInfo(
            2 * bytesPerObject * backupConfiguration.usageCheckpointCount(),
            2L * backupConfiguration.usageCheckpointCount()));

    // We should still be waiting since we have a slow delete
    assertThat(future).isNotDone();

    slow.complete(null);
    future.join();
    assertThat(backupsDb.getMediaUsage(backupUser).join().usageInfo())
        .isEqualTo(new UsageInfo(bytesPerObject * 50, 50));
  }

  @Test
  public void copyFailure() {
    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupCredentialType.MEDIA, BackupLevel.PAID);
    assertThat(copyError(backupUser, new SourceObjectNotFoundException()).outcome())
        .isEqualTo(CopyResult.Outcome.SOURCE_NOT_FOUND);

    // usage should be rolled back after a known copy failure
    final Map<String, AttributeValue> backup = getBackupItem(backupUser);
    assertThat(AttributeValues.getLong(backup, BackupsDb.ATTR_MEDIA_BYTES_USED, -1L)).isEqualTo(0L);
    assertThat(AttributeValues.getLong(backup, BackupsDb.ATTR_MEDIA_COUNT, -1L)).isEqualTo(0L);
  }

  @Test
  public void copyPartialSuccess() {
    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupCredentialType.MEDIA, BackupLevel.PAID);
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

    assertThat(results).hasSize(3);
    assertThat(results.get(0).outcome()).isEqualTo(CopyResult.Outcome.SUCCESS);
    assertThat(results.get(1).outcome()).isEqualTo(CopyResult.Outcome.SOURCE_NOT_FOUND);
    assertThat(results.get(2).outcome()).isEqualTo(CopyResult.Outcome.SOURCE_WRONG_LENGTH);

    // usage should be rolled back after a known copy failure
    final Map<String, AttributeValue> backup = getBackupItem(backupUser);
    assertThat(AttributeValues.getLong(backup, BackupsDb.ATTR_MEDIA_BYTES_USED, -1L))
        .isEqualTo(toCopy.getFirst().destinationObjectSize());
    assertThat(AttributeValues.getLong(backup, BackupsDb.ATTR_MEDIA_COUNT, -1L)).isEqualTo(1L);
  }

  @Test
  public void copyWrongCredentialType() {
    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupCredentialType.MESSAGES, BackupLevel.PAID);

    assertThatExceptionOfType(StatusRuntimeException.class)
        .isThrownBy(() -> copy(backupUser))
        .extracting(StatusRuntimeException::getStatus)
        .extracting(Status::getCode)
        .isEqualTo(Status.Code.UNAUTHENTICATED);
  }

  @Test
  public void quotaEnforcementNoRecalculation() {
    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupCredentialType.MEDIA, BackupLevel.PAID);
    verifyNoInteractions(remoteStorageManager);

    // set the backupsDb to be out of quota at t=0
    testClock.pin(Instant.ofEpochSecond(1));
    backupsDb.setMediaUsage(backupUser, new UsageInfo(MAX_TOTAL_MEDIA_BYTES, 1000)).join();
    // check still within staleness bound (t=0 + 1 day - 1 sec)
    testClock.pin(Instant.ofEpochSecond(0)
        .plus(backupConfiguration.maxQuotaStaleness())
        .minus(Duration.ofSeconds(1)));

    // Try to copy
    assertThat(copy(backupUser).outcome()).isEqualTo(CopyResult.Outcome.OUT_OF_QUOTA);
  }

  @Test
  public void quotaEnforcementRecalculation() {
    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupCredentialType.MEDIA, BackupLevel.PAID);
    final String backupMediaPrefix = "%s/%s/".formatted(backupUser.backupDir(), backupUser.mediaDir());

    final long remainingAfterRecalc = MAX_TOTAL_MEDIA_BYTES - COPY_PARAM.destinationObjectSize();

    // on recalculation, say there's actually enough left to do the copy
    when(remoteStorageManager.calculateBytesUsed(eq(backupMediaPrefix)))
        .thenReturn(CompletableFuture.completedFuture(new UsageInfo(remainingAfterRecalc, 1000)));

    // set the backupsDb to be totally out of quota at t=0
    testClock.pin(Instant.ofEpochSecond(0));
    backupsDb.setMediaUsage(backupUser, new UsageInfo(MAX_TOTAL_MEDIA_BYTES, 1000)).join();
    testClock.pin(Instant.ofEpochSecond(0).plus(backupConfiguration.maxQuotaStaleness()));

    // Should recalculate quota and copy can succeed
    assertThat(copy(backupUser).outcome()).isEqualTo(CopyResult.Outcome.SUCCESS);

    // backupsDb should have the new value
    final BackupsDb.TimestampedUsageInfo info = backupsDb.getMediaUsage(backupUser).join();
    assertThat(info.lastRecalculationTime())
        .isEqualTo(Instant.ofEpochSecond(0).plus(backupConfiguration.maxQuotaStaleness()));
    assertThat(info.usageInfo().bytesUsed()).isEqualTo(MAX_TOTAL_MEDIA_BYTES);
    assertThat(info.usageInfo().numObjects()).isEqualTo(1001);
  }

  @CartesianTest()
  public void quotaEnforcement(
      @CartesianTest.Values(booleans = {true, false}) boolean hasSpaceBeforeRecalc,
      @CartesianTest.Values(booleans = {true, false}) boolean hasSpaceAfterRecalc,
      @CartesianTest.Values(booleans = {true, false}) boolean doesReaclc) {
    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupCredentialType.MEDIA, BackupLevel.PAID);
    final String backupMediaPrefix = "%s/%s/".formatted(backupUser.backupDir(), backupUser.mediaDir());

    final long destSize = COPY_PARAM.destinationObjectSize();
    final long originalRemainingSpace =
        MAX_TOTAL_MEDIA_BYTES - (hasSpaceBeforeRecalc ? destSize : (destSize - 1));
    final long afterRecalcRemainingSpace =
        MAX_TOTAL_MEDIA_BYTES - (hasSpaceAfterRecalc ? destSize : (destSize - 1));

    // set the backupsDb to be out of quota at t=0
    testClock.pin(Instant.ofEpochSecond(0));
    backupsDb.setMediaUsage(backupUser, new UsageInfo(originalRemainingSpace, 1000)).join();

    if (doesReaclc) {
      testClock.pin(Instant.ofEpochSecond(0).plus(backupConfiguration.maxQuotaStaleness()).plus(Duration.ofSeconds(1)));
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

  @Test
  public void requestRecalculation() {
    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupCredentialType.MEDIA, BackupLevel.PAID);
    final String backupMediaPrefix = "%s/%s/".formatted(backupUser.backupDir(), backupUser.mediaDir());
    final UsageInfo oldUsage = new UsageInfo(1000, 100);
    final UsageInfo newUsage = new UsageInfo(2000, 200);

    testClock.pin(Instant.ofEpochSecond(123));
    backupsDb.setMediaUsage(backupUser, oldUsage).join();
    when(remoteStorageManager.calculateBytesUsed(eq(backupMediaPrefix)))
        .thenReturn(CompletableFuture.completedFuture(newUsage));
    final StoredBackupAttributes attrs = backupManager.listBackupAttributes(1)
        .single()
        .blockOptional().orElseThrow();

    testClock.pin(Instant.ofEpochSecond(456));
    assertThat(backupManager.recalculateQuota(attrs).toCompletableFuture().join())
        .get()
        .isEqualTo(new BackupManager.RecalculationResult(oldUsage, newUsage));

    // backupsDb should have the new value
    final BackupsDb.TimestampedUsageInfo info = backupsDb.getMediaUsage(backupUser).join();
    assertThat(info.lastRecalculationTime()).isEqualTo(Instant.ofEpochSecond(456));
    assertThat(info.usageInfo()).isEqualTo(newUsage);
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "cursor"})
  public void list(final String cursorVal) {
    final Optional<String> cursor = Optional.of(cursorVal).filter(StringUtils::isNotBlank);
    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupCredentialType.MESSAGES, BackupLevel.PAID);
    final String backupMediaPrefix = "%s/%s/".formatted(backupUser.backupDir(), backupUser.mediaDir());

    when(remoteStorageManager.cdnNumber()).thenReturn(13);
    when(remoteStorageManager.list(eq(backupMediaPrefix), eq(cursor), eq(17L)))
        .thenReturn(CompletableFuture.completedFuture(new RemoteStorageManager.ListResult(
            List.of(new RemoteStorageManager.ListResult.Entry("aaa", 123)),
            Optional.of("newCursor")
        )));

    final BackupManager.ListMediaResult result = backupManager.list(backupUser, cursor, 17);
    assertThat(result.media()).hasSize(1);
    assertThat(result.media().getFirst().cdn()).isEqualTo(13);
    assertThat(result.media().getFirst().key()).isEqualTo(
        Base64.getDecoder().decode("aaa".getBytes(StandardCharsets.UTF_8)));
    assertThat(result.media().getFirst().length()).isEqualTo(123);
    assertThat(result.cursor().orElseThrow()).isEqualTo("newCursor");

  }

  @Test
  public void deleteEntireBackup() {
    final AuthenticatedBackupUser original = backupUser(TestRandomUtil.nextBytes(16), BackupCredentialType.MESSAGES, BackupLevel.PAID);

    testClock.pin(Instant.ofEpochSecond(10));

    when(svrbClient.removeData(anyString())).thenReturn(CompletableFuture.completedFuture(null));

    // Deleting should swap the backupDir for the user
    backupManager.deleteEntireBackup(original);
    verifyNoInteractions(remoteStorageManager);
    verify(svrbClient).removeData(HexFormat.of().formatHex(BackupsDb.hashedBackupId(original.backupId())));

    final AuthenticatedBackupUser after = retrieveBackupUser(original.backupId(), BackupCredentialType.MESSAGES, BackupLevel.PAID);
    assertThat(original.backupDir()).isNotEqualTo(after.backupDir());
    assertThat(original.mediaDir()).isNotEqualTo(after.mediaDir());

    // Trying again should do the deletion inline
    when(remoteStorageManager.list(anyString(), any(), anyLong()))
        .thenReturn(CompletableFuture.completedFuture(new RemoteStorageManager.ListResult(
            Collections.emptyList(),
            Optional.empty()
        )));
    backupManager.deleteEntireBackup(after);
    verify(remoteStorageManager, times(1))
        .list(eq(after.backupDir() + "/"), eq(Optional.empty()), anyLong());

    // The original prefix to expire should be flagged as requiring expiration
    final ExpiredBackup expiredBackup = backupManager
        .getExpiredBackups(1, Schedulers.immediate(), Instant.ofEpochSecond(1L))
        .collectList()
        .blockOptional().orElseThrow()
        .getFirst();
    assertThat(expiredBackup.hashedBackupId()).isEqualTo(hashedBackupId(original.backupId()));
    assertThat(expiredBackup.prefixToDelete()).isEqualTo(original.backupDir());
    assertThat(expiredBackup.expirationType()).isEqualTo(ExpiredBackup.ExpirationType.GARBAGE_COLLECTION);
  }

  @Test
  public void delete() {
    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupCredentialType.MEDIA, BackupLevel.PAID);
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
  public void deleteWrongCredentialType() {
    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupCredentialType.MESSAGES, BackupLevel.PAID);
    final byte[] mediaId = TestRandomUtil.nextBytes(16);
    assertThatThrownBy(() ->
        backupManager.deleteMedia(backupUser, List.of(new BackupManager.StorageDescriptor(5, mediaId))).then().block())
        .isInstanceOf(StatusRuntimeException.class)
        .matches(e -> ((StatusRuntimeException) e).getStatus().getCode() == Status.UNAUTHENTICATED.getCode());
  }

  @Test
  public void deleteUnknownCdn() {
    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupCredentialType.MEDIA, BackupLevel.PAID);
    final BackupManager.StorageDescriptor sd = new BackupManager.StorageDescriptor(4, TestRandomUtil.nextBytes(15));
    when(remoteStorageManager.cdnNumber()).thenReturn(5);
    assertThatThrownBy(() ->
        backupManager.deleteMedia(backupUser, List.of(sd)).then().block())
        .isInstanceOf(StatusRuntimeException.class)
        .matches(e -> ((StatusRuntimeException) e).getStatus().getCode() == Status.INVALID_ARGUMENT.getCode());
  }

  @Test
  public void deleteUsageCheckpoints() throws InterruptedException {
    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupCredentialType.MEDIA,
        BackupLevel.PAID);

    // 100 objects, each 2 bytes large
    final List<byte[]> mediaIds = IntStream.range(0, 100).mapToObj(_ -> TestRandomUtil.nextBytes(16)).toList();
    backupsDb.setMediaUsage(backupUser, new UsageInfo(200, 100)).join();

    // One object is slow to delete
    final CompletableFuture<Long> slowFuture = new CompletableFuture<>();
    final String slowMediaKey = "%s/%s/%s".formatted(
        backupUser.backupDir(),
        backupUser.mediaDir(),
        BackupManager.encodeMediaIdForCdn(mediaIds.get(backupConfiguration.usageCheckpointCount() + 3)));

    when(remoteStorageManager.delete(anyString())).thenReturn(CompletableFuture.completedFuture(2L));
    when(remoteStorageManager.delete(slowMediaKey)).thenReturn(slowFuture);
    when(remoteStorageManager.cdnNumber()).thenReturn(5);


    final Flux<BackupManager.StorageDescriptor> flux = backupManager.deleteMedia(backupUser,
        mediaIds.stream()
            .map(i -> new BackupManager.StorageDescriptor(5, i))
            .toList());
    final ArrayBlockingQueue<BackupManager.StorageDescriptor> sds = new ArrayBlockingQueue<>(100);
    final CompletableFuture<Void> future = flux.doOnNext(sds::add).then().toFuture();
    for (int i = 0; i < backupConfiguration.usageCheckpointCount(); i++) {
      sds.poll(1, TimeUnit.SECONDS);
    }

    // We should still be waiting since we have a slow delete
    assertThat(future).isNotDone();
    // But we should checkpoint the usage periodically
    assertThat(backupsDb.getMediaUsage(backupUser).join().usageInfo())
        .isEqualTo(new UsageInfo(
            200 - (2L * backupConfiguration.usageCheckpointCount()),
            100 - backupConfiguration.usageCheckpointCount()));

    slowFuture.complete(2L);
    future.join();
    assertThat(backupsDb.getMediaUsage(backupUser).join().usageInfo())
        .isEqualTo(new UsageInfo(0L, 0L));
  }

  @Test
  public void deletePartialFailure() {
    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupCredentialType.MEDIA, BackupLevel.PAID);

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
          : CompletableFuture.completedFuture((long) i);

      when(remoteStorageManager.delete(backupMediaKey)).thenReturn(deleteResult);
    }
    when(remoteStorageManager.cdnNumber()).thenReturn(5);
    backupsDb.setMediaUsage(backupUser, new UsageInfo(initialBytes, 5)).join();

    final List<BackupManager.StorageDescriptor> deleted = backupManager
        .deleteMedia(backupUser, descriptors)
        .onErrorComplete()
        .collectList()
        .blockOptional().orElseThrow();
    // first two objects should be deleted
    assertThat(deleted.size()).isEqualTo(2);
    assertThat(backupsDb.getMediaUsage(backupUser).join().usageInfo())
        .isEqualTo(new UsageInfo(initialBytes - 1 - 2, 3));

  }

  @Test
  public void alreadyDeleted() {
    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupCredentialType.MEDIA, BackupLevel.PAID);
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
        .mapToObj(_ -> backupUser(TestRandomUtil.nextBytes(16), BackupCredentialType.MESSAGES, BackupLevel.PAID))
        .toList();
    for (int i = 0; i < backupUsers.size(); i++) {
      testClock.pin(days(i));
      backupManager.createMessageBackupUploadDescriptor(backupUsers.get(i));
    }

    // set of backup-id hashes that should be expired (initially t=0)
    final Set<ByteBuffer> expectedHashes = new HashSet<>();

    for (int i = 0; i < backupUsers.size(); i++) {
      final Instant day = days(i);
      testClock.pin(day);

      // get backups expired at t=i
      final List<ExpiredBackup> expired = backupManager
          .getExpiredBackups(1, Schedulers.immediate(), day)
          .collectList()
          .blockOptional().orElseThrow();

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
    testClock.pin(days(5));
    backupManager.createMessageBackupUploadDescriptor(backupUser(backupId, BackupCredentialType.MESSAGES, BackupLevel.PAID));

    // refreshed messages timestamp at t=6
    testClock.pin(days(6));
    backupManager.createMessageBackupUploadDescriptor(backupUser(backupId, BackupCredentialType.MESSAGES, BackupLevel.FREE));

    Function<Instant, List<ExpiredBackup>> getExpired = time -> backupManager
        .getExpiredBackups(1, Schedulers.immediate(), time)
        .collectList().block();

    assertThat(getExpired.apply(days(5))).isEmpty();

    assertThat(getExpired.apply(days(6)))
        .hasSize(1).first()
        .matches(eb -> eb.expirationType() == ExpiredBackup.ExpirationType.MEDIA, "is media tier");

    assertThat(getExpired.apply(days(7)))
        .hasSize(1).first()
        .matches(eb -> eb.expirationType() == ExpiredBackup.ExpirationType.ALL, "is messages tier");
  }

  @ParameterizedTest
  @EnumSource(mode = EnumSource.Mode.INCLUDE, names = {"MEDIA", "ALL"})
  public void expireBackup(ExpiredBackup.ExpirationType expirationType) {
    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupCredentialType.MESSAGES, BackupLevel.PAID);
    backupManager.createMessageBackupUploadDescriptor(backupUser);

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

    when(svrbClient.removeData(anyString())).thenReturn(CompletableFuture.completedFuture(null));

    backupManager.expireBackup(expiredBackup(expirationType, backupUser)).join();
    verify(remoteStorageManager, times(1)).list(anyString(), any(), anyLong());
    verify(remoteStorageManager, times(1)).delete(expectedPrefixToDelete + "abc");
    verify(remoteStorageManager, times(1)).delete(expectedPrefixToDelete + "def");
    verify(remoteStorageManager, times(1)).delete(expectedPrefixToDelete + "ghi");
    verify(svrbClient, times(expirationType == ExpiredBackup.ExpirationType.ALL ? 1 : 0))
        .removeData(HexFormat.of().formatHex(BackupsDb.hashedBackupId(backupUser.backupId())));
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
    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupCredentialType.MESSAGES, BackupLevel.PAID);
    backupManager.createMessageBackupUploadDescriptor(backupUser);

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

  @ParameterizedTest
  @EnumSource(BackupLevel.class)
  void svrbAuthValid(BackupLevel backupLevel) {
    testClock.pin(Instant.ofEpochSecond(123));
    final AuthenticatedBackupUser backupUser =
        backupUser(TestRandomUtil.nextBytes(16), BackupCredentialType.MESSAGES, backupLevel);
    final ExternalServiceCredentials creds = backupManager.generateSvrbAuth(backupUser);

    assertThat(HexFormat.of().parseHex(creds.username())).hasSize(16);
    final String[] split = creds.password().split(":", 2);
    assertThat(Long.parseLong(split[0])).isEqualTo(123);
  }

  @ParameterizedTest
  @EnumSource(BackupLevel.class)
  void svrbAuthInvalid(BackupLevel backupLevel) {
    // Can't use MEDIA for svrb auth
    final AuthenticatedBackupUser backupUser =
        backupUser(TestRandomUtil.nextBytes(16), BackupCredentialType.MEDIA, backupLevel);
    assertThatExceptionOfType(StatusRuntimeException.class)
        .isThrownBy(() -> backupManager.generateSvrbAuth(backupUser))
        .extracting(StatusRuntimeException::getStatus)
        .extracting(Status::getCode)
        .isEqualTo(Status.Code.UNAUTHENTICATED);
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
   * Create BackupUser with the provided backupId, credential type, and tier
   */
  private AuthenticatedBackupUser backupUser(final byte[] backupId, final BackupCredentialType credentialType, final BackupLevel backupLevel) {
    // Won't actually validate the public key, but need to have a public key to perform BackupsDB operations
    byte[] privateKey = new byte[32];
    ByteBuffer.wrap(privateKey).put(backupId);
    try {
      backupsDb.setPublicKey(backupId, backupLevel, new ECPrivateKey(privateKey).publicKey()).join();
    } catch (InvalidKeyException e) {
      throw new RuntimeException(e);
    }
    return retrieveBackupUser(backupId, credentialType, backupLevel);
  }

  /**
   * Retrieve an existing BackupUser from the database
   */
  private AuthenticatedBackupUser retrieveBackupUser(final byte[] backupId, final BackupCredentialType credentialType, final BackupLevel backupLevel) {
    final BackupsDb.AuthenticationData authData = backupsDb.retrieveAuthenticationData(backupId).join().orElseThrow();
    return new AuthenticatedBackupUser(backupId, credentialType, backupLevel, authData.backupDir(), authData.mediaDir(), null);
  }

  private static Instant days(int n) {
    return Instant.EPOCH.plus(Duration.ofDays(n));
  }
}
