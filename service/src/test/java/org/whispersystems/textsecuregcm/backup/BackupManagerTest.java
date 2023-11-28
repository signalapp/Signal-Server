/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.backup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.backups.BackupAuthCredentialPresentation;
import org.whispersystems.textsecuregcm.auth.AuthenticatedBackupUser;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtension;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.CompletableFutureTestUtil;
import org.whispersystems.textsecuregcm.util.TestClock;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;

public class BackupManagerTest {

  @RegisterExtension
  public static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(
      DynamoDbExtensionSchema.Tables.BACKUPS,
      DynamoDbExtensionSchema.Tables.BACKUP_MEDIA);

  private final TestClock testClock = TestClock.now();
  private final BackupAuthTestUtil backupAuthTestUtil = new BackupAuthTestUtil(testClock);
  private final Cdn3BackupCredentialGenerator tusCredentialGenerator = mock(Cdn3BackupCredentialGenerator.class);
  private final RemoteStorageManager remoteStorageManager = mock(RemoteStorageManager.class);
  private final byte[] backupKey = RandomUtils.nextBytes(32);
  private final UUID aci = UUID.randomUUID();

  private BackupManager backupManager;

  @BeforeEach
  public void setup() {
    reset(tusCredentialGenerator);
    testClock.unpin();
    this.backupManager = new BackupManager(
        new BackupsDb(DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
            DynamoDbExtensionSchema.Tables.BACKUPS.tableName(), DynamoDbExtensionSchema.Tables.BACKUP_MEDIA.tableName(),
            testClock),
        backupAuthTestUtil.params,
        tusCredentialGenerator,
        remoteStorageManager,
        Map.of(3, "cdn3.example.org/attachments"),
        testClock);
  }

  @ParameterizedTest
  @EnumSource(mode = EnumSource.Mode.EXCLUDE, names = {"NONE"})
  public void createBackup(final BackupTier backupTier) {

    final Instant now = Instant.ofEpochSecond(Duration.ofDays(1).getSeconds());
    testClock.pin(now);

    final AuthenticatedBackupUser backupUser = backupUser(RandomUtils.nextBytes(16), backupTier);
    final String encodedBackupId = Base64.getUrlEncoder().encodeToString(hashedBackupId(backupUser.backupId()));

    backupManager.createMessageBackupUploadDescriptor(backupUser).join();
    verify(tusCredentialGenerator, times(1))
        .generateUpload(encodedBackupId, BackupManager.MESSAGE_BACKUP_NAME);

    final BackupManager.BackupInfo info = backupManager.backupInfo(backupUser).join();
    assertThat(info.backupSubdir()).isEqualTo(encodedBackupId);
    assertThat(info.messageBackupKey()).isEqualTo(BackupManager.MESSAGE_BACKUP_NAME);
    assertThat(info.mediaUsedSpace()).isEqualTo(Optional.empty());

    // Check that the initial expiration times are the initial write times
    checkExpectedExpirations(now, backupTier == BackupTier.MEDIA ? now : null, backupUser);
  }

  @ParameterizedTest
  @EnumSource(mode = EnumSource.Mode.EXCLUDE, names = {"NONE"})
  public void ttlRefresh(final BackupTier backupTier) {
    final AuthenticatedBackupUser backupUser = backupUser(RandomUtils.nextBytes(16), backupTier);

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
        backupTier == BackupTier.MEDIA ? tnext : null,
        backupUser);
  }

  @ParameterizedTest
  @EnumSource(mode = EnumSource.Mode.EXCLUDE, names = {"NONE"})
  public void createBackupRefreshesTtl(final BackupTier backupTier) {
    final Instant tstart = Instant.ofEpochSecond(1).plus(Duration.ofDays(1));
    final Instant tnext = tstart.plus(Duration.ofSeconds(1));

    final AuthenticatedBackupUser backupUser = backupUser(RandomUtils.nextBytes(16), backupTier);

    // create backup at t=tstart
    testClock.pin(tstart);
    backupManager.createMessageBackupUploadDescriptor(backupUser).join();

    // create again at t=tnext
    testClock.pin(tnext);
    backupManager.createMessageBackupUploadDescriptor(backupUser).join();

    checkExpectedExpirations(
        tnext,
        backupTier == BackupTier.MEDIA ? tnext : null,
        backupUser);
  }

  @Test
  public void unknownPublicKey() throws VerificationFailedException {
    final BackupAuthCredentialPresentation presentation = backupAuthTestUtil.getPresentation(
        BackupTier.MESSAGES, backupKey, aci);

    final ECKeyPair keyPair = Curve.generateKeyPair();
    final byte[] signature = keyPair.getPrivateKey().calculateSignature(presentation.serialize());

    // haven't set a public key yet
    assertThat(CompletableFutureTestUtil.assertFailsWithCause(
            StatusRuntimeException.class,
            backupManager.authenticateBackupUser(presentation, signature))
        .getStatus().getCode())
        .isEqualTo(Status.NOT_FOUND.getCode());
  }

  @Test
  public void mismatchedPublicKey() throws VerificationFailedException {
    final BackupAuthCredentialPresentation presentation = backupAuthTestUtil.getPresentation(
        BackupTier.MESSAGES, backupKey, aci);

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
        BackupTier.MESSAGES, backupKey, aci);

    final ECKeyPair keyPair = Curve.generateKeyPair();
    final byte[] signature = keyPair.getPrivateKey().calculateSignature(presentation.serialize());

    // an invalid signature
    final byte[] wrongSignature = Arrays.copyOf(signature, signature.length);
    wrongSignature[1] += 1;

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
    assertThat(user.backupTier()).isEqualTo(BackupTier.MESSAGES);
  }

  @Test
  public void credentialExpiration() throws VerificationFailedException {

    // credential for 1 day after epoch
    testClock.pin(Instant.ofEpochSecond(1).plus(Duration.ofDays(1)));
    final BackupAuthCredentialPresentation oldCredential = backupAuthTestUtil.getPresentation(BackupTier.MESSAGES,
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
    assertThat(CompletableFutureTestUtil.assertFailsWithCause(
            StatusRuntimeException.class,
            backupManager.authenticateBackupUser(oldCredential, signature))
        .getStatus().getCode())
        .isEqualTo(Status.UNAUTHENTICATED.getCode());
  }

  @Test
  public void copySuccess() {
    final AuthenticatedBackupUser backupUser = backupUser(RandomUtils.nextBytes(16), BackupTier.MEDIA);
    when(tusCredentialGenerator.generateUpload(any(), any()))
        .thenReturn(new MessageBackupUploadDescriptor(3, "def", Collections.emptyMap(), ""));
    when(remoteStorageManager.copy(eq(URI.create("cdn3.example.org/attachments/abc")), eq(100), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    final BackupManager.StorageDescriptor copied = backupManager.copyToBackup(
        backupUser, 3, "abc", 100, mock(MediaEncryptionParameters.class),
        "def".getBytes(StandardCharsets.UTF_8)).join();

    assertThat(copied.cdn()).isEqualTo(3);
    assertThat(copied.key()).isEqualTo("def".getBytes(StandardCharsets.UTF_8));

    final Map<String, AttributeValue> backup = getBackupItem(backupUser);
    final long bytesUsed = AttributeValues.getLong(backup, BackupsDb.ATTR_MEDIA_BYTES_USED, 0L);
    assertThat(bytesUsed).isEqualTo(100);

    final long mediaCount = AttributeValues.getLong(backup, BackupsDb.ATTR_MEDIA_COUNT, 0L);
    assertThat(mediaCount).isEqualTo(1);

    final Map<String, AttributeValue> mediaItem = getBackupMediaItem(backupUser,
        "def".getBytes(StandardCharsets.UTF_8));
    final long mediaLength = AttributeValues.getLong(mediaItem, BackupsDb.ATTR_LENGTH, 0L);
    assertThat(mediaLength).isEqualTo(100L);
  }

  @Test
  public void copyFailure() {
    final AuthenticatedBackupUser backupUser = backupUser(RandomUtils.nextBytes(16), BackupTier.MEDIA);
    when(tusCredentialGenerator.generateUpload(any(), any()))
        .thenReturn(new MessageBackupUploadDescriptor(3, "def", Collections.emptyMap(), ""));
    when(remoteStorageManager.copy(eq(URI.create("cdn3.example.org/attachments/abc")), eq(100), any(), any()))
        .thenReturn(CompletableFuture.failedFuture(new SourceObjectNotFoundException()));

    CompletableFutureTestUtil.assertFailsWithCause(SourceObjectNotFoundException.class,
        backupManager.copyToBackup(
            backupUser,
            3, "abc", 100,
            mock(MediaEncryptionParameters.class),
            "def".getBytes(StandardCharsets.UTF_8)));

    final Map<String, AttributeValue> backup = getBackupItem(backupUser);
    assertThat(AttributeValues.getLong(backup, BackupsDb.ATTR_MEDIA_BYTES_USED, -1L)).isEqualTo(0L);
    assertThat(AttributeValues.getLong(backup, BackupsDb.ATTR_MEDIA_COUNT, -1L)).isEqualTo(0L);

    final Map<String, AttributeValue> media = getBackupMediaItem(backupUser, "def".getBytes(StandardCharsets.UTF_8));
    assertThat(media).isEmpty();
  }

  private Map<String, AttributeValue> getBackupItem(final AuthenticatedBackupUser backupUser) {
    return DYNAMO_DB_EXTENSION.getDynamoDbClient().getItem(GetItemRequest.builder()
            .tableName(DynamoDbExtensionSchema.Tables.BACKUPS.tableName())
            .key(Map.of(BackupsDb.KEY_BACKUP_ID_HASH, AttributeValues.b(hashedBackupId(backupUser.backupId()))))
            .build())
        .item();
  }

  private Map<String, AttributeValue> getBackupMediaItem(final AuthenticatedBackupUser backupUser,
      final byte[] mediaId) {
    return DYNAMO_DB_EXTENSION.getDynamoDbClient().getItem(GetItemRequest.builder()
            .tableName(DynamoDbExtensionSchema.Tables.BACKUP_MEDIA.tableName())
            .key(Map.of(
                BackupsDb.KEY_BACKUP_ID_HASH, AttributeValues.b(hashedBackupId(backupUser.backupId())),
                BackupsDb.KEY_MEDIA_ID, AttributeValues.b(mediaId)))
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

  private AuthenticatedBackupUser backupUser(final byte[] backupId, final BackupTier backupTier) {
    return new AuthenticatedBackupUser(backupId, backupTier);
  }
}
