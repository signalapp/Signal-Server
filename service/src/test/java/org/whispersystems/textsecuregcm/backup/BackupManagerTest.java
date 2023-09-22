/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.backup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.commons.lang3.RandomUtils;
import org.assertj.core.api.ThrowableAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.backups.BackupAuthCredentialPresentation;
import org.whispersystems.textsecuregcm.auth.AuthenticatedBackupUser;
import org.whispersystems.textsecuregcm.backup.BackupManager.BackupInfo;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtension;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import org.whispersystems.textsecuregcm.util.TestClock;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;

public class BackupManagerTest {

  @RegisterExtension
  private static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(
      DynamoDbExtensionSchema.Tables.BACKUPS);

  private final TestClock testClock = TestClock.now();
  private final BackupAuthTestUtil backupAuthTestUtil = new BackupAuthTestUtil(testClock);
  private final TusBackupCredentialGenerator tusCredentialGenerator = mock(TusBackupCredentialGenerator.class);
  private final byte[] backupKey = RandomUtils.nextBytes(32);
  private final UUID aci = UUID.randomUUID();

  private BackupManager backupManager;

  @BeforeEach
  public void setup() {
    reset(tusCredentialGenerator);
    testClock.unpin();
    this.backupManager = new BackupManager(
        backupAuthTestUtil.params,
        tusCredentialGenerator,
        DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
        DynamoDbExtensionSchema.Tables.BACKUPS.tableName(),
        testClock);
  }

  @ParameterizedTest
  @EnumSource(mode = EnumSource.Mode.EXCLUDE, names = {"NONE"})
  public void createBackup(final BackupTier backupTier) throws InvalidInputException, VerificationFailedException {

    final Instant now = Instant.ofEpochSecond(Duration.ofDays(1).getSeconds());
    testClock.pin(now);

    final AuthenticatedBackupUser backupUser = backupUser(RandomUtils.nextBytes(16), backupTier);
    final String encodedBackupId = Base64.getUrlEncoder().encodeToString(hashedBackupId(backupUser.backupId()));

    backupManager.createMessageBackupUploadDescriptor(backupUser).join();
    verify(tusCredentialGenerator, times(1))
        .generateUpload(encodedBackupId, BackupManager.MESSAGE_BACKUP_NAME);

    final BackupInfo info = backupManager.backupInfo(backupUser).join();
    assertThat(info.backupSubdir()).isEqualTo(encodedBackupId);
    assertThat(info.messageBackupKey()).isEqualTo(BackupManager.MESSAGE_BACKUP_NAME);
    assertThat(info.mediaUsedSpace()).isEqualTo(Optional.empty());

    // Check that the initial expiration times are the initial write times
    checkExpectedExpirations(now, backupTier == BackupTier.MEDIA ? now : null, backupUser.backupId());
  }

  @ParameterizedTest
  @EnumSource(mode = EnumSource.Mode.EXCLUDE, names = {"NONE"})
  public void ttlRefresh(final BackupTier backupTier) throws InvalidInputException, VerificationFailedException {
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
        backupUser.backupId());
  }

  @ParameterizedTest
  @EnumSource(mode = EnumSource.Mode.EXCLUDE, names = {"NONE"})
  public void createBackupRefreshesTtl(final BackupTier backupTier) throws VerificationFailedException {
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
        backupUser.backupId());
  }

  @Test
  public void unknownPublicKey() throws VerificationFailedException {
    final BackupAuthCredentialPresentation presentation = backupAuthTestUtil.getPresentation(
        BackupTier.MESSAGES, backupKey, aci);

    final ECKeyPair keyPair = Curve.generateKeyPair();
    final byte[] signature = keyPair.getPrivateKey().calculateSignature(presentation.serialize());

    // haven't set a public key yet
    assertThatExceptionOfType(StatusRuntimeException.class)
        .isThrownBy(unwrapExceptions(() -> backupManager.authenticateBackupUser(presentation, signature)))
        .extracting(ex -> ex.getStatus().getCode())
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
    assertThatExceptionOfType(StatusRuntimeException.class)
        .isThrownBy(unwrapExceptions(() -> backupManager.setPublicKey(presentation, signature2, keyPair2.getPublicKey())))
        .extracting(ex -> ex.getStatus().getCode())
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
        .isThrownBy(unwrapExceptions(() -> backupManager.setPublicKey(presentation, wrongSignature, keyPair.getPublicKey())))
        .extracting(ex -> ex.getStatus().getCode())
        .isEqualTo(Status.UNAUTHENTICATED.getCode());

    backupManager.setPublicKey(presentation, signature, keyPair.getPublicKey()).join();

    // shouldn't be able to authenticate with an invalid signature
    assertThatExceptionOfType(StatusRuntimeException.class)
        .isThrownBy(unwrapExceptions(() -> backupManager.authenticateBackupUser(presentation, wrongSignature)))
        .extracting(ex -> ex.getStatus().getCode())
        .isEqualTo(Status.UNAUTHENTICATED.getCode());

    // correct signature
    final AuthenticatedBackupUser user = backupManager.authenticateBackupUser(presentation, signature).join();
    assertThat(user.backupId()).isEqualTo(presentation.getBackupId());
    assertThat(user.backupTier()).isEqualTo(BackupTier.MESSAGES);
  }

  @Test
  public void credentialExpiration() throws InvalidInputException, VerificationFailedException {

    // credential for 1 day after epoch
    testClock.pin(Instant.ofEpochSecond(1).plus(Duration.ofDays(1)));
    final BackupAuthCredentialPresentation oldCredential = backupAuthTestUtil.getPresentation(BackupTier.MESSAGES, backupKey, aci);
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
        .isThrownBy(unwrapExceptions(() -> backupManager.authenticateBackupUser(oldCredential, signature)))
        .extracting(ex -> ex.getStatus().getCode())
        .isEqualTo(Status.UNAUTHENTICATED.getCode());
  }

  private void checkExpectedExpirations(
      final Instant expectedExpiration,
      final @Nullable Instant expectedMediaExpiration,
      final byte[] backupId) {
    final GetItemResponse item = DYNAMO_DB_EXTENSION.getDynamoDbClient().getItem(GetItemRequest.builder()
        .tableName(DynamoDbExtensionSchema.Tables.BACKUPS.tableName())
        .key(Map.of(BackupManager.KEY_BACKUP_ID_HASH, AttributeValues.b(hashedBackupId(backupId))))
        .build());
    assertThat(item.hasItem()).isTrue();
    final Instant refresh = Instant.ofEpochSecond(Long.parseLong(item.item().get(BackupManager.ATTR_LAST_REFRESH).n()));
    assertThat(refresh).isEqualTo(expectedExpiration);

    if (expectedMediaExpiration == null) {
      assertThat(item.item()).doesNotContainKey(BackupManager.ATTR_LAST_MEDIA_REFRESH);
    } else {
      assertThat(Instant.ofEpochSecond(Long.parseLong(item.item().get(BackupManager.ATTR_LAST_MEDIA_REFRESH).n())))
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

  private <T> ThrowableAssert.ThrowingCallable unwrapExceptions(final Supplier<CompletableFuture<T>> f) {
    return () -> {
      try {
        f.get().join();
      } catch (Exception e) {
        if (ExceptionUtils.unwrap(e) instanceof StatusRuntimeException ex) {
          throw ex;
        }
        throw e;
      }
    };
  }
}
