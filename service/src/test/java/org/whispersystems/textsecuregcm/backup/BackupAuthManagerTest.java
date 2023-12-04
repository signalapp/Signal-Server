/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.backup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ThrowableAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.backups.BackupAuthCredentialRequest;
import org.signal.libsignal.zkgroup.backups.BackupAuthCredentialRequestContext;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.tests.util.ExperimentHelper;
import org.whispersystems.textsecuregcm.util.TestClock;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

public class BackupAuthManagerTest {
  private final UUID aci = UUID.randomUUID();
  private final byte[] backupKey = TestRandomUtil.nextBytes(32);
  private final TestClock clock = TestClock.now();
  private final BackupAuthTestUtil backupAuthTestUtil = new BackupAuthTestUtil(clock);

  @BeforeEach
  void setUp() {
    clock.unpin();
  }


  @ParameterizedTest
  @EnumSource
  void commitRequiresBackupTier(final BackupTier backupTier) {
    final AccountsManager accountsManager = mock(AccountsManager.class);
    final BackupAuthManager authManager = new BackupAuthManager(
        ExperimentHelper.withEnrollment(experimentName(backupTier), aci),
        allowRateLimiter(),
        accountsManager,
        backupAuthTestUtil.params,
        clock);
    final Account account = mock(Account.class);
    when(account.getUuid()).thenReturn(aci);
    when(accountsManager.updateAsync(any(), any())).thenReturn(CompletableFuture.completedFuture(account));

    final ThrowableAssert.ThrowingCallable commit = () ->
        authManager.commitBackupId(account, backupAuthTestUtil.getRequest(backupKey, aci)).join();
    if (backupTier == BackupTier.NONE) {
      Assertions.assertThatExceptionOfType(StatusRuntimeException.class)
          .isThrownBy(commit)
          .extracting(ex -> ex.getStatus().getCode())
          .isEqualTo(Status.Code.PERMISSION_DENIED);
    } else {
      Assertions.assertThatNoException().isThrownBy(commit);
    }
  }


  @ParameterizedTest
  @EnumSource
  void credentialsRequiresBackupTier(final BackupTier backupTier) {
    final BackupAuthManager authManager = new BackupAuthManager(
        ExperimentHelper.withEnrollment(experimentName(backupTier), aci),
        allowRateLimiter(),
        mock(AccountsManager.class),
        backupAuthTestUtil.params,
        clock);

    final Account account = mock(Account.class);
    when(account.getUuid()).thenReturn(aci);
    when(account.getBackupCredentialRequest()).thenReturn(backupAuthTestUtil.getRequest(backupKey, aci).serialize());

    final ThrowableAssert.ThrowingCallable getCreds = () ->
        assertThat(authManager.getBackupAuthCredentials(account,
            clock.instant().truncatedTo(ChronoUnit.DAYS),
            clock.instant().plus(Duration.ofDays(1)).truncatedTo(ChronoUnit.DAYS)).join())
            .hasSize(2);
    if (backupTier == BackupTier.NONE) {
      Assertions.assertThatExceptionOfType(StatusRuntimeException.class)
          .isThrownBy(getCreds)
          .extracting(ex -> ex.getStatus().getCode())
          .isEqualTo(Status.Code.PERMISSION_DENIED);
    } else {
      Assertions.assertThatNoException().isThrownBy(getCreds);
    }
  }

  @ParameterizedTest
  @EnumSource(mode = EnumSource.Mode.EXCLUDE, names = {"NONE"})
  void getReceiptCredentials(final BackupTier backupTier) throws VerificationFailedException {
    final BackupAuthManager authManager = new BackupAuthManager(
        ExperimentHelper.withEnrollment(experimentName(backupTier), aci),
        allowRateLimiter(),
        mock(AccountsManager.class),
        backupAuthTestUtil.params,
        clock);

    final BackupAuthCredentialRequestContext requestContext = BackupAuthCredentialRequestContext.create(backupKey, aci);

    final Account account = mock(Account.class);
    when(account.getUuid()).thenReturn(aci);
    when(account.getBackupCredentialRequest()).thenReturn(requestContext.getRequest().serialize());

    final Instant start = clock.instant().truncatedTo(ChronoUnit.DAYS);
    final List<BackupAuthManager.Credential> creds = authManager.getBackupAuthCredentials(account,
        start, start.plus(Duration.ofDays(7))).join();

    assertThat(creds).hasSize(8);
    Instant redemptionTime = start;
    for (BackupAuthManager.Credential cred : creds) {
      requestContext.receiveResponse(cred.credential(), backupAuthTestUtil.params.getPublicParams(),
          backupTier.getReceiptLevel());
      assertThat(cred.redemptionTime().getEpochSecond())
          .isEqualTo(redemptionTime.getEpochSecond());
      redemptionTime = redemptionTime.plus(Duration.ofDays(1));
    }
  }

  static Stream<Arguments> invalidCredentialTimeWindows() {
    final Duration max = Duration.ofDays(7);
    final Instant day0 = Instant.EPOCH;
    final Instant day1 = Instant.EPOCH.plus(Duration.ofDays(1));
    return Stream.of(
        // non-truncated start
        Arguments.of(Instant.ofEpochSecond(100), day0.plus(max), Instant.ofEpochSecond(100)),
        // non-truncated end
        Arguments.of(day0, Instant.ofEpochSecond(1).plus(max), Instant.ofEpochSecond(100)),
        // start to old
        Arguments.of(day0, day0.plus(max), day1),
        // end to new
        Arguments.of(day1, day1.plus(max), day0),
        // end before start
        Arguments.of(day1, day0, day1),
        // window too big
        Arguments.of(day0, day0.plus(max).plus(Duration.ofDays(1)), Instant.ofEpochSecond(100))
    );
  }

  @ParameterizedTest
  @MethodSource
  void invalidCredentialTimeWindows(final Instant requestRedemptionStart, final Instant requestRedemptionEnd,
      final Instant now) {
    final BackupAuthManager authManager = new BackupAuthManager(
        ExperimentHelper.withEnrollment(experimentName(BackupTier.MESSAGES), aci),
        allowRateLimiter(),
        mock(AccountsManager.class),
        backupAuthTestUtil.params,
        clock);

    final Account account = mock(Account.class);
    when(account.getUuid()).thenReturn(aci);
    when(account.getBackupCredentialRequest()).thenReturn(backupAuthTestUtil.getRequest(backupKey, aci).serialize());

    clock.pin(now);
    assertThatExceptionOfType(StatusRuntimeException.class)
        .isThrownBy(
            () -> authManager.getBackupAuthCredentials(account, requestRedemptionStart, requestRedemptionEnd).join())
        .extracting(ex -> ex.getStatus().getCode())
        .isEqualTo(Status.Code.INVALID_ARGUMENT);
  }

  @Test
  void testRateLimits() throws RateLimitExceededException {
    final AccountsManager accountsManager = mock(AccountsManager.class);
    final BackupAuthManager authManager = new BackupAuthManager(
        ExperimentHelper.withEnrollment(experimentName(BackupTier.MESSAGES), aci),
        denyRateLimiter(aci),
        accountsManager,
        backupAuthTestUtil.params,
        clock);

    final BackupAuthCredentialRequest credentialRequest = backupAuthTestUtil.getRequest(backupKey, aci);

    final Account account = mock(Account.class);
    when(account.getUuid()).thenReturn(aci);
    when(accountsManager.updateAsync(any(), any())).thenReturn(CompletableFuture.completedFuture(account));

    // Should be rate limited
    assertThatExceptionOfType(RateLimitExceededException.class)
        .isThrownBy(() -> authManager.commitBackupId(account, credentialRequest).join());

    // If we don't change the request, shouldn't be rate limited
    when(account.getBackupCredentialRequest()).thenReturn(credentialRequest.serialize());
    assertDoesNotThrow(() -> authManager.commitBackupId(account, credentialRequest).join());
  }

  private static String experimentName(BackupTier backupTier) {
    return switch (backupTier) {
      case MESSAGES -> BackupAuthManager.BACKUP_EXPERIMENT_NAME;
      case MEDIA -> BackupAuthManager.BACKUP_MEDIA_EXPERIMENT_NAME;
      case NONE -> "fake_experiment";
    };
  }

  private static RateLimiters allowRateLimiter() {
    final RateLimiters limiters = mock(RateLimiters.class);
    final RateLimiter limiter = mock(RateLimiter.class);
    when(limiters.forDescriptor(RateLimiters.For.SET_BACKUP_ID)).thenReturn(limiter);
    return limiters;
  }

  private static RateLimiters denyRateLimiter(final UUID aci) throws RateLimitExceededException {
    final RateLimiters limiters = mock(RateLimiters.class);
    final RateLimiter limiter = mock(RateLimiter.class);
    doThrow(new RateLimitExceededException(null, false)).when(limiter).validate(aci);
    when(limiters.forDescriptor(RateLimiters.For.SET_BACKUP_ID)).thenReturn(limiter);
    return limiters;
  }
}
