/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.backup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ThrowableAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.ServerSecretParams;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.backups.BackupAuthCredentialRequest;
import org.signal.libsignal.zkgroup.backups.BackupAuthCredentialRequestContext;
import org.signal.libsignal.zkgroup.backups.BackupLevel;
import org.signal.libsignal.zkgroup.receipts.ClientZkReceiptOperations;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredential;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialPresentation;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialRequestContext;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialResponse;
import org.signal.libsignal.zkgroup.receipts.ReceiptSerial;
import org.signal.libsignal.zkgroup.receipts.ServerZkReceiptOperations;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.RedeemedReceiptsManager;
import org.whispersystems.textsecuregcm.tests.util.ExperimentHelper;
import org.whispersystems.textsecuregcm.util.CompletableFutureTestUtil;
import org.whispersystems.textsecuregcm.util.TestClock;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

public class BackupAuthManagerTest {

  private final UUID aci = UUID.randomUUID();
  private final byte[] backupKey = TestRandomUtil.nextBytes(32);
  private final ServerSecretParams receiptParams = ServerSecretParams.generate();
  private final TestClock clock = TestClock.now();
  private final BackupAuthTestUtil backupAuthTestUtil = new BackupAuthTestUtil(clock);
  private final AccountsManager accountsManager = mock(AccountsManager.class);
  private final RedeemedReceiptsManager redeemedReceiptsManager = mock(RedeemedReceiptsManager.class);

  @BeforeEach
  void setUp() {
    clock.unpin();
    reset(accountsManager);
    reset(redeemedReceiptsManager);
  }

  BackupAuthManager create(@Nullable BackupLevel backupLevel, boolean rateLimit) {
    return new BackupAuthManager(
        ExperimentHelper.withEnrollment(experimentName(backupLevel), aci),
        rateLimit ? denyRateLimiter(aci) : allowRateLimiter(),
        accountsManager,
        new ServerZkReceiptOperations(receiptParams),
        redeemedReceiptsManager,
        backupAuthTestUtil.params,
        clock);
  }

  @ParameterizedTest
  @EnumSource
  @NullSource
  void commitRequiresBackupLevel(final BackupLevel backupLevel) {
    final BackupAuthManager authManager = create(backupLevel, false);
    final Account account = mock(Account.class);
    when(account.getUuid()).thenReturn(aci);
    when(accountsManager.updateAsync(any(), any())).thenReturn(CompletableFuture.completedFuture(account));

    final ThrowableAssert.ThrowingCallable commit = () ->
        authManager.commitBackupId(account, backupAuthTestUtil.getRequest(backupKey, aci)).join();
    if (backupLevel == null) {
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
  @NullSource
  void credentialsRequiresBackupLevel(final BackupLevel backupLevel) {
    final BackupAuthManager authManager = create(backupLevel, false);

    final Account account = mock(Account.class);
    when(account.getUuid()).thenReturn(aci);
    when(account.getBackupCredentialRequest()).thenReturn(backupAuthTestUtil.getRequest(backupKey, aci).serialize());

    final ThrowableAssert.ThrowingCallable getCreds = () ->
        assertThat(authManager.getBackupAuthCredentials(account,
            clock.instant().truncatedTo(ChronoUnit.DAYS),
            clock.instant().plus(Duration.ofDays(1)).truncatedTo(ChronoUnit.DAYS)).join())
            .hasSize(2);
    if (backupLevel == null) {
      Assertions.assertThatExceptionOfType(StatusRuntimeException.class)
          .isThrownBy(getCreds)
          .extracting(ex -> ex.getStatus().getCode())
          .isEqualTo(Status.Code.PERMISSION_DENIED);
    } else {
      Assertions.assertThatNoException().isThrownBy(getCreds);
    }
  }

  @ParameterizedTest
  @EnumSource
  void getReceiptCredentials(final BackupLevel backupLevel) throws VerificationFailedException {
    final BackupAuthManager authManager = create(backupLevel, false);

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
      assertThat(requestContext
          .receiveResponse(cred.credential(), redemptionTime, backupAuthTestUtil.params.getPublicParams())
          .getBackupLevel())
          .isEqualTo(backupLevel);
      assertThat(cred.redemptionTime().getEpochSecond()).isEqualTo(redemptionTime.getEpochSecond());
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
    final BackupAuthManager authManager = create(BackupLevel.MESSAGES, false);

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
  void expiringBackupPayment() throws VerificationFailedException {
    clock.pin(Instant.ofEpochSecond(1));
    final Instant day0 = Instant.EPOCH;
    final Instant day4 = Instant.EPOCH.plus(Duration.ofDays(4));
    final Instant dayMax = day0.plus(BackupAuthManager.MAX_REDEMPTION_DURATION);

    final BackupAuthManager authManager = create(BackupLevel.MESSAGES, false);

    final Account account = mock(Account.class);
    when(account.getUuid()).thenReturn(aci);
    when(account.getBackupCredentialRequest()).thenReturn(backupAuthTestUtil.getRequest(backupKey, aci).serialize());
    when(account.getBackupVoucher()).thenReturn(new Account.BackupVoucher(201, day4));

    final List<BackupAuthManager.Credential> creds = authManager.getBackupAuthCredentials(account, day0, dayMax).join();
    Instant redemptionTime = day0;
    final BackupAuthCredentialRequestContext requestContext = BackupAuthCredentialRequestContext.create(backupKey, aci);
    for (int i = 0; i < creds.size(); i++) {
      // Before the expiration, credentials should have a media receipt, otherwise messages only
      final BackupLevel level = i < 5 ? BackupLevel.MEDIA : BackupLevel.MESSAGES;
      final BackupAuthManager.Credential cred = creds.get(i);
      assertThat(requestContext
          .receiveResponse(cred.credential(), redemptionTime, backupAuthTestUtil.params.getPublicParams())
          .getBackupLevel())
          .isEqualTo(level);
      assertThat(cred.redemptionTime().getEpochSecond()).isEqualTo(redemptionTime.getEpochSecond());
      redemptionTime = redemptionTime.plus(Duration.ofDays(1));
    }
  }

  @Test
  void expiredBackupPayment() {
    final Instant day1 = Instant.EPOCH.plus(Duration.ofDays(1));
    final Instant day2 = Instant.EPOCH.plus(Duration.ofDays(2));
    final Instant day3 = Instant.EPOCH.plus(Duration.ofDays(3));

    final BackupAuthManager authManager = create(BackupLevel.MESSAGES, false);
    final Account account = mock(Account.class);
    when(account.getUuid()).thenReturn(aci);
    when(account.getBackupVoucher()).thenReturn(new Account.BackupVoucher(3, day1));

    final Account updated = mock(Account.class);
    when(updated.getUuid()).thenReturn(aci);
    when(updated.getBackupCredentialRequest()).thenReturn(backupAuthTestUtil.getRequest(backupKey, aci).serialize());
    when(updated.getBackupVoucher()).thenReturn(null);
    when(accountsManager.updateAsync(any(), any())).thenReturn(CompletableFuture.completedFuture(updated));

    clock.pin(day2.plus(Duration.ofSeconds(1)));
    assertThat(authManager.getBackupAuthCredentials(account, day2, day2.plus(Duration.ofDays(7))).join())
        .hasSize(8);

    @SuppressWarnings("unchecked") final ArgumentCaptor<Consumer<Account>> accountUpdater = ArgumentCaptor.forClass(
        Consumer.class);
    verify(accountsManager, times(1)).updateAsync(any(), accountUpdater.capture());

    // If the account is not expired when we go to update it, we shouldn't wipe it out
    final Account alreadyUpdated = mock(Account.class);
    when(alreadyUpdated.getBackupVoucher()).thenReturn(new Account.BackupVoucher(3, day3));
    accountUpdater.getValue().accept(alreadyUpdated);
    verify(alreadyUpdated, never()).setBackupVoucher(any());

    // If the account is still expired when we go to update it, we can wipe it out
    final Account expired = mock(Account.class);
    when(expired.getBackupVoucher()).thenReturn(new Account.BackupVoucher(3, day1));
    accountUpdater.getValue().accept(expired);
    verify(expired, times(1)).setBackupVoucher(null);
  }


  @Test
  void redeemReceipt() throws InvalidInputException, VerificationFailedException {
    final Instant expirationTime = Instant.EPOCH.plus(Duration.ofDays(1));
    final BackupAuthManager authManager = create(BackupLevel.MESSAGES, false);
    final Account account = mock(Account.class);
    when(account.getUuid()).thenReturn(aci);

    clock.pin(Instant.EPOCH.plus(Duration.ofDays(1)));
    when(accountsManager.updateAsync(any(), any())).thenReturn(CompletableFuture.completedFuture(account));
    when(redeemedReceiptsManager.put(any(), eq(expirationTime.getEpochSecond()), eq(201L), eq(aci)))
        .thenReturn(CompletableFuture.completedFuture(true));
    authManager.redeemReceipt(account, receiptPresentation(201, expirationTime)).join();
    verify(accountsManager, times(1)).updateAsync(any(), any());
  }

  @Test
  void mergeRedemptions() throws InvalidInputException, VerificationFailedException {
    final Instant newExpirationTime = Instant.EPOCH.plus(Duration.ofDays(1));
    final Instant existingExpirationTime = Instant.EPOCH.plus(Duration.ofDays(1)).plus(Duration.ofSeconds(1));

    final BackupAuthManager authManager = create(BackupLevel.MESSAGES, false);
    final Account account = mock(Account.class);
    when(account.getUuid()).thenReturn(aci);

    // The account has an existing voucher with a later expiration date
    when(account.getBackupVoucher()).thenReturn(new Account.BackupVoucher(201, existingExpirationTime));

    clock.pin(Instant.EPOCH.plus(Duration.ofDays(1)));
    when(accountsManager.updateAsync(any(), any())).thenReturn(CompletableFuture.completedFuture(account));
    when(redeemedReceiptsManager.put(any(), eq(newExpirationTime.getEpochSecond()), eq(201L), eq(aci)))
        .thenReturn(CompletableFuture.completedFuture(true));
    authManager.redeemReceipt(account, receiptPresentation(201, newExpirationTime)).join();

    final ArgumentCaptor<Consumer<Account>> updaterCaptor = ArgumentCaptor.captor();
    verify(accountsManager, times(1)).updateAsync(any(), updaterCaptor.capture());

    updaterCaptor.getValue().accept(account);
    // Should select the voucher with the later expiration time
    verify(account).setBackupVoucher(eq(new Account.BackupVoucher(201, existingExpirationTime)));
  }

  @Test
  void redeemExpiredReceipt() {
    final Instant expirationTime = Instant.EPOCH.plus(Duration.ofDays(1));
    clock.pin(expirationTime.plus(Duration.ofSeconds(1)));
    final BackupAuthManager authManager = create(BackupLevel.MESSAGES, false);
    Assertions.assertThatExceptionOfType(StatusRuntimeException.class)
        .isThrownBy(() -> authManager.redeemReceipt(mock(Account.class), receiptPresentation(3, expirationTime)).join())
        .extracting(ex -> ex.getStatus().getCode())
        .isEqualTo(Status.Code.INVALID_ARGUMENT);
    verifyNoInteractions(accountsManager);
    verifyNoInteractions(redeemedReceiptsManager);
  }

  @ParameterizedTest
  @ValueSource(longs = {0, 1, 2, 200, 500})
  void redeemInvalidLevel(long level) {
    final Instant expirationTime = Instant.EPOCH.plus(Duration.ofDays(1));
    clock.pin(expirationTime.plus(Duration.ofSeconds(1)));
    final BackupAuthManager authManager = create(BackupLevel.MESSAGES, false);
    Assertions.assertThatExceptionOfType(StatusRuntimeException.class)
        .isThrownBy(() ->
            authManager.redeemReceipt(mock(Account.class), receiptPresentation(level, expirationTime)).join())
        .extracting(ex -> ex.getStatus().getCode())
        .isEqualTo(Status.Code.INVALID_ARGUMENT);
    verifyNoInteractions(accountsManager);
    verifyNoInteractions(redeemedReceiptsManager);
  }

  @Test
  void redeemInvalidPresentation() throws InvalidInputException, VerificationFailedException {
    final BackupAuthManager authManager = create(BackupLevel.MESSAGES, false);
    final ReceiptCredentialPresentation invalid = receiptPresentation(ServerSecretParams.generate(), 3L, Instant.EPOCH);
    Assertions.assertThatExceptionOfType(StatusRuntimeException.class)
        .isThrownBy(() -> authManager.redeemReceipt(mock(Account.class), invalid).join())
        .extracting(ex -> ex.getStatus().getCode())
        .isEqualTo(Status.Code.INVALID_ARGUMENT);
    verifyNoInteractions(accountsManager);
    verifyNoInteractions(redeemedReceiptsManager);
  }

  @Test
  void receiptAlreadyRedeemed() throws InvalidInputException, VerificationFailedException {
    final Instant expirationTime = Instant.EPOCH.plus(Duration.ofDays(1));
    final BackupAuthManager authManager = create(BackupLevel.MESSAGES, false);
    final Account account = mock(Account.class);
    when(account.getUuid()).thenReturn(aci);

    clock.pin(Instant.EPOCH.plus(Duration.ofDays(1)));
    when(accountsManager.updateAsync(any(), any())).thenReturn(CompletableFuture.completedFuture(account));
    when(redeemedReceiptsManager.put(any(), eq(expirationTime.getEpochSecond()), eq(201L), eq(aci)))
        .thenReturn(CompletableFuture.completedFuture(false));

    final CompletableFuture<Void> result = authManager.redeemReceipt(account, receiptPresentation(201, expirationTime));
    assertThat(CompletableFutureTestUtil.assertFailsWithCause(StatusRuntimeException.class, result))
        .extracting(ex -> ex.getStatus().getCode())
        .isEqualTo(Status.Code.INVALID_ARGUMENT);
    verifyNoInteractions(accountsManager);
  }

  private ReceiptCredentialPresentation receiptPresentation(long level, Instant redemptionTime)
      throws InvalidInputException, VerificationFailedException {
    return receiptPresentation(receiptParams, level, redemptionTime);
  }

  private ReceiptCredentialPresentation receiptPresentation(ServerSecretParams params, long level,
      Instant redemptionTime)
      throws InvalidInputException, VerificationFailedException {
    final ServerZkReceiptOperations serverOps = new ServerZkReceiptOperations(params);
    final ClientZkReceiptOperations clientOps = new ClientZkReceiptOperations(params.getPublicParams());

    final ReceiptCredentialRequestContext rcrc = clientOps
        .createReceiptCredentialRequestContext(new ReceiptSerial(TestRandomUtil.nextBytes(ReceiptSerial.SIZE)));

    final ReceiptCredentialResponse response =
        serverOps.issueReceiptCredential(rcrc.getRequest(), redemptionTime.getEpochSecond(), level);
    final ReceiptCredential receiptCredential = clientOps.receiveReceiptCredential(rcrc, response);
    return clientOps.createReceiptCredentialPresentation(receiptCredential);
  }


  @Test
  void testRateLimits() {
    final AccountsManager accountsManager = mock(AccountsManager.class);
    final BackupAuthManager authManager = create(BackupLevel.MESSAGES, true);

    final BackupAuthCredentialRequest credentialRequest = backupAuthTestUtil.getRequest(backupKey, aci);

    final Account account = mock(Account.class);
    when(account.getUuid()).thenReturn(aci);
    when(accountsManager.updateAsync(any(), any())).thenReturn(CompletableFuture.completedFuture(account));

    // Should be rate limited
    final RateLimitExceededException ex = CompletableFutureTestUtil.assertFailsWithCause(
        RateLimitExceededException.class,
        authManager.commitBackupId(account, credentialRequest));

    // If we don't change the request, shouldn't be rate limited
    when(account.getBackupCredentialRequest()).thenReturn(credentialRequest.serialize());
    assertDoesNotThrow(() -> authManager.commitBackupId(account, credentialRequest).join());
  }

  private static String experimentName(@Nullable BackupLevel backupLevel) {
    return switch (backupLevel) {
      case MESSAGES -> BackupAuthManager.BACKUP_EXPERIMENT_NAME;
      case MEDIA -> BackupAuthManager.BACKUP_MEDIA_EXPERIMENT_NAME;
      case null -> "fake_experiment";
    };
  }

  private static RateLimiters allowRateLimiter() {
    final RateLimiters limiters = mock(RateLimiters.class);
    final RateLimiter limiter = mock(RateLimiter.class);
    when(limiter.validateAsync(any(UUID.class))).thenReturn(CompletableFuture.completedFuture(null));
    when(limiters.forDescriptor(RateLimiters.For.SET_BACKUP_ID)).thenReturn(limiter);
    return limiters;
  }

  private static RateLimiters denyRateLimiter(final UUID aci) {
    final RateLimiters limiters = mock(RateLimiters.class);
    final RateLimiter limiter = mock(RateLimiter.class);
    when(limiter.validateAsync(aci))
        .thenReturn(CompletableFuture.failedFuture(new RateLimitExceededException(null)));
    when(limiters.forDescriptor(RateLimiters.For.SET_BACKUP_ID)).thenReturn(limiter);
    return limiters;
  }
}
