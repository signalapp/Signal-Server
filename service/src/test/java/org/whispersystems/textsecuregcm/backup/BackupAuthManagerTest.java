/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.backup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatException;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
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
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ThrowableAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.mockito.ArgumentCaptor;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.ServerSecretParams;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.backups.BackupAuthCredentialRequest;
import org.signal.libsignal.zkgroup.backups.BackupAuthCredentialRequestContext;
import org.signal.libsignal.zkgroup.backups.BackupCredentialType;
import org.signal.libsignal.zkgroup.backups.BackupLevel;
import org.signal.libsignal.zkgroup.receipts.ClientZkReceiptOperations;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredential;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialPresentation;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialRequestContext;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialResponse;
import org.signal.libsignal.zkgroup.receipts.ReceiptSerial;
import org.signal.libsignal.zkgroup.receipts.ServerZkReceiptOperations;
import org.whispersystems.textsecuregcm.auth.RedemptionRange;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiterConfig;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.RedeemedReceiptsManager;
import org.whispersystems.textsecuregcm.tests.util.ExperimentHelper;
import org.whispersystems.textsecuregcm.util.TestClock;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

public class BackupAuthManagerTest {

  private static final Instant NOW = Instant.now();

  private final UUID aci = UUID.randomUUID();
  private final byte[] messagesBackupKey = TestRandomUtil.nextBytes(32);
  private final byte[] mediaBackupKey = TestRandomUtil.nextBytes(32);
  private final ServerSecretParams receiptParams = ServerSecretParams.generate();
  private final TestClock clock = TestClock.pinned(NOW);
  private final BackupAuthTestUtil backupAuthTestUtil = new BackupAuthTestUtil(clock);
  private final AccountsManager accountsManager = mock(AccountsManager.class);
  private final RedeemedReceiptsManager redeemedReceiptsManager = mock(RedeemedReceiptsManager.class);

  @BeforeEach
  void setUp() {
    clock.pin(NOW);
    reset(accountsManager);
    reset(redeemedReceiptsManager);
  }

  BackupAuthManager create() {
    return create(BackupLevel.FREE, rateLimiter(aci, false, false));
  }

  BackupAuthManager create(BackupLevel defaultBackupLevel, RateLimiters rateLimiters) {
    return new BackupAuthManager(
        switch (defaultBackupLevel) {
          case FREE -> mock(ExperimentEnrollmentManager.class);
          case PAID -> ExperimentHelper.withEnrollment(BackupAuthManager.BACKUP_MEDIA_EXPERIMENT_NAME, aci);
        },
        rateLimiters,
        accountsManager,
        new ServerZkReceiptOperations(receiptParams),
        redeemedReceiptsManager,
        backupAuthTestUtil.params,
        clock);
  }

  @Test
  void commitBackupId() throws RateLimitExceededException {
    final BackupAuthManager authManager = create();

    final Account account = mock(Account.class);
    when(account.getUuid()).thenReturn(aci);
    when(accountsManager.update(any(), any()))
        .thenAnswer(invocation -> {
          final Account a = invocation.getArgument(0);
          final Consumer<Account> updater = invocation.getArgument(1);

          updater.accept(a);
          return a;
        });

    final BackupAuthCredentialRequest messagesCredentialRequest = backupAuthTestUtil.getRequest(messagesBackupKey, aci);
    final BackupAuthCredentialRequest mediaCredentialRequest = backupAuthTestUtil.getRequest(mediaBackupKey, aci);

    authManager.commitBackupId(account, primaryDevice(),
        Optional.of(messagesCredentialRequest),
        Optional.of(mediaCredentialRequest));

    verify(account).setBackupCredentialRequests(messagesCredentialRequest.serialize(),
        mediaCredentialRequest.serialize());
  }

  @ParameterizedTest
  @EnumSource
  void commitOnAnyBackupLevel(final BackupLevel backupLevel) {
    final BackupAuthManager authManager = create();
    final Account account = new MockAccountBuilder().backupLevel(backupLevel).build();
    when(accountsManager.update(any(), any())).thenReturn(account);

    final ThrowableAssert.ThrowingCallable commit = () ->
        authManager.commitBackupId(account,
            primaryDevice(),
            Optional.of(backupAuthTestUtil.getRequest(messagesBackupKey, aci)),
            Optional.of(backupAuthTestUtil.getRequest(mediaBackupKey, aci)));
    Assertions.assertThatNoException().isThrownBy(commit);
  }

  @Test
  void commitRequiresPrimary() {
    final BackupAuthManager authManager = create();
    final Account account = new MockAccountBuilder().build();
    when(accountsManager.update(any(), any())).thenReturn(account);

    final ThrowableAssert.ThrowingCallable commit = () ->
        authManager.commitBackupId(account,
            linkedDevice(),
            Optional.of(backupAuthTestUtil.getRequest(messagesBackupKey, aci)),
            Optional.of(backupAuthTestUtil.getRequest(mediaBackupKey, aci)));
    assertThatExceptionOfType(StatusRuntimeException.class)
        .isThrownBy(commit)
        .extracting(ex -> ex.getStatus().getCode())
        .isEqualTo(Status.Code.PERMISSION_DENIED);
  }

  @CartesianTest
  void paidTierCredentialViaConfiguration(@CartesianTest.Enum final BackupCredentialType credentialType)
      throws VerificationFailedException {
    final BackupAuthManager authManager = create(BackupLevel.PAID, rateLimiter(aci, false, false));

    final byte[] backupKey = switch (credentialType) {
      case MESSAGES -> messagesBackupKey;
      case MEDIA -> mediaBackupKey;
    };

    // Account does not have PAID tier set
    final Account account = new MockAccountBuilder()
        .messagesCredential(backupAuthTestUtil.getRequest(messagesBackupKey, aci))
        .mediaCredential(backupAuthTestUtil.getRequest(mediaBackupKey, aci))
        .build();

    final BackupAuthCredentialRequestContext requestContext =
        BackupAuthCredentialRequestContext.create(backupKey, aci);

    final RedemptionRange range = range(Duration.ofDays(1));
    final List<BackupAuthManager.Credential> creds =
        authManager.getBackupAuthCredentials(account, credentialType, range(Duration.ofDays(1)));

    assertThat(creds).hasSize(2);
    assertThat(requestContext
        .receiveResponse(creds.getFirst().credential(), range.iterator().next(), backupAuthTestUtil.params.getPublicParams())
        .getBackupLevel())
        .isEqualTo(BackupLevel.PAID);
  }

  @CartesianTest
  void getBackupAuthCredentials(@CartesianTest.Enum final BackupLevel backupLevel,
      @CartesianTest.Enum final BackupCredentialType credentialType) {

    final BackupAuthManager authManager = create();

    final Account account = new MockAccountBuilder()
        .backupLevel(backupLevel)
        .messagesCredential(backupAuthTestUtil.getRequest(messagesBackupKey, aci))
        .mediaCredential(backupAuthTestUtil.getRequest(mediaBackupKey, aci))
        .build();

    assertThat(authManager.getBackupAuthCredentials(account, credentialType, range(Duration.ofDays(1))))
        .hasSize(2);
  }

  @ParameterizedTest
  @EnumSource
  void getBackupAuthCredentialsNoCommittedId(final BackupCredentialType credentialType) {
    final BackupAuthManager authManager = create();

    final Account account = new MockAccountBuilder().build();

    assertThatExceptionOfType(StatusRuntimeException.class)
        .isThrownBy(() ->
            authManager.getBackupAuthCredentials(account, credentialType, range(Duration.ofDays(1))))
        .extracting(ex -> ex.getStatus().getCode())
        .isEqualTo(Status.Code.NOT_FOUND);
  }

  @CartesianTest
  void getReceiptCredentials(@CartesianTest.Enum final BackupLevel backupLevel,
      @CartesianTest.Enum final BackupCredentialType credentialType) throws VerificationFailedException {
    final BackupAuthManager authManager = create();

    final byte[] backupKey = switch (credentialType) {
      case MESSAGES -> messagesBackupKey;
      case MEDIA -> mediaBackupKey;
    };

    final BackupAuthCredentialRequestContext requestContext =
        BackupAuthCredentialRequestContext.create(backupKey, aci);

    final Account account = new MockAccountBuilder()
        .backupLevel(backupLevel)
        .mediaCredential(backupAuthTestUtil.getRequest(mediaBackupKey, aci))
        .messagesCredential(backupAuthTestUtil.getRequest(messagesBackupKey, aci))
        .build();

    final List<BackupAuthManager.Credential> creds = authManager.getBackupAuthCredentials(account,
        credentialType, range(Duration.ofDays(7)));

    assertThat(creds).hasSize(8);
    Instant redemptionTime = clock.instant().truncatedTo(ChronoUnit.DAYS);
    for (BackupAuthManager.Credential cred : creds) {
      assertThat(requestContext
          .receiveResponse(cred.credential(), redemptionTime, backupAuthTestUtil.params.getPublicParams())
          .getBackupLevel())
          .isEqualTo(backupLevel);
      assertThat(cred.redemptionTime().getEpochSecond()).isEqualTo(redemptionTime.getEpochSecond());
      redemptionTime = redemptionTime.plus(Duration.ofDays(1));
    }
  }

  @Test
  void expiringBackupPayment() throws VerificationFailedException {
    clock.pin(Instant.ofEpochSecond(1));
    final Instant day4 = Instant.EPOCH.plus(Duration.ofDays(4));

    final BackupAuthManager authManager = create();

    final Account account = new MockAccountBuilder()
        .messagesCredential(backupAuthTestUtil.getRequest(messagesBackupKey, aci))
        .mediaCredential(backupAuthTestUtil.getRequest(mediaBackupKey, aci))
        .backupVoucher(new Account.BackupVoucher(201, day4))
        .build();

    final List<BackupAuthManager.Credential> creds = authManager.getBackupAuthCredentials(
            account,
            BackupCredentialType.MESSAGES,
            range(RedemptionRange.MAX_REDEMPTION_DURATION));
    Instant redemptionTime = Instant.EPOCH;
    final BackupAuthCredentialRequestContext requestContext = BackupAuthCredentialRequestContext.create(
        messagesBackupKey, aci);
    for (int i = 0; i < creds.size(); i++) {
      // Before the expiration, credentials should have a media receipt, otherwise messages only
      final BackupLevel level = i < 5 ? BackupLevel.PAID : BackupLevel.FREE;
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

    final BackupAuthManager authManager = create();
    final Account account = new MockAccountBuilder()
        .messagesCredential(backupAuthTestUtil.getRequest(messagesBackupKey, aci))
        .mediaCredential(backupAuthTestUtil.getRequest(mediaBackupKey, aci))
        .backupVoucher(new Account.BackupVoucher(3, day1))
        .build();

    final Account updated = new MockAccountBuilder()
        .messagesCredential(backupAuthTestUtil.getRequest(messagesBackupKey, aci))
        .mediaCredential(backupAuthTestUtil.getRequest(mediaBackupKey, aci))
        .backupVoucher(null)
        .build();

    when(accountsManager.update(any(), any())).thenReturn(updated);

    clock.pin(day2.plus(Duration.ofSeconds(1)));
    assertThat(authManager.getBackupAuthCredentials(account, BackupCredentialType.MESSAGES, range(Duration.ofDays(7))))
        .hasSize(8);

    @SuppressWarnings("unchecked") final ArgumentCaptor<Consumer<Account>> accountUpdater = ArgumentCaptor.forClass(
        Consumer.class);
    verify(accountsManager, times(1)).update(any(), accountUpdater.capture());

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
    final BackupAuthManager authManager = create();
    final Account account = new MockAccountBuilder()
        .mediaCredential(Optional.of(new byte[0]))
        .build();
    clock.pin(Instant.EPOCH.plus(Duration.ofDays(1)));
    when(accountsManager.update(any(), any())).thenReturn(account);
    when(redeemedReceiptsManager.put(any(), eq(expirationTime.getEpochSecond()), eq(201L), eq(aci)))
        .thenReturn(CompletableFuture.completedFuture(true));
    authManager.redeemReceipt(account, receiptPresentation(201, expirationTime));
    verify(accountsManager, times(1)).update(any(), any());
  }

  @Test
  void redeemReceiptNoBackupRequest() {
    final Instant expirationTime = Instant.EPOCH.plus(Duration.ofDays(1));
    final BackupAuthManager authManager = create();
    final Account account = new MockAccountBuilder().mediaCredential(Optional.empty()).build();

    clock.pin(Instant.EPOCH.plus(Duration.ofDays(1)));
    when(redeemedReceiptsManager.put(any(), eq(expirationTime.getEpochSecond()), eq(201L), eq(aci)))
        .thenReturn(CompletableFuture.completedFuture(true));
    assertThatExceptionOfType(StatusRuntimeException.class)
        .isThrownBy(() ->
            authManager.redeemReceipt(account, receiptPresentation(201, expirationTime)))
        .extracting(ex -> ex.getStatus().getCode())
        .isEqualTo(Status.Code.ABORTED);
  }

  @Test
  void mergeRedemptions() throws InvalidInputException, VerificationFailedException {
    final Instant newExpirationTime = Instant.EPOCH.plus(Duration.ofDays(1));
    final Instant existingExpirationTime = Instant.EPOCH.plus(Duration.ofDays(1)).plus(Duration.ofSeconds(1));

    final BackupAuthManager authManager = create();
    final Account account = new MockAccountBuilder()
        .mediaCredential(Optional.of(new byte[0]))
        // The account has an existing voucher with a later expiration date
        .backupVoucher(new Account.BackupVoucher(201, existingExpirationTime))
        .build();

    clock.pin(Instant.EPOCH.plus(Duration.ofDays(1)));
    when(accountsManager.update(any(), any())).thenReturn(account);
    when(redeemedReceiptsManager.put(any(), eq(newExpirationTime.getEpochSecond()), eq(201L), eq(aci)))
        .thenReturn(CompletableFuture.completedFuture(true));
    authManager.redeemReceipt(account, receiptPresentation(201, newExpirationTime));

    final ArgumentCaptor<Consumer<Account>> updaterCaptor = ArgumentCaptor.captor();
    verify(accountsManager, times(1)).update(any(), updaterCaptor.capture());

    updaterCaptor.getValue().accept(account);
    // Should select the voucher with the later expiration time
    verify(account).setBackupVoucher(eq(new Account.BackupVoucher(201, existingExpirationTime)));
  }

  @Test
  void redeemExpiredReceipt() {
    final Instant expirationTime = Instant.EPOCH.plus(Duration.ofDays(1));
    clock.pin(expirationTime.plus(Duration.ofSeconds(1)));
    final BackupAuthManager authManager = create();
    assertThatExceptionOfType(StatusRuntimeException.class)
        .isThrownBy(() -> authManager.redeemReceipt(mock(Account.class), receiptPresentation(3, expirationTime)))
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
    final BackupAuthManager authManager = create();
    assertThatExceptionOfType(StatusRuntimeException.class)
        .isThrownBy(() ->
            authManager.redeemReceipt(mock(Account.class), receiptPresentation(level, expirationTime)))
        .extracting(ex -> ex.getStatus().getCode())
        .isEqualTo(Status.Code.INVALID_ARGUMENT);
    verifyNoInteractions(accountsManager);
    verifyNoInteractions(redeemedReceiptsManager);
  }

  @Test
  void redeemInvalidPresentation() throws InvalidInputException, VerificationFailedException {
    final BackupAuthManager authManager = create();
    final ReceiptCredentialPresentation invalid = receiptPresentation(ServerSecretParams.generate(), 3L, Instant.EPOCH);
    assertThatExceptionOfType(StatusRuntimeException.class)
        .isThrownBy(() -> authManager.redeemReceipt(mock(Account.class), invalid))
        .extracting(ex -> ex.getStatus().getCode())
        .isEqualTo(Status.Code.INVALID_ARGUMENT);
    verifyNoInteractions(accountsManager);
    verifyNoInteractions(redeemedReceiptsManager);
  }

  @Test
  void receiptAlreadyRedeemed()  {
    final Instant expirationTime = Instant.EPOCH.plus(Duration.ofDays(1));
    final BackupAuthManager authManager = create();
    final Account account = new MockAccountBuilder()
        .mediaCredential(Optional.of(new byte[0]))
        .build();

    clock.pin(Instant.EPOCH.plus(Duration.ofDays(1)));
    when(accountsManager.update(any(), any())).thenReturn(account);
    when(redeemedReceiptsManager.put(any(), eq(expirationTime.getEpochSecond()), eq(201L), eq(aci)))
        .thenReturn(CompletableFuture.completedFuture(false));

    assertThatExceptionOfType(StatusRuntimeException.class)
        .isThrownBy(() -> authManager.redeemReceipt(account, receiptPresentation(201, expirationTime)))
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

  @CartesianTest
  void testCheckLimits(
      @CartesianTest.Values(booleans = {true, false}) boolean messageLimited,
      @CartesianTest.Values(booleans = {true, false}) boolean mediaLimited,
      @CartesianTest.Values(booleans = {true, false}) boolean hasVoucher) {
    clock.pin(Instant.EPOCH);
    final BackupAuthManager authManager = create(BackupLevel.FREE, rateLimiter(aci, messageLimited, mediaLimited));
    final Account account = new MockAccountBuilder()
        .backupVoucher(hasVoucher
            ? new Account.BackupVoucher(1, Instant.EPOCH.plus(Duration.ofSeconds(1)))
            : null)
        .build();
    final BackupAuthManager.BackupIdRotationLimit limit = authManager.checkBackupIdRotationLimit(account);
    final boolean expectHasPermits = !messageLimited && (!mediaLimited || !hasVoucher);
    final Duration expectedDuration = expectHasPermits ? Duration.ZERO : Duration.ofDays(1);
    assertThat(limit.hasPermitsRemaining()).isEqualTo(expectHasPermits);
    assertThat(limit.nextPermitAvailable()).isEqualTo(expectedDuration);
  }

  enum CredentialChangeType {
    // Provided a new credential that matches the stored credential
    MATCH,
    // Provided a new credential that did not match the stored credential
    MISMATCH,
    // Provided no credential (should not update the credential)
    NO_UPDATE
  }


  @CartesianTest
  void testChangeIdRateLimits(
      @CartesianTest.Enum CredentialChangeType messageChange,
      @CartesianTest.Enum CredentialChangeType mediaChange,
      @CartesianTest.Values(booleans = {true, false}) boolean paid,
      @CartesianTest.Values(booleans = {true, false}) boolean rateLimitMessagesBackupId,
      @CartesianTest.Values(booleans = {true, false}) boolean rateLimitMediaBackupId) {

    final BackupAuthManager authManager =
        create(BackupLevel.FREE, rateLimiter(aci, rateLimitMessagesBackupId, rateLimitMediaBackupId));
    final BackupAuthCredentialRequest storedMessagesCredential = backupAuthTestUtil.getRequest(messagesBackupKey, aci);
    final BackupAuthCredentialRequest storedMediaCredential = backupAuthTestUtil.getRequest(mediaBackupKey, aci);

    // Set clock before the voucher expires if paid, otherwise after
    final Account.BackupVoucher backupVoucher = new Account.BackupVoucher(1, Instant.ofEpochSecond(100));
    clock.pin(paid ? Instant.ofEpochSecond(99) : Instant.ofEpochSecond(101));
    final Account account = new MockAccountBuilder()
        .mediaCredential(storedMediaCredential)
        .messagesCredential(storedMessagesCredential)
        .backupVoucher(backupVoucher)
        .build();

    when(accountsManager.update(any(), any())).thenReturn(account);

    final Optional<BackupAuthCredentialRequest> newMessagesCredential = switch (messageChange) {
      case MATCH -> Optional.of(storedMessagesCredential);
      case MISMATCH -> Optional.of(backupAuthTestUtil.getRequest(TestRandomUtil.nextBytes(32), aci));
      case NO_UPDATE -> Optional.empty();
    };
    final Optional<BackupAuthCredentialRequest> newMediaCredential = switch (mediaChange) {
      case MATCH -> Optional.of(storedMediaCredential);
      case MISMATCH -> Optional.of(backupAuthTestUtil.getRequest(TestRandomUtil.nextBytes(32), aci));
      case NO_UPDATE -> Optional.empty();
    };

    // We should get rate limited if we try to change and
    // 1. we are out of media changes on a paid account, or
    // 2. we are out of messages changes
    final boolean expectRateLimit = ((mediaChange == CredentialChangeType.MISMATCH) && rateLimitMediaBackupId && paid)
        || ((messageChange == CredentialChangeType.MISMATCH) && rateLimitMessagesBackupId);
    final ThrowableAssert.ThrowingCallable commit = () ->
        authManager.commitBackupId(account, primaryDevice(), newMessagesCredential, newMediaCredential);

    if (messageChange == CredentialChangeType.NO_UPDATE && mediaChange == CredentialChangeType.NO_UPDATE) {
      assertThatExceptionOfType(StatusRuntimeException.class)
          .isThrownBy(commit)
          .extracting(ex -> ex.getStatus().getCode())
          .isEqualTo(Status.Code.INVALID_ARGUMENT);
    } else if (expectRateLimit) {
      assertThatExceptionOfType(RateLimitExceededException.class).isThrownBy(commit);
    } else {
      assertThatNoException().isThrownBy(commit);
    }
  }

  private Device primaryDevice() {
    final Device device = mock(Device.class);
    when(device.isPrimary()).thenReturn(true);
    return device;
  }

  private Device linkedDevice() {
    final Device device = mock(Device.class);
    when(device.isPrimary()).thenReturn(false);
    return device;
  }

  private class MockAccountBuilder {

    private final Account account = mock(Account.class);

    MockAccountBuilder() {
      when(account.getUuid()).thenReturn(aci);
    }

    MockAccountBuilder backupLevel(BackupLevel backupLevel) {
      if (backupLevel == BackupLevel.PAID) {
        return backupVoucher(new Account.BackupVoucher(201L, clock.instant().plus(Duration.ofDays(8))));
      }
      return this;
    }

    MockAccountBuilder backupVoucher(Account.BackupVoucher backupVoucher) {
      when(account.getBackupVoucher()).thenReturn(backupVoucher);
      return this;
    }

    MockAccountBuilder mediaCredential(final BackupAuthCredentialRequest storedMediaCredential) {
      return mediaCredential(Optional.of(storedMediaCredential.serialize()));
    }

    MockAccountBuilder mediaCredential(final Optional<byte[]> serializedMediaCredential) {
      when(account.getBackupCredentialRequest(BackupCredentialType.MEDIA))
          .thenReturn(serializedMediaCredential);
      return this;
    }

    MockAccountBuilder messagesCredential(final BackupAuthCredentialRequest storedMessagesCredential) {
      when(account.getBackupCredentialRequest(BackupCredentialType.MESSAGES))
          .thenReturn(Optional.of(storedMessagesCredential.serialize()));
      return this;
    }

    Account build() {
      return account;
    }
  }


  private static RateLimiters rateLimiter(final UUID aci, boolean rateLimitBackupId, boolean rateLimitPaidMediaBackupId) {
    try {
      final RateLimiters limiters = mock(RateLimiters.class);

      final RateLimiter allowLimiter = mock(RateLimiter.class);
      when(allowLimiter.hasAvailablePermitsAsync(eq(aci), anyLong())).thenReturn(
          CompletableFuture.completedFuture(true));
      when(allowLimiter.config()).thenReturn(new RateLimiterConfig(1, Duration.ofDays(1), false));

      final RateLimiter denyLimiter = mock(RateLimiter.class);
      when(denyLimiter.hasAvailablePermitsAsync(eq(aci), anyLong())).thenReturn(
          CompletableFuture.completedFuture(false));
      doThrow(new RateLimitExceededException(null)).when(denyLimiter).validate(aci);
      when(denyLimiter.config()).thenReturn(new RateLimiterConfig(1, Duration.ofDays(1), false));

      when(limiters.forDescriptor(RateLimiters.For.SET_BACKUP_ID))
          .thenReturn(rateLimitBackupId ? denyLimiter : allowLimiter);
      when(limiters.forDescriptor(RateLimiters.For.SET_PAID_MEDIA_BACKUP_ID))
          .thenReturn(rateLimitPaidMediaBackupId ? denyLimiter : allowLimiter);
      return limiters;
    } catch (RateLimitExceededException e) {
      throw new RuntimeException(e);
    }
  }

  private RedemptionRange range(Duration length) {
    final Instant start = clock.instant().truncatedTo(ChronoUnit.DAYS);
    return RedemptionRange.inclusive(clock, start, start.plus(length));
  }
}
