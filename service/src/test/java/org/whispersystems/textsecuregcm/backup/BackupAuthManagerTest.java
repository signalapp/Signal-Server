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
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ThrowableAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
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
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.RedeemedReceiptsManager;
import org.whispersystems.textsecuregcm.tests.util.ExperimentHelper;
import org.whispersystems.textsecuregcm.util.CompletableFutureTestUtil;
import org.whispersystems.textsecuregcm.util.TestClock;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

public class BackupAuthManagerTest {

  private final UUID aci = UUID.randomUUID();
  private final byte[] messagesBackupKey = TestRandomUtil.nextBytes(32);
  private final byte[] mediaBackupKey = TestRandomUtil.nextBytes(32);
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
  void commitBackupId() {
    final BackupAuthManager authManager = create();

    final Account account = mock(Account.class);
    when(account.getUuid()).thenReturn(aci);
    when(accountsManager.updateAsync(any(), any()))
        .thenAnswer(invocation -> {
          final Account a = invocation.getArgument(0);
          final Consumer<Account> updater = invocation.getArgument(1);

          updater.accept(a);

          return CompletableFuture.completedFuture(a);
        });

    final BackupAuthCredentialRequest messagesCredentialRequest = backupAuthTestUtil.getRequest(messagesBackupKey, aci);
    final BackupAuthCredentialRequest mediaCredentialRequest = backupAuthTestUtil.getRequest(mediaBackupKey, aci);

    authManager.commitBackupId(account, primaryDevice(), messagesCredentialRequest, mediaCredentialRequest).join();

    verify(account).setBackupCredentialRequests(messagesCredentialRequest.serialize(),
        mediaCredentialRequest.serialize());
  }

  @ParameterizedTest
  @EnumSource
  void commitOnAnyBackupLevel(final BackupLevel backupLevel) {
    final BackupAuthManager authManager = create();
    final Account account = new MockAccountBuilder().backupLevel(backupLevel).build();
    when(accountsManager.updateAsync(any(), any())).thenReturn(CompletableFuture.completedFuture(account));

    final ThrowableAssert.ThrowingCallable commit = () ->
        authManager.commitBackupId(account,
            primaryDevice(),
            backupAuthTestUtil.getRequest(messagesBackupKey, aci),
            backupAuthTestUtil.getRequest(mediaBackupKey, aci)).join();
    Assertions.assertThatNoException().isThrownBy(commit);
  }

  @Test
  void commitRequiresPrimary() {
    final BackupAuthManager authManager = create();
    final Account account = new MockAccountBuilder().build();
    when(accountsManager.updateAsync(any(), any())).thenReturn(CompletableFuture.completedFuture(account));

    final ThrowableAssert.ThrowingCallable commit = () ->
        authManager.commitBackupId(account,
            linkedDevice(),
            backupAuthTestUtil.getRequest(messagesBackupKey, aci),
            backupAuthTestUtil.getRequest(mediaBackupKey, aci)).join();
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

    final Instant start = clock.instant().truncatedTo(ChronoUnit.DAYS);
    final List<BackupAuthManager.Credential> creds = authManager.getBackupAuthCredentials(account,
        credentialType, start, start.plus(Duration.ofDays(1))).join();

    assertThat(creds).hasSize(2);
    assertThat(requestContext
        .receiveResponse(creds.getFirst().credential(), start, backupAuthTestUtil.params.getPublicParams())
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

    assertThat(authManager.getBackupAuthCredentials(account,
        credentialType,
        clock.instant().truncatedTo(ChronoUnit.DAYS),
        clock.instant().plus(Duration.ofDays(1)).truncatedTo(ChronoUnit.DAYS)).join())
        .hasSize(2);
  }

  @ParameterizedTest
  @EnumSource
  void getBackupAuthCredentialsNoCommittedId(final BackupCredentialType credentialType) {
    final BackupAuthManager authManager = create();

    final Account account = new MockAccountBuilder().build();

    assertThatExceptionOfType(StatusRuntimeException.class)
        .isThrownBy(() -> authManager.getBackupAuthCredentials(account,
            credentialType,
            clock.instant().truncatedTo(ChronoUnit.DAYS),
            clock.instant().plus(Duration.ofDays(1)).truncatedTo(ChronoUnit.DAYS)).join())
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

    final Instant start = clock.instant().truncatedTo(ChronoUnit.DAYS);
    final List<BackupAuthManager.Credential> creds = authManager.getBackupAuthCredentials(account,
        credentialType, start, start.plus(Duration.ofDays(7))).join();

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
    final BackupAuthManager authManager = create();

    final Account account = new MockAccountBuilder()
        .messagesCredential(backupAuthTestUtil.getRequest(messagesBackupKey, aci))
        .mediaCredential(backupAuthTestUtil.getRequest(mediaBackupKey, aci))
        .build();

    clock.pin(now);
    assertThatExceptionOfType(StatusRuntimeException.class)
        .isThrownBy(
            () -> authManager.getBackupAuthCredentials(account, BackupCredentialType.MESSAGES, requestRedemptionStart, requestRedemptionEnd).join())
        .extracting(ex -> ex.getStatus().getCode())
        .isEqualTo(Status.Code.INVALID_ARGUMENT);
  }

  @Test
  void expiringBackupPayment() throws VerificationFailedException {
    clock.pin(Instant.ofEpochSecond(1));
    final Instant day0 = Instant.EPOCH;
    final Instant day4 = Instant.EPOCH.plus(Duration.ofDays(4));
    final Instant dayMax = day0.plus(BackupAuthManager.MAX_REDEMPTION_DURATION);

    final BackupAuthManager authManager = create();

    final Account account = new MockAccountBuilder()
        .messagesCredential(backupAuthTestUtil.getRequest(messagesBackupKey, aci))
        .mediaCredential(backupAuthTestUtil.getRequest(mediaBackupKey, aci))
        .backupVoucher(new Account.BackupVoucher(201, day4))
        .build();

    final List<BackupAuthManager.Credential> creds = authManager.getBackupAuthCredentials(account, BackupCredentialType.MESSAGES, day0, dayMax).join();
    Instant redemptionTime = day0;
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

    when(accountsManager.updateAsync(any(), any())).thenReturn(CompletableFuture.completedFuture(updated));

    clock.pin(day2.plus(Duration.ofSeconds(1)));
    assertThat(authManager.getBackupAuthCredentials(account, BackupCredentialType.MESSAGES, day2, day2.plus(Duration.ofDays(7))).join())
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
    final BackupAuthManager authManager = create();
    final Account account = new MockAccountBuilder()
        .mediaCredential(Optional.of(new byte[0]))
        .build();
    clock.pin(Instant.EPOCH.plus(Duration.ofDays(1)));
    when(accountsManager.updateAsync(any(), any())).thenReturn(CompletableFuture.completedFuture(account));
    when(redeemedReceiptsManager.put(any(), eq(expirationTime.getEpochSecond()), eq(201L), eq(aci)))
        .thenReturn(CompletableFuture.completedFuture(true));
    authManager.redeemReceipt(account, receiptPresentation(201, expirationTime)).join();
    verify(accountsManager, times(1)).updateAsync(any(), any());
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
            authManager.redeemReceipt(account, receiptPresentation(201, expirationTime)).join())
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
    final BackupAuthManager authManager = create();
    assertThatExceptionOfType(StatusRuntimeException.class)
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
    final BackupAuthManager authManager = create();
    assertThatExceptionOfType(StatusRuntimeException.class)
        .isThrownBy(() ->
            authManager.redeemReceipt(mock(Account.class), receiptPresentation(level, expirationTime)).join())
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
        .isThrownBy(() -> authManager.redeemReceipt(mock(Account.class), invalid).join())
        .extracting(ex -> ex.getStatus().getCode())
        .isEqualTo(Status.Code.INVALID_ARGUMENT);
    verifyNoInteractions(accountsManager);
    verifyNoInteractions(redeemedReceiptsManager);
  }

  @Test
  void receiptAlreadyRedeemed() throws InvalidInputException, VerificationFailedException {
    final Instant expirationTime = Instant.EPOCH.plus(Duration.ofDays(1));
    final BackupAuthManager authManager = create();
    final Account account = new MockAccountBuilder()
        .mediaCredential(Optional.of(new byte[0]))
        .build();

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


  @CartesianTest
  void testChangeIdRateLimits(
      @CartesianTest.Values(booleans = {true, false}) boolean changeMessage,
      @CartesianTest.Values(booleans = {true, false}) boolean changeMedia,
      @CartesianTest.Values(booleans = {true, false}) boolean rateLimitBackupId) {

    final BackupAuthManager authManager = create(BackupLevel.FREE, rateLimiter(aci, rateLimitBackupId, false));
    final BackupAuthCredentialRequest storedMessagesCredential = backupAuthTestUtil.getRequest(messagesBackupKey, aci);
    final BackupAuthCredentialRequest storedMediaCredential = backupAuthTestUtil.getRequest(mediaBackupKey, aci);
    final Account account = new MockAccountBuilder()
        .mediaCredential(storedMediaCredential)
        .messagesCredential(storedMessagesCredential)
        .backupVoucher(null)
        .build();
    when(accountsManager.updateAsync(any(), any())).thenReturn(CompletableFuture.completedFuture(account));

    final BackupAuthCredentialRequest newMessagesCredential = changeMessage
        ? backupAuthTestUtil.getRequest(TestRandomUtil.nextBytes(32), aci)
        : storedMessagesCredential;

    final BackupAuthCredentialRequest newMediaCredential = changeMedia
        ? backupAuthTestUtil.getRequest(TestRandomUtil.nextBytes(32), aci)
        : storedMediaCredential;

    final boolean expectRateLimit = (changeMedia || changeMessage) && rateLimitBackupId;
    final CompletableFuture<Void> future = authManager.commitBackupId(account, primaryDevice(), newMessagesCredential, newMediaCredential);
    if (expectRateLimit) {
      CompletableFutureTestUtil.assertFailsWithCause(RateLimitExceededException.class, future);
    } else {
      assertDoesNotThrow(() -> future.join());
    }
  }

  @CartesianTest
  void testChangePaidMediaIdRateLimits(
      @CartesianTest.Values(booleans = {true, false}) boolean changeMessage,
      @CartesianTest.Values(booleans = {true, false}) boolean changeMedia,
      @CartesianTest.Values(booleans = {true, false}) boolean paid,
      @CartesianTest.Values(booleans = {true, false}) boolean rateLimitPaidMedia) {

    final BackupAuthManager authManager = create(BackupLevel.FREE, rateLimiter(aci, false, rateLimitPaidMedia));
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
    when(accountsManager.updateAsync(any(), any())).thenReturn(CompletableFuture.completedFuture(account));

    final BackupAuthCredentialRequest newMessagesCredential = changeMessage
        ? backupAuthTestUtil.getRequest(TestRandomUtil.nextBytes(32), aci)
        : storedMessagesCredential;

    final BackupAuthCredentialRequest newMediaCredential = changeMedia
        ? backupAuthTestUtil.getRequest(TestRandomUtil.nextBytes(32), aci)
        : storedMediaCredential;

    // We should get rate limited iff we are out of paid media changes and we changed the media backup-id
    final boolean expectRateLimit =  changeMedia && paid && rateLimitPaidMedia;
    final CompletableFuture<Void> future = authManager.commitBackupId(account, primaryDevice(), newMessagesCredential, newMediaCredential);
    if (expectRateLimit) {
      CompletableFutureTestUtil.assertFailsWithCause(RateLimitExceededException.class, future);
    } else {
      assertDoesNotThrow(() -> future.join());
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

    private Account account = mock(Account.class);

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


  private static RateLimiters rateLimiter(final UUID aci, boolean rateLimitBackupId,
      boolean rateLimitPaidMediaBackupId) {
    final RateLimiters limiters = mock(RateLimiters.class);

    final RateLimiter allowLimiter = mock(RateLimiter.class);
    when(allowLimiter.validateAsync(aci)).thenReturn(CompletableFuture.completedFuture(null));

    final RateLimiter denyLimiter = mock(RateLimiter.class);
    when(denyLimiter.validateAsync(aci))
        .thenReturn(CompletableFuture.failedFuture(new RateLimitExceededException(null)));

    when(limiters.forDescriptor(RateLimiters.For.SET_BACKUP_ID))
        .thenReturn(rateLimitBackupId ? denyLimiter : allowLimiter);
    when(limiters.forDescriptor(RateLimiters.For.SET_PAID_MEDIA_BACKUP_ID))
        .thenReturn(rateLimitPaidMediaBackupId ? denyLimiter : allowLimiter);
    return limiters;
  }
}
