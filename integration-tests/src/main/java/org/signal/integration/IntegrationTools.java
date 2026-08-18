/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.integration;

import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.signal.integration.config.Config;
import org.signal.libsignal.zkgroup.receipts.ReceiptSerial;
import org.whispersystems.textsecuregcm.metrics.NoopAwsSdkMetricPublisher;
import org.whispersystems.textsecuregcm.registration.VerificationSession;
import org.whispersystems.textsecuregcm.storage.ChangeNumberWaitingPeriods;
import org.whispersystems.textsecuregcm.storage.PhoneNumberIdentifiers;
import org.whispersystems.textsecuregcm.storage.PhoneNumberRecoveryPasswords;
import org.whispersystems.textsecuregcm.storage.PhoneNumberRecoveryPasswordsManager;
import org.whispersystems.textsecuregcm.storage.RedeemedReceiptsManager;
import org.whispersystems.textsecuregcm.storage.VerificationSessionManager;
import org.whispersystems.textsecuregcm.storage.VerificationSessions;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class IntegrationTools {

  private final PhoneNumberRecoveryPasswordsManager phoneNumberRecoveryPasswordsManager;

  private final VerificationSessionManager verificationSessionManager;

  private final PhoneNumberIdentifiers phoneNumberIdentifiers;

  private final ChangeNumberWaitingPeriods changeNumberWaitingPeriods;

  private final RedeemedReceiptsManager redeemedReceiptsManager;

  public static IntegrationTools create(final Config config) {
    final AwsCredentialsProvider credentialsProvider = DefaultCredentialsProvider.builder().build();

    final DynamoDbAsyncClient dynamoDbAsyncClient =
        config.dynamoDbClient().buildAsyncClient(credentialsProvider, new NoopAwsSdkMetricPublisher());

    final DynamoDbClient dynamoDbClient =
        config.dynamoDbClient().buildSyncClient(credentialsProvider, new NoopAwsSdkMetricPublisher());

    final PhoneNumberRecoveryPasswords phoneNumberRecoveryPasswords = new PhoneNumberRecoveryPasswords(
        config.dynamoDbTables().registrationRecovery(), Duration.ofDays(1), dynamoDbClient, Clock.systemUTC());

    final VerificationSessions verificationSessions = new VerificationSessions(
        dynamoDbClient, config.dynamoDbTables().verificationSessions(), Clock.systemUTC());

    return new IntegrationTools(
        new PhoneNumberRecoveryPasswordsManager(phoneNumberRecoveryPasswords),
        new VerificationSessionManager(verificationSessions),
        new PhoneNumberIdentifiers(dynamoDbAsyncClient, config.dynamoDbTables().phoneNumberIdentifiers()),
        new ChangeNumberWaitingPeriods(config.dynamoDbTables().changeNumberWaitingPeriods(), dynamoDbClient),
        new RedeemedReceiptsManager(Clock.systemUTC(), config.dynamoDbTables().redeemedReceipts(), dynamoDbClient)
    );
  }

  private IntegrationTools(
      final PhoneNumberRecoveryPasswordsManager phoneNumberRecoveryPasswordsManager,
      final VerificationSessionManager verificationSessionManager,
      final PhoneNumberIdentifiers phoneNumberIdentifiers,
      final ChangeNumberWaitingPeriods changeNumberWaitingPeriods,
      final RedeemedReceiptsManager redeemedReceiptsManager) {
    this.phoneNumberRecoveryPasswordsManager = phoneNumberRecoveryPasswordsManager;
    this.verificationSessionManager = verificationSessionManager;
    this.phoneNumberIdentifiers = phoneNumberIdentifiers;
    this.changeNumberWaitingPeriods = changeNumberWaitingPeriods;
    this.redeemedReceiptsManager = redeemedReceiptsManager;
  }

  public void populateRecoveryPassword(final String phoneNumber, final byte[] password) {
    try {
      final UUID pni = phoneNumberIdentifiers
          .getPhoneNumberIdentifier(phoneNumber).get(5, TimeUnit.SECONDS);
      phoneNumberRecoveryPasswordsManager.store(pni, password);
    } catch (ExecutionException | InterruptedException | TimeoutException e) {
      throw new RuntimeException("failed to get pni", e);
    }
  }

  public Optional<String> peekVerificationSessionPushChallenge(final String sessionId) {
    return verificationSessionManager.findForId(sessionId).map(VerificationSession::pushChallenge);
  }

  public void clearChangeNumberWaitingPeriod(TestUser user) {
    changeNumberWaitingPeriods.delete(user.aciUuid());
  }

  public void deleteRedeemedReceipt(final ReceiptSerial receiptSerial) {
    redeemedReceiptsManager.deleteReceipt(receiptSerial);
  }
}
