/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.integration;

import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.signal.integration.config.Config;
import org.whispersystems.textsecuregcm.metrics.NoopAwsSdkMetricPublisher;
import org.whispersystems.textsecuregcm.registration.VerificationSession;
import org.whispersystems.textsecuregcm.storage.ChangeNumberWaitingPeriods;
import org.whispersystems.textsecuregcm.storage.PhoneNumberIdentifiers;
import org.whispersystems.textsecuregcm.storage.RegistrationRecoveryPasswords;
import org.whispersystems.textsecuregcm.storage.RegistrationRecoveryPasswordsManager;
import org.whispersystems.textsecuregcm.storage.VerificationSessionManager;
import org.whispersystems.textsecuregcm.storage.VerificationSessions;
import org.whispersystems.textsecuregcm.util.Util;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class IntegrationTools {

  private final RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager;

  private final VerificationSessionManager verificationSessionManager;

  private final PhoneNumberIdentifiers phoneNumberIdentifiers;

  private final ChangeNumberWaitingPeriods changeNumberWaitingPeriods;

  public static IntegrationTools create(final Config config) {
    final AwsCredentialsProvider credentialsProvider = DefaultCredentialsProvider.builder().build();

    final DynamoDbAsyncClient dynamoDbAsyncClient =
        config.dynamoDbClient().buildAsyncClient(credentialsProvider, new NoopAwsSdkMetricPublisher());

    final DynamoDbClient dynamoDbClient =
        config.dynamoDbClient().buildSyncClient(credentialsProvider, new NoopAwsSdkMetricPublisher());

    final RegistrationRecoveryPasswords registrationRecoveryPasswords = new RegistrationRecoveryPasswords(
        config.dynamoDbTables().registrationRecovery(), Duration.ofDays(1), dynamoDbAsyncClient, Clock.systemUTC());

    final VerificationSessions verificationSessions = new VerificationSessions(
        dynamoDbClient, config.dynamoDbTables().verificationSessions(), Clock.systemUTC());

    return new IntegrationTools(
        new RegistrationRecoveryPasswordsManager(registrationRecoveryPasswords),
        new VerificationSessionManager(verificationSessions),
        new PhoneNumberIdentifiers(dynamoDbAsyncClient, config.dynamoDbTables().phoneNumberIdentifiers()),
        new ChangeNumberWaitingPeriods(config.dynamoDbTables().changeNumberWaitingPeriods(), dynamoDbClient)
    );
  }

  private IntegrationTools(
      final RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager,
      final VerificationSessionManager verificationSessionManager,
      final PhoneNumberIdentifiers phoneNumberIdentifiers,
      final ChangeNumberWaitingPeriods changeNumberWaitingPeriods) {
    this.registrationRecoveryPasswordsManager = registrationRecoveryPasswordsManager;
    this.verificationSessionManager = verificationSessionManager;
    this.phoneNumberIdentifiers = phoneNumberIdentifiers;
    this.changeNumberWaitingPeriods = changeNumberWaitingPeriods;
  }

  public CompletableFuture<Void> populateRecoveryPassword(final String phoneNumber, final byte[] password) {
    return phoneNumberIdentifiers
        .getPhoneNumberIdentifier(phoneNumber)
        .thenCompose(pni -> registrationRecoveryPasswordsManager.store(pni, password))
        .thenRun(Util.NOOP);
  }

  public Optional<String> peekVerificationSessionPushChallenge(final String sessionId) {
    return verificationSessionManager.findForId(sessionId).map(VerificationSession::pushChallenge);
  }

  public void clearChangeNumberWaitingPeriod(TestUser user) {
    changeNumberWaitingPeriods.delete(user.aciUuid());
  }
}
