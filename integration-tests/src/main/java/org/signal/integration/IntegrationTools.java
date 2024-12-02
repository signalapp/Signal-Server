/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.integration;

import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.signal.integration.config.Config;
import org.whispersystems.textsecuregcm.metrics.NoopAwsSdkMetricPublisher;
import org.whispersystems.textsecuregcm.registration.VerificationSession;
import org.whispersystems.textsecuregcm.storage.PhoneNumberIdentifiers;
import org.whispersystems.textsecuregcm.storage.RegistrationRecoveryPasswords;
import org.whispersystems.textsecuregcm.storage.RegistrationRecoveryPasswordsManager;
import org.whispersystems.textsecuregcm.storage.VerificationSessionManager;
import org.whispersystems.textsecuregcm.storage.VerificationSessions;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

public class IntegrationTools {

  private final RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager;

  private final VerificationSessionManager verificationSessionManager;

  private final PhoneNumberIdentifiers phoneNumberIdentifiers;


  public static IntegrationTools create(final Config config) {
    final AwsCredentialsProvider credentialsProvider = DefaultCredentialsProvider.builder().build();

    final DynamoDbAsyncClient dynamoDbAsyncClient =
        config.dynamoDbClient().buildAsyncClient(credentialsProvider, new NoopAwsSdkMetricPublisher());

    final RegistrationRecoveryPasswords registrationRecoveryPasswords = new RegistrationRecoveryPasswords(
        config.dynamoDbTables().registrationRecovery(), Duration.ofDays(1), dynamoDbAsyncClient, Clock.systemUTC());

    final VerificationSessions verificationSessions = new VerificationSessions(
        dynamoDbAsyncClient, config.dynamoDbTables().verificationSessions(), Clock.systemUTC());

    return new IntegrationTools(
        new RegistrationRecoveryPasswordsManager(registrationRecoveryPasswords),
        new VerificationSessionManager(verificationSessions),
        new PhoneNumberIdentifiers(dynamoDbAsyncClient, config.dynamoDbTables().phoneNumberIdentifiers())
    );
  }

  private IntegrationTools(
      final RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager,
      final VerificationSessionManager verificationSessionManager,
      final PhoneNumberIdentifiers phoneNumberIdentifiers) {
    this.registrationRecoveryPasswordsManager = registrationRecoveryPasswordsManager;
    this.verificationSessionManager = verificationSessionManager;
    this.phoneNumberIdentifiers = phoneNumberIdentifiers;
  }

  public CompletableFuture<Void> populateRecoveryPassword(final String phoneNumber, final byte[] password) {
    return phoneNumberIdentifiers
        .getPhoneNumberIdentifier(phoneNumber)
        .thenCompose(pni -> registrationRecoveryPasswordsManager.store(pni, password));
  }

  public CompletableFuture<Optional<String>> peekVerificationSessionPushChallenge(final String sessionId) {
    return verificationSessionManager.findForId(sessionId)
        .thenApply(maybeSession -> maybeSession.map(VerificationSession::pushChallenge));
  }
}
