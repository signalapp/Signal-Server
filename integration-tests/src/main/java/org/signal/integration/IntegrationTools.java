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
import org.whispersystems.textsecuregcm.storage.RegistrationRecoveryPasswords;
import org.whispersystems.textsecuregcm.storage.RegistrationRecoveryPasswordsManager;
import org.whispersystems.textsecuregcm.storage.VerificationSessionManager;
import org.whispersystems.textsecuregcm.storage.VerificationSessions;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class IntegrationTools {

  private final RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager;

  private final VerificationSessionManager verificationSessionManager;


  public static IntegrationTools create(final Config config) {
    final AwsCredentialsProvider credentialsProvider = DefaultCredentialsProvider.builder().build();

    final DynamoDbAsyncClient dynamoDbAsyncClient =
        config.dynamoDbClient().buildAsyncClient(credentialsProvider, new NoopAwsSdkMetricPublisher());

    final DynamoDbClient dynamoDbClient =
        config.dynamoDbClient().buildSyncClient(credentialsProvider, new NoopAwsSdkMetricPublisher());

    final RegistrationRecoveryPasswords registrationRecoveryPasswords = new RegistrationRecoveryPasswords(
        config.dynamoDbTables().registrationRecovery(), Duration.ofDays(1), dynamoDbClient, dynamoDbAsyncClient);

    final VerificationSessions verificationSessions = new VerificationSessions(
        dynamoDbAsyncClient, config.dynamoDbTables().verificationSessions(), Clock.systemUTC());

    return new IntegrationTools(
        new RegistrationRecoveryPasswordsManager(registrationRecoveryPasswords),
        new VerificationSessionManager(verificationSessions)
    );
  }

  private IntegrationTools(
      final RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager,
      final VerificationSessionManager verificationSessionManager) {
    this.registrationRecoveryPasswordsManager = registrationRecoveryPasswordsManager;
    this.verificationSessionManager = verificationSessionManager;
  }

  public CompletableFuture<Void> populateRecoveryPassword(final String e164, final byte[] password) {
    return registrationRecoveryPasswordsManager.storeForCurrentNumber(e164, password);
  }

  public CompletableFuture<Optional<String>> peekVerificationSessionPushChallenge(final String sessionId) {
    return verificationSessionManager.findForId(sessionId)
        .thenApply(maybeSession -> maybeSession.map(VerificationSession::pushChallenge));
  }
}
