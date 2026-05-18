/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.integration;

import io.lettuce.core.resource.ClientResources;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.signal.integration.config.Config;
import org.whispersystems.textsecuregcm.metrics.NoopAwsSdkMetricPublisher;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;
import org.whispersystems.textsecuregcm.registration.VerificationSession;
import org.whispersystems.textsecuregcm.storage.ChangeNumberWaitingPeriodManager;
import org.whispersystems.textsecuregcm.storage.PhoneNumberIdentifiers;
import org.whispersystems.textsecuregcm.storage.RegistrationRecoveryPasswords;
import org.whispersystems.textsecuregcm.storage.RegistrationRecoveryPasswordsManager;
import org.whispersystems.textsecuregcm.storage.VerificationSessionManager;
import org.whispersystems.textsecuregcm.storage.VerificationSessions;
import org.whispersystems.textsecuregcm.util.Util;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

public class IntegrationTools {

  private final RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager;

  private final VerificationSessionManager verificationSessionManager;

  private final PhoneNumberIdentifiers phoneNumberIdentifiers;

  private final ChangeNumberWaitingPeriodManager changeNumberWaitingPeriodManager;

  public static IntegrationTools create(final Config config) {
    final AwsCredentialsProvider credentialsProvider = DefaultCredentialsProvider.builder().build();

    final DynamoDbAsyncClient dynamoDbAsyncClient =
        config.dynamoDbClient().buildAsyncClient(credentialsProvider, new NoopAwsSdkMetricPublisher());

    final RegistrationRecoveryPasswords registrationRecoveryPasswords = new RegistrationRecoveryPasswords(
        config.dynamoDbTables().registrationRecovery(), Duration.ofDays(1), dynamoDbAsyncClient, Clock.systemUTC());

    final VerificationSessions verificationSessions = new VerificationSessions(
        dynamoDbAsyncClient, config.dynamoDbTables().verificationSessions(), Clock.systemUTC());

    final FaultTolerantRedisClusterClient rateLimitersClient = new FaultTolerantRedisClusterClient(
        "rateLimiters",
        config.redis().rateLimiters(),
        ClientResources.builder());

    return new IntegrationTools(
        new RegistrationRecoveryPasswordsManager(registrationRecoveryPasswords),
        new VerificationSessionManager(verificationSessions),
        new PhoneNumberIdentifiers(dynamoDbAsyncClient, config.dynamoDbTables().phoneNumberIdentifiers()),
        new ChangeNumberWaitingPeriodManager(rateLimitersClient, Duration.ZERO)
    );
  }

  private IntegrationTools(
      final RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager,
      final VerificationSessionManager verificationSessionManager,
      final PhoneNumberIdentifiers phoneNumberIdentifiers,
      final ChangeNumberWaitingPeriodManager changeNumberWaitingPeriodManager) {
    this.registrationRecoveryPasswordsManager = registrationRecoveryPasswordsManager;
    this.verificationSessionManager = verificationSessionManager;
    this.phoneNumberIdentifiers = phoneNumberIdentifiers;
    this.changeNumberWaitingPeriodManager = changeNumberWaitingPeriodManager;
  }

  public CompletableFuture<Void> populateRecoveryPassword(final String phoneNumber, final byte[] password) {
    return phoneNumberIdentifiers
        .getPhoneNumberIdentifier(phoneNumber)
        .thenCompose(pni -> registrationRecoveryPasswordsManager.store(pni, password))
        .thenRun(Util.NOOP);
  }

  public CompletableFuture<Optional<String>> peekVerificationSessionPushChallenge(final String sessionId) {
    return verificationSessionManager.findForId(sessionId)
        .thenApply(maybeSession -> maybeSession.map(VerificationSession::pushChallenge));
  }

  public void clearChangeNumberWaitingPeriod(TestUser user) {
    try {
      changeNumberWaitingPeriodManager.handleAccountCreated(user.aciUuid(), Instant.now().minus(Duration.ofDays(1)))
          .get(5, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }
}
