/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.Phonenumber;
import io.dropwizard.core.setup.Environment;
import jakarta.validation.constraints.NotNull;
import java.io.IOException;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretBytes;
import org.whispersystems.textsecuregcm.entities.RegistrationServiceSession;
import org.whispersystems.textsecuregcm.registration.ClientType;
import org.whispersystems.textsecuregcm.registration.MessageTransport;
import org.whispersystems.textsecuregcm.registration.RegistrationServiceClient;

@JsonTypeName("stub")
public class StubRegistrationServiceClientFactory implements RegistrationServiceClientFactory {

  @JsonProperty
  @NotNull
  private String registrationCaCertificate;

  @JsonProperty
  @NotNull
  private SecretBytes collationKeySalt;

  @Override
  public RegistrationServiceClient build(final Environment environment, final Executor callbackExecutor,
      final ScheduledExecutorService identityRefreshExecutor) {

    try {
      return new StubRegistrationServiceClient(registrationCaCertificate, collationKeySalt.value());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static class StubRegistrationServiceClient extends RegistrationServiceClient {

    private final static Map<String, RegistrationServiceSession> SESSIONS = new ConcurrentHashMap<>();

    public StubRegistrationServiceClient(final String registrationCaCertificate, final byte[] collationKeySalt) throws IOException {
      super("example.com", 8080, null, registrationCaCertificate,  collationKeySalt, null);
    }

    @Override
    public CompletableFuture<RegistrationServiceSession> createRegistrationSession(
        final Phonenumber.PhoneNumber phoneNumber, final String sourceHost, final boolean accountExistsWithPhoneNumber, final Duration timeout) {

      final String e164 = PhoneNumberUtil.getInstance()
          .format(phoneNumber, PhoneNumberUtil.PhoneNumberFormat.E164);

      final byte[] id = new byte[32];
      new SecureRandom().nextBytes(id);
      final RegistrationServiceSession session = new RegistrationServiceSession(id, e164, false, 0L, 0L, null,
          Instant.now().plus(Duration.ofMinutes(10)).toEpochMilli());
      SESSIONS.put(Base64.getEncoder().encodeToString(id), session);

      return CompletableFuture.completedFuture(session);
    }

    @Override
    public CompletableFuture<RegistrationServiceSession> sendVerificationCode(final byte[] sessionId,
        final MessageTransport messageTransport, final ClientType clientType, final @Nullable String acceptLanguage,
        final @Nullable String senderOverride, final Duration timeout) {
      return CompletableFuture.completedFuture(SESSIONS.get(Base64.getEncoder().encodeToString(sessionId)));
    }

    @Override
    public CompletableFuture<RegistrationServiceSession> checkVerificationCode(final byte[] sessionId,
        final String verificationCode, final Duration timeout) {
      final RegistrationServiceSession session = SESSIONS.get(Base64.getEncoder().encodeToString(sessionId));

      final RegistrationServiceSession updatedSession = new RegistrationServiceSession(sessionId, session.number(),
          true, 0L, 0L, 0L,
          Instant.now().plus(Duration.ofMinutes(10)).toEpochMilli());

      SESSIONS.put(Base64.getEncoder().encodeToString(sessionId), updatedSession);
      return CompletableFuture.completedFuture(updatedSession);
    }

    @Override
    public CompletableFuture<Optional<RegistrationServiceSession>> getSession(final byte[] sessionId,
        final Duration timeout) {
      return CompletableFuture.completedFuture(
          Optional.ofNullable(SESSIONS.get(Base64.getEncoder().encodeToString(sessionId))));
    }
  }

}
