/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.registration.VerificationSession;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema.Tables;
import org.whispersystems.textsecuregcm.telephony.CarrierData;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;

class VerificationSessionsTest {

  private static final Clock clock = Clock.systemUTC();

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(Tables.VERIFICATION_SESSIONS);

  private VerificationSessions verificationSessions;

  @BeforeEach
  void setUp() {
    verificationSessions = new VerificationSessions(
        DYNAMO_DB_EXTENSION.getDynamoDbClient(), Tables.VERIFICATION_SESSIONS.tableName(), clock);
  }

  @Test
  void testExpiration() {
    final Instant created = Instant.now().minusSeconds(60);
    final Instant updates = Instant.now();
    final Duration remoteExpiration = Duration.ofMinutes(2);

    final VerificationSession verificationSession = new VerificationSession("test", null, null,
        List.of(VerificationSession.Information.PUSH_CHALLENGE), Collections.emptyList(), null, null, true,
        created.toEpochMilli(), updates.toEpochMilli(), remoteExpiration.toSeconds());

    assertEquals(updates.plus(remoteExpiration).getEpochSecond(), verificationSession.getExpirationEpochSeconds());
  }

  @Test
  void testStore() {

    assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {

      final String sessionId = "sessionId";

      final Optional<VerificationSession> absentSession = verificationSessions.findForKey(sessionId);
      assertTrue(absentSession.isEmpty());

      final VerificationSession session = new VerificationSession(sessionId, null, new CarrierData("Test", CarrierData.LineType.MOBILE, Optional.of("123"), Optional.empty(), Optional.empty(), Optional.empty()),
          List.of(VerificationSession.Information.PUSH_CHALLENGE), Collections.emptyList(), null, null, true,
          clock.millis(), clock.millis(), Duration.ofMinutes(1).toSeconds());

      verificationSessions.insert(sessionId, session);

      assertEquals(session, verificationSessions.findForKey(sessionId).orElseThrow());

      assertThrows(ConditionalCheckFailedException.class, () -> verificationSessions.insert(sessionId, session),
          "inserting with the same key should fail conditional checks");

      final VerificationSession updatedSession = new VerificationSession(sessionId, null, new CarrierData("Test", CarrierData.LineType.MOBILE, Optional.of("123"), Optional.empty(), Optional.empty(), Optional.empty()), Collections.emptyList(),
          List.of(VerificationSession.Information.PUSH_CHALLENGE), null, null, true, clock.millis(), clock.millis(),
          Duration.ofMinutes(2).toSeconds());
      verificationSessions.update(sessionId, updatedSession);

      assertEquals(updatedSession, verificationSessions.findForKey(sessionId).orElseThrow());
    });
  }

}
