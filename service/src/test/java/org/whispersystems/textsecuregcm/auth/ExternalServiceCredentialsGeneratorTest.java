/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.whispersystems.textsecuregcm.util.HmacUtils.hmac256TruncatedToHexString;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.util.MockUtils;
import org.whispersystems.textsecuregcm.util.MutableClock;

class ExternalServiceCredentialsGeneratorTest {
  private static final String PREFIX = "prefix";

  private static final String E164 = "+14152222222";

  private static final long TIME_SECONDS = 12345;

  private static final long TIME_MILLIS = TimeUnit.SECONDS.toMillis(TIME_SECONDS);

  private static final String TIME_SECONDS_STRING = Long.toString(TIME_SECONDS);

  private static final String USERNAME_TIMESTAMP = PREFIX + ":" + Instant.ofEpochSecond(TIME_SECONDS).truncatedTo(ChronoUnit.DAYS).getEpochSecond();

  private static final MutableClock clock = MockUtils.mutableClock(TIME_MILLIS);

  private static final ExternalServiceCredentialsGenerator standardGenerator = ExternalServiceCredentialsGenerator
      .builder(new byte[32])
      .withClock(clock)
      .build();

  private static final ExternalServiceCredentials standardCredentials = standardGenerator.generateFor(E164);

  private static final ExternalServiceCredentialsGenerator usernameIsTimestampGenerator = ExternalServiceCredentialsGenerator
      .builder(new byte[32])
      .withUsernameTimestampTruncatorAndPrefix(timestamp -> timestamp.truncatedTo(ChronoUnit.DAYS), PREFIX)
      .withClock(clock)
      .build();

  private static final ExternalServiceCredentials usernameIsTimestampCredentials = usernameIsTimestampGenerator.generateWithTimestampAsUsername();

  @BeforeEach
  public void before() throws Exception {
    clock.setTimeMillis(TIME_MILLIS);
  }

  @Test
  void testInvalidConstructor() {
    assertThrows(RuntimeException.class, () -> ExternalServiceCredentialsGenerator
        .builder(new byte[32])
        .withUsernameTimestampTruncatorAndPrefix(null, PREFIX)
        .build());

    assertThrows(RuntimeException.class, () -> ExternalServiceCredentialsGenerator
        .builder(new byte[32])
        .withUsernameTimestampTruncatorAndPrefix(timestamp -> timestamp.truncatedTo(ChronoUnit.DAYS), null)
        .build());
  }

  @Test
  void testGenerateDerivedUsername() {
    final ExternalServiceCredentialsGenerator generator = ExternalServiceCredentialsGenerator
        .builder(new byte[32])
        .withUserDerivationKey(new byte[32])
        .build();
    final ExternalServiceCredentials credentials = generator.generateFor(E164);
    assertNotEquals(credentials.username(), E164);
    assertFalse(credentials.password().startsWith(E164));
    assertEquals(credentials.password().split(":").length, 3);
  }

  @Test
  void testGenerateNoDerivedUsername() {
    assertEquals(standardCredentials.username(), E164);
    assertTrue(standardCredentials.password().startsWith(E164));
    assertEquals(standardCredentials.password().split(":").length, 3);
  }

  @Test
  public void testNotPrependUsername() throws Exception {
    final ExternalServiceCredentialsGenerator generator = ExternalServiceCredentialsGenerator
        .builder(new byte[32])
        .prependUsername(false)
        .withClock(clock)
        .build();
    final ExternalServiceCredentials credentials = generator.generateFor(E164);
    assertEquals(credentials.username(), E164);
    assertTrue(credentials.password().startsWith(TIME_SECONDS_STRING));
    assertEquals(credentials.password().split(":").length, 2);
  }

  @Test
  public void testWithUsernameIsTimestamp() {
    assertEquals(USERNAME_TIMESTAMP, usernameIsTimestampCredentials.username());

    final String[] passwordComponents = usernameIsTimestampCredentials.password().split(":");
    assertEquals(USERNAME_TIMESTAMP, passwordComponents[0] + ":" + passwordComponents[1]);
    assertEquals(hmac256TruncatedToHexString(new byte[32], USERNAME_TIMESTAMP, 10), passwordComponents[2]);
  }

  @Test
  public void testValidateValid() throws Exception {
    assertEquals(standardGenerator.validateAndGetTimestamp(standardCredentials).orElseThrow(), TIME_SECONDS);
  }

  @Test
  public void testValidateValidWithUsernameIsTimestamp() {
    final long expectedTimestamp = Instant.ofEpochSecond(TIME_SECONDS).truncatedTo(ChronoUnit.DAYS).getEpochSecond();
    assertEquals(expectedTimestamp, usernameIsTimestampGenerator.validateAndGetTimestamp(usernameIsTimestampCredentials).orElseThrow());
  }

  @Test
  public void testValidateInvalid() throws Exception {
    final ExternalServiceCredentials corruptedStandardUsername = new ExternalServiceCredentials(
        standardCredentials.username(), standardCredentials.password().replace(E164, E164 + "0"));
    final ExternalServiceCredentials corruptedStandardTimestamp = new ExternalServiceCredentials(
        standardCredentials.username(), standardCredentials.password().replace(TIME_SECONDS_STRING, TIME_SECONDS_STRING + "0"));
    final ExternalServiceCredentials corruptedStandardPassword = new ExternalServiceCredentials(
        standardCredentials.username(), standardCredentials.password() + "0");

    final ExternalServiceCredentials corruptedUsernameTimestamp = new ExternalServiceCredentials(
        usernameIsTimestampCredentials.username(), usernameIsTimestampCredentials.password().replace(USERNAME_TIMESTAMP, USERNAME_TIMESTAMP
        + "0"));
    final ExternalServiceCredentials corruptedUsernameTimestampPassword = new ExternalServiceCredentials(
        usernameIsTimestampCredentials.username(), usernameIsTimestampCredentials.password() + "0");

    assertTrue(standardGenerator.validateAndGetTimestamp(corruptedStandardUsername).isEmpty());
    assertTrue(standardGenerator.validateAndGetTimestamp(corruptedStandardTimestamp).isEmpty());
    assertTrue(standardGenerator.validateAndGetTimestamp(corruptedStandardPassword).isEmpty());

    assertTrue(usernameIsTimestampGenerator.validateAndGetTimestamp(corruptedUsernameTimestamp).isEmpty());
    assertTrue(usernameIsTimestampGenerator.validateAndGetTimestamp(corruptedUsernameTimestampPassword).isEmpty());
  }

  @Test
  public void testValidateWithExpiration() throws Exception {
    final long elapsedSeconds = 10000;
    clock.incrementSeconds(elapsedSeconds);

    assertEquals(standardGenerator.validateAndGetTimestamp(standardCredentials, elapsedSeconds + 1).orElseThrow(), TIME_SECONDS);
    assertTrue(standardGenerator.validateAndGetTimestamp(standardCredentials, elapsedSeconds - 1).isEmpty());
  }

  @Test
  public void testGetIdentityFromSignature() {
    final String identity = standardGenerator.identityFromSignature(standardCredentials.password()).orElseThrow();
    assertEquals(E164, identity);
  }

  @Test
  public void testGetIdentityFromSignatureIsTimestamp() {
    final String identity = usernameIsTimestampGenerator.identityFromSignature(usernameIsTimestampCredentials.password()).orElseThrow();
    assertEquals(USERNAME_TIMESTAMP, identity);
  }

  @Test
  public void testTruncateLength() throws Exception {
    final ExternalServiceCredentialsGenerator generator = ExternalServiceCredentialsGenerator.builder(new byte[32])
            .withUserDerivationKey(new byte[32])
            .withDerivedUsernameTruncateLength(14)
            .build();
    final ExternalServiceCredentials creds = generator.generateFor(E164);
    assertEquals(14*2 /* 2 chars per byte, because hex */, creds.username().length());
    assertEquals("805b84df7eff1e8fe1baf0c6e838", creds.username());
    generator.validateAndGetTimestamp(creds);
  }
}
