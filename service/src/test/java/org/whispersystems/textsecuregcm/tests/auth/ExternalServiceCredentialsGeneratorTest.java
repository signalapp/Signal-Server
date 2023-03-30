/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.util.MockUtils;
import org.whispersystems.textsecuregcm.util.MutableClock;

class ExternalServiceCredentialsGeneratorTest {

  private static final String E164 = "+14152222222";

  private static final long TIME_SECONDS = 12345;

  private static final long TIME_MILLIS = TimeUnit.SECONDS.toMillis(TIME_SECONDS);

  private static final String TIME_SECONDS_STRING = Long.toString(TIME_SECONDS);


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
    final ExternalServiceCredentialsGenerator generator = ExternalServiceCredentialsGenerator
        .builder(new byte[32])
        .build();
    final ExternalServiceCredentials credentials = generator.generateFor(E164);
    assertEquals(credentials.username(), E164);
    assertTrue(credentials.password().startsWith(E164));
    assertEquals(credentials.password().split(":").length, 3);
  }

  @Test
  public void testNotPrependUsername() throws Exception {
    final MutableClock clock = MockUtils.mutableClock(TIME_MILLIS);
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
  public void testValidateValid() throws Exception {
    final MutableClock clock = MockUtils.mutableClock(TIME_MILLIS);
    final ExternalServiceCredentialsGenerator generator = ExternalServiceCredentialsGenerator
        .builder(new byte[32])
        .withClock(clock)
        .build();
    final ExternalServiceCredentials credentials = generator.generateFor(E164);
    assertEquals(generator.validateAndGetTimestamp(credentials).orElseThrow(), TIME_SECONDS);
  }

  @Test
  public void testValidateInvalid() throws Exception {
    final MutableClock clock = MockUtils.mutableClock(TIME_MILLIS);
    final ExternalServiceCredentialsGenerator generator = ExternalServiceCredentialsGenerator
        .builder(new byte[32])
        .withClock(clock)
        .build();
    final ExternalServiceCredentials credentials = generator.generateFor(E164);

    final ExternalServiceCredentials corruptedUsername = new ExternalServiceCredentials(
        credentials.username(), credentials.password().replace(E164, E164 + "0"));
    final ExternalServiceCredentials corruptedTimestamp = new ExternalServiceCredentials(
        credentials.username(), credentials.password().replace(TIME_SECONDS_STRING, TIME_SECONDS_STRING + "0"));
    final ExternalServiceCredentials corruptedPassword = new ExternalServiceCredentials(
        credentials.username(), credentials.password() + "0");

    assertTrue(generator.validateAndGetTimestamp(corruptedUsername).isEmpty());
    assertTrue(generator.validateAndGetTimestamp(corruptedTimestamp).isEmpty());
    assertTrue(generator.validateAndGetTimestamp(corruptedPassword).isEmpty());
  }

  @Test
  public void testValidateWithExpiration() throws Exception {
    final MutableClock clock = MockUtils.mutableClock(TIME_MILLIS);
    final ExternalServiceCredentialsGenerator generator = ExternalServiceCredentialsGenerator
        .builder(new byte[32])
        .withClock(clock)
        .build();
    final ExternalServiceCredentials credentials = generator.generateFor(E164);

    final long elapsedSeconds = 10000;
    clock.incrementSeconds(elapsedSeconds);

    assertEquals(generator.validateAndGetTimestamp(credentials, elapsedSeconds + 1).orElseThrow(), TIME_SECONDS);
    assertTrue(generator.validateAndGetTimestamp(credentials, elapsedSeconds - 1).isEmpty());
  }

  @Test
  public void testTruncateLength() throws Exception {
    final ExternalServiceCredentialsGenerator generator = ExternalServiceCredentialsGenerator.builder(new byte[32])
            .withUserDerivationKey(new byte[32])
            .withTruncateLength(14)
            .build();
    final ExternalServiceCredentials creds = generator.generateFor(E164);
    assertEquals(14*2 /* 2 chars per byte, because hex */, creds.username().length());
    assertEquals("805b84df7eff1e8fe1baf0c6e838", creds.username());
  }
}
