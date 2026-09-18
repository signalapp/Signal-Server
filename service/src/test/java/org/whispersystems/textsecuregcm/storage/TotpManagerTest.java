/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nullable;
import javax.crypto.KeyGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

class TotpManagerTest {

  @RegisterExtension
  private static final RedisClusterExtension REDIS_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  private static final Duration MAX_TOTP_VALIDATION_DELAY = TotpManager.TOTP.getTimeStep().dividedBy(2);

  private static final TotpParameters TOTP_PARAMETERS = new TotpParameters(TotpManager.TOTP.getAlgorithm(),
      TotpManager.TOTP.getPasswordLength(),
      TotpManager.TOTP.getTimeStep());

  private TotpManager totpManager;

  @BeforeEach
  void setUp() {
    totpManager = new TotpManager(REDIS_CLUSTER_EXTENSION.getRedisCluster(), MAX_TOTP_VALIDATION_DELAY);
  }

  private static AnnotatedTotpKey generateTotpKey() throws NoSuchAlgorithmException {
    final KeyGenerator totpKeyGenerator = KeyGenerator.getInstance(TotpManager.TOTP.getAlgorithm());
    totpKeyGenerator.init(TotpManager.getTotpKeyLengthBits());

    return new AnnotatedTotpKey(new TotpKey(TOTP_PARAMETERS, totpKeyGenerator.generateKey().getEncoded()),
        TestRandomUtil.nextBytes(16));
  }

  private static Instant totpWindowStart(final Instant instant) {
    return Instant.ofEpochMilli((instant.toEpochMilli() / TotpManager.TOTP.getTimeStep().toMillis()) *
        TotpManager.TOTP.getTimeStep().toMillis());
  }


  @Test
  void checkTotpAcceptsReuse() throws NoSuchAlgorithmException, InvalidKeyException {
    final AnnotatedTotpKey totpKey = generateTotpKey();

    final Instant timestamp = Instant.now();
    final int oneTimePassword = TotpManager.TOTP.generateOneTimePassword(totpKey, timestamp);
    final UUID aci = UUID.randomUUID();

    assertTrue(totpManager.checkTotpMatches(aci, totpKey.totpKey(), timestamp, oneTimePassword));
    assertTrue(totpManager.checkTotpMatches(aci, totpKey.totpKey(), timestamp, oneTimePassword));
    assertFalse(totpManager.checkTotpMatches(aci, totpKey.totpKey(), timestamp, oneTimePassword + 1));
  }

  @Test
  void verifyTotpRejectsReuse() throws NoSuchAlgorithmException, InvalidKeyException {
    final AnnotatedTotpKey totpKey = generateTotpKey();

    final Account account = mock(Account.class);
    when(account.getAccountIdentifier()).thenReturn(UUID.randomUUID());
    when(account.getMfaKeys()).thenReturn(Map.of((byte) 1, totpKey));

    final Instant timestamp = Instant.now();
    final int oneTimePassword = TotpManager.TOTP.generateOneTimePassword(totpKey, timestamp);

    assertTrue(totpManager.verifyTotp(account, timestamp, oneTimePassword));

    assertFalse(totpManager.verifyTotp(account, timestamp, oneTimePassword),
        "One-time password should not be redeemable more than once");

    // A different ACI / same one-time password should work
    when(account.getAccountIdentifier()).thenReturn(UUID.randomUUID());
    assertTrue(totpManager.verifyTotp(account, timestamp, oneTimePassword));
  }

  @RepeatedTest(value = 10, failureThreshold = 2)
  void verifyTotpWithDelay() throws NoSuchAlgorithmException, InvalidKeyException {
    final AnnotatedTotpKey totpKey = generateTotpKey();

    final Account account = mock(Account.class);
    when(account.getAccountIdentifier()).thenAnswer(_ -> UUID.randomUUID());
    when(account.getMfaKeys()).thenReturn(Map.of((byte) 1, totpKey));

    final Instant beginningOfTotpWindow = totpWindowStart(Instant.now());
    final int oneTimePassword = TotpManager.TOTP.generateOneTimePassword(totpKey, beginningOfTotpWindow);

    assertTrue(totpManager.verifyTotp(account, beginningOfTotpWindow, oneTimePassword),
        "One-time password should be valid at the start of the window in which it was generated");

    assertTrue(totpManager.verifyTotp(account, beginningOfTotpWindow.plus(TotpManager.TOTP.getTimeStep()), oneTimePassword),
        "One-time password should be valid at the start of the window after which it was generated");

    assertTrue(totpManager.verifyTotp(account, beginningOfTotpWindow.plus(TotpManager.TOTP.getTimeStep()).plus(MAX_TOTP_VALIDATION_DELAY).minusMillis(1), oneTimePassword),
        "One-time password should be valid up until max delay after end of current TOTP window");

    // With six-digit OTPs, there's a one-in-a-million chance of this returning a false positive, and so we repeat the
    // test several allowing for failure
    assertFalse(totpManager.verifyTotp(account, beginningOfTotpWindow.plus(TotpManager.TOTP.getTimeStep()).plus(MAX_TOTP_VALIDATION_DELAY), oneTimePassword),
        "One-time password should not be valid after max delay past end of current TOTP window");
  }

  @ParameterizedTest
  @MethodSource
  void verifyTotp(final Map<Byte, AnnotatedMfaKey> mfaKeys,
      final Instant timestamp,
      @Nullable final Integer oneTimePassword,
      final boolean expectVerified) {

    final Account account = mock(Account.class);
    when(account.getAccountIdentifier()).thenReturn(UUID.randomUUID());
    when(account.getMfaKeys()).thenReturn(mfaKeys);

    assertEquals(expectVerified, totpManager.verifyTotp(account, timestamp, oneTimePassword));
  }

  private static List<Arguments> verifyTotp() throws NoSuchAlgorithmException, InvalidKeyException {
    final Instant timestamp = Instant.now();

    final AnnotatedTotpKey totpKey = generateTotpKey();
    final AnnotatedTotpKey secondTotpKey = generateTotpKey();

    return List.of(
        Arguments.argumentSet("No keys, no password provided",
            Collections.emptyMap(), timestamp, null, true),

        Arguments.argumentSet("No keys, password provided",
            Collections.emptyMap(), timestamp, 123456, false),

        Arguments.argumentSet("Has key, correct password provided",
            Map.of((byte) 1, totpKey), timestamp, TotpManager.TOTP.generateOneTimePassword(totpKey, timestamp), true),

        Arguments.argumentSet("Has key, incorrect password provided",
            Map.of((byte) 1, totpKey), timestamp, TotpManager.TOTP.generateOneTimePassword(totpKey, timestamp) + 1, false),

        Arguments.argumentSet("Has key, no password provided",
            Map.of((byte) 1, totpKey), timestamp, null, false),

        Arguments.argumentSet("Has multiple keys, correct password provided for one key",
            Map.of((byte) 1, totpKey, (byte) 2, secondTotpKey), timestamp, TotpManager.TOTP.generateOneTimePassword(totpKey, timestamp), true)
    );
  }
}
