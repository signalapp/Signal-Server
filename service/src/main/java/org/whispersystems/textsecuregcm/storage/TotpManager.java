/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import com.eatthepath.otp.TimeBasedOneTimePasswordGenerator;
import com.google.common.annotations.VisibleForTesting;
import io.lettuce.core.SetArgs;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import javax.annotation.Nullable;
import javax.crypto.KeyGenerator;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;
import org.whispersystems.textsecuregcm.util.logging.ImpossibleEvents;

public class TotpManager {
  private static final Logger logger = LoggerFactory.getLogger(TotpManager.class);

  @VisibleForTesting
  public static final TimeBasedOneTimePasswordGenerator TOTP = new TimeBasedOneTimePasswordGenerator();
  private static final TotpParameters TOTP_PARAMETERS =
      new TotpParameters(TOTP.getAlgorithm(), TOTP.getPasswordLength(), TOTP.getTimeStep());

  private final FaultTolerantRedisClusterClient rateLimitCluster;
  private final Duration maxTotpValidationDelay;
  private final KeyGenerator totpKeyGenerator;


  public TotpManager(final FaultTolerantRedisClusterClient rateLimitCluster, final Duration maxTotpValidationDelay) {
    if (maxTotpValidationDelay.compareTo(TOTP.getTimeStep()) > 0) {
      throw new IllegalArgumentException("Max TOTP validation delay must be less than or equal to TOTP time step");
    }

    this.rateLimitCluster = rateLimitCluster;
    this.maxTotpValidationDelay = maxTotpValidationDelay;
    try {
      this.totpKeyGenerator = KeyGenerator.getInstance(TOTP.getAlgorithm());
      totpKeyGenerator.init(getTotpKeyLengthBits());
    } catch (final NoSuchAlgorithmException e) {
      throw new AssertionError("Every implementation of the Java platform is required to support the HmacSHA256 KeyGenerator algorithm", e);
    }

  }

  public TotpKey generateTotpKey() {
    final SecretKey secretKey = totpKeyGenerator.generateKey();
    return new TotpKey(TOTP_PARAMETERS, secretKey.getEncoded());
  }


  /// Verifies the provided `oneTimePassword` against the account's registered TOTP keys.
  ///
  /// TOTPs are checked against the provided `validationTimestamp` but allow for some staleness configured via
  /// `maxTotpValidationDelay`. An OTP can only be used once per time-window.
  ///
  /// @param account             The account to check
  /// @param validationTimestamp The current time to check against
  /// @param oneTimePassword     The `oneTimePassword` to check against the account
  ///
  /// @return true if the `oneTimePassword` matches at least one of the keys registered in the account, or if the
  /// account has no keys AND the provided `oneTimePassword` is `null`.
  public boolean verifyTotp(final Account account, final Instant validationTimestamp, @Nullable final Integer oneTimePassword) {
    final List<AnnotatedTotpKey> totpKeys = account.getMfaKeys().values().stream().filter(AnnotatedTotpKey.class::isInstance).map(AnnotatedTotpKey.class::cast).toList();
    if (totpKeys.isEmpty()) {
      return oneTimePassword == null;
    }

    if (oneTimePassword == null) {
      // The account has TOTP keys, but the caller hasn't provided a one-time password
      return false;
    }

    for (final SecretKey totpKey : totpKeys) {
      try {
        if (evaluateTotp(account.getAccountIdentifier(), totpKey, validationTimestamp, oneTimePassword) == TotpOutcome.MATCH) {
          return true;
        }
      } catch (final InvalidKeyException e) {
        ImpossibleEvents.logImpossible(logger, "Invalid TOTP key for account {}", account.getAccountIdentifier(), e);
      }
    }

    return false;
  }


  /// Check the provided `oneTimePassword` against a specific `totpKey`. This can be used to confirm that a specific key
  /// matches the provided `oneTimePassword`. This method marks the provided `oneTimePassword` as used, but does not
  /// fail if the password is used multiple times.
  ///
  /// @return true if the `oneTimePassword` matches the provided `totpKey`, regardless of whether it has been checked before
  boolean checkTotpMatches(final UUID aci, final SecretKey totpKey, final Instant validationTimestamp, final int oneTimePassword) throws InvalidKeyException {
    return switch(evaluateTotp(aci, totpKey, validationTimestamp, oneTimePassword)) {
      case MISMATCH -> false;
      case REUSED, MATCH -> true;
    };
  }

  private enum TotpOutcome {
    /// The TOTP matched the key and this is the first time the TOTP was seen
    MATCH,
    /// The TOTP did not match the key
    MISMATCH,
    /// The TOTP matched the key, but has been seen before
    REUSED
  }

  /// Check the provided `oneTimePassword` against a specific `totpKey`
  private TotpOutcome evaluateTotp(final UUID aci, final SecretKey totpKey, final Instant validationTimestamp, final int oneTimePassword) throws InvalidKeyException {
    for (final Instant timestamp : new Instant[]{validationTimestamp, validationTimestamp.minus(maxTotpValidationDelay)}) {
      if (TOTP.validateOneTimePassword(totpKey, timestamp, oneTimePassword)) {
        return claimTotp(aci, oneTimePassword) ? TotpOutcome.MATCH : TotpOutcome.REUSED;
      }
    }
    return TotpOutcome.MISMATCH;
  }

  /// @return true if this was the first time this OTP was used
  private boolean claimTotp(final UUID aci, final int oneTimePassword) {
    return rateLimitCluster.withCluster(conn ->
        "OK".equals(conn.sync().set(getTotpFreshnessKey(aci, oneTimePassword), "", SetArgs.Builder
            .nx()
            .ex(TOTP.getTimeStep().plus(maxTotpValidationDelay)))));
  }

  private static String getTotpFreshnessKey(final UUID aci, final int oneTimePassword) {
    return "totp_used::" + aci + "::" + oneTimePassword;
  }

  @VisibleForTesting
  static int getTotpKeyLengthBits() {
    try {
      // The HOTP/TOTP spec recommends using a key length that's the same as the HMAC block length
      return Mac.getInstance(TOTP.getAlgorithm()).getMacLength() * 8;
    } catch (final NoSuchAlgorithmException e) {
      throw new AssertionError("Algorithm used by TOTP generator not found", e);
    }
  }
}
