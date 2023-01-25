/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import static java.util.Objects.requireNonNull;
import static org.whispersystems.textsecuregcm.util.HmacUtils.hmac256ToHexString;
import static org.whispersystems.textsecuregcm.util.HmacUtils.hmac256TruncatedToHexString;

import java.time.Clock;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.Validate;

public class ExternalServiceCredentialsGenerator {

  private static final int TRUNCATE_LENGTH = 10;

  private final byte[] key;

  private final byte[] userDerivationKey;

  private final boolean prependUsername;

  private final boolean truncateSignature;

  private final Clock clock;


  public static ExternalServiceCredentialsGenerator.Builder builder(final byte[] key) {
    return new Builder(key);
  }

  private ExternalServiceCredentialsGenerator(
      final byte[] key,
      final byte[] userDerivationKey,
      final boolean prependUsername,
      final boolean truncateSignature,
      final Clock clock) {
    this.key = requireNonNull(key);
    this.userDerivationKey = requireNonNull(userDerivationKey);
    this.prependUsername = prependUsername;
    this.truncateSignature = truncateSignature;
    this.clock = requireNonNull(clock);
  }

  public ExternalServiceCredentials generateForUuid(final UUID uuid) {
    return generateFor(uuid.toString());
  }

  public ExternalServiceCredentials generateFor(final String identity) {
    final String username = userDerivationKey.length > 0
        ? hmac256TruncatedToHexString(userDerivationKey, identity, TRUNCATE_LENGTH)
        : identity;

    final long currentTimeSeconds = TimeUnit.MILLISECONDS.toSeconds(clock.millis());

    final String dataToSign = username + ":" + currentTimeSeconds;

    final String signature = truncateSignature
        ? hmac256TruncatedToHexString(key, dataToSign, TRUNCATE_LENGTH)
        : hmac256ToHexString(key, dataToSign);

    final String token = (prependUsername ? dataToSign : currentTimeSeconds) + ":" + signature;

    return new ExternalServiceCredentials(username, token);
  }

  public static class Builder {

    private final byte[] key;

    private byte[] userDerivationKey = new byte[0];

    private boolean prependUsername = true;

    private boolean truncateSignature = true;

    private Clock clock = Clock.systemUTC();


    private Builder(final byte[] key) {
      this.key = requireNonNull(key);
    }

    public Builder withUserDerivationKey(final byte[] userDerivationKey) {
      Validate.isTrue(requireNonNull(userDerivationKey).length > 0, "userDerivationKey must not be empty");
      this.userDerivationKey = userDerivationKey;
      return this;
    }

    public Builder withClock(final Clock clock) {
      this.clock = requireNonNull(clock);
      return this;
    }

    public Builder prependUsername(final boolean prependUsername) {
      this.prependUsername = prependUsername;
      return this;
    }

    public Builder truncateSignature(final boolean truncateSignature) {
      this.truncateSignature = truncateSignature;
      return this;
    }

    public ExternalServiceCredentialsGenerator build() {
      return new ExternalServiceCredentialsGenerator(
          key, userDerivationKey, prependUsername, truncateSignature, clock);
    }
  }
}
