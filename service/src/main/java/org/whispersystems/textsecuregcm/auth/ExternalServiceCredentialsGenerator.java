/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import static java.util.Objects.requireNonNull;
import static org.whispersystems.textsecuregcm.util.HmacUtils.hmac256ToHexString;
import static org.whispersystems.textsecuregcm.util.HmacUtils.hmac256TruncatedToHexString;
import static org.whispersystems.textsecuregcm.util.HmacUtils.hmacHexStringsEqual;

import java.time.Clock;
import java.util.Optional;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

public class ExternalServiceCredentialsGenerator {

  private static final int TRUNCATE_LENGTH = 10;

  private static final String DELIMITER = ":";

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

  /**
   * A convenience method for the case of identity in the form of {@link UUID}.
   * @param uuid identity to generate credentials for
   * @return an instance of {@link ExternalServiceCredentials}
   */
  public ExternalServiceCredentials generateForUuid(final UUID uuid) {
    return generateFor(uuid.toString());
  }

  /**
   * Generates `ExternalServiceCredentials` for the given identity following this generator's configuration.
   * @param identity identity string to generate credentials for
   * @return an instance of {@link ExternalServiceCredentials}
   */
  public ExternalServiceCredentials generateFor(final String identity) {
    final String username = shouldDeriveUsername()
        ? hmac256TruncatedToHexString(userDerivationKey, identity, TRUNCATE_LENGTH)
        : identity;

    final long currentTimeSeconds = currentTimeSeconds();

    final String dataToSign = username + DELIMITER + currentTimeSeconds;

    final String signature = truncateSignature
        ? hmac256TruncatedToHexString(key, dataToSign, TRUNCATE_LENGTH)
        : hmac256ToHexString(key, dataToSign);

    final String token = (prependUsername ? dataToSign : currentTimeSeconds) + DELIMITER + signature;

    return new ExternalServiceCredentials(username, token);
  }

  /**
   * In certain cases, identity (as it was passed to `generateFor` method)
   * is a part of the signature (`password`, in terms of `ExternalServiceCredentials`) string itself.
   * For such cases, this method returns the value of the identity string.
   * @param password `password` part of `ExternalServiceCredentials`
   * @return non-empty optional with an identity string value, or empty if value can't be extracted.
   */
  public Optional<String> identityFromSignature(final String password) {
    // for some generators, identity in the clear is just not a part of the password
    if (!prependUsername || shouldDeriveUsername() || StringUtils.isBlank(password)) {
      return Optional.empty();
    }
    // checking for the case of unexpected format
    return StringUtils.countMatches(password, DELIMITER) == 2
        ? Optional.of(password.substring(0, password.indexOf(DELIMITER)))
        : Optional.empty();
  }

  /**
   * Given an instance of {@link ExternalServiceCredentials} object, checks that the password
   * matches the username taking into accound this generator's configuration.
   * @param credentials an instance of {@link ExternalServiceCredentials}
   * @return An optional with a timestamp (seconds) of when the credentials were generated,
   *         or an empty optional if the password doesn't match the username for any reason (including malformed data)
   */
  public Optional<Long> validateAndGetTimestamp(final ExternalServiceCredentials credentials) {
    final String[] parts = requireNonNull(credentials).password().split(DELIMITER);
    final String timestampSeconds;
    final String actualSignature;

    // making sure password format matches our expectations based on the generator configuration
    if (parts.length == 3 && prependUsername) {
      final String username = parts[0];
      // username has to match the one from `credentials`
      if (!credentials.username().equals(username)) {
        return Optional.empty();
      }
      timestampSeconds = parts[1];
      actualSignature = parts[2];
    } else if (parts.length == 2 && !prependUsername) {
      timestampSeconds = parts[0];
      actualSignature = parts[1];
    } else {
      // unexpected password format
      return Optional.empty();
    }

    final String signedData = credentials.username() + DELIMITER + timestampSeconds;
    final String expectedSignature = truncateSignature
        ? hmac256TruncatedToHexString(key, signedData, TRUNCATE_LENGTH)
        : hmac256ToHexString(key, signedData);

    // if the signature is valid it's safe to parse the `timestampSeconds` string into Long
    return hmacHexStringsEqual(expectedSignature, actualSignature)
        ? Optional.of(Long.valueOf(timestampSeconds))
        : Optional.empty();
  }

  /**
   * Given an instance of {@link ExternalServiceCredentials} object and the max allowed age for those credentials,
   * checks if credentials are valid and not expired.
   * @param credentials an instance of {@link ExternalServiceCredentials}
   * @param maxAgeSeconds age in seconds
   * @return An optional with a timestamp (seconds) of when the credentials were generated,
   *         or an empty optional if the password doesn't match the username for any reason (including malformed data)
   */
  public Optional<Long> validateAndGetTimestamp(final ExternalServiceCredentials credentials, final long maxAgeSeconds) {
    return validateAndGetTimestamp(credentials)
        .filter(ts -> currentTimeSeconds() - ts <= maxAgeSeconds);
  }

  private boolean shouldDeriveUsername() {
    return userDerivationKey.length > 0;
  }

  private long currentTimeSeconds() {
    return clock.instant().getEpochSecond();
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
