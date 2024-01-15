/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import static java.util.Objects.requireNonNull;
import static org.whispersystems.textsecuregcm.util.HmacUtils.hmac256ToHexString;
import static org.whispersystems.textsecuregcm.util.HmacUtils.hmac256TruncatedToHexString;
import static org.whispersystems.textsecuregcm.util.HmacUtils.hmacHexStringsEqual;

import com.google.common.annotations.VisibleForTesting;
import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretBytes;

public class ExternalServiceCredentialsGenerator {

  private static final String DELIMITER = ":";

  private static final int TRUNCATED_SIGNATURE_LENGTH = 10;

  private final byte[] key;

  private final byte[] userDerivationKey;

  private final boolean prependUsername;

  private final boolean truncateSignature;

  private final String usernameTimestampPrefix;

  private final Function<Instant, Instant> usernameTimestampTruncator;

  private final Clock clock;

  private final int derivedUsernameTruncateLength;


  public static ExternalServiceCredentialsGenerator.Builder builder(final SecretBytes key) {
    return builder(key.value());
  }

  @VisibleForTesting
  public static ExternalServiceCredentialsGenerator.Builder builder(final byte[] key) {
    return new Builder(key);
  }

  private ExternalServiceCredentialsGenerator(
      final byte[] key,
      final byte[] userDerivationKey,
      final boolean prependUsername,
      final boolean truncateSignature,
      final int derivedUsernameTruncateLength,
      final String usernameTimestampPrefix,
      final Function<Instant, Instant> usernameTimestampTruncator,
      final Clock clock) {
    this.key = requireNonNull(key);
    this.userDerivationKey = requireNonNull(userDerivationKey);
    this.prependUsername = prependUsername;
    this.truncateSignature = truncateSignature;
    this.usernameTimestampPrefix = usernameTimestampPrefix;
    this.usernameTimestampTruncator = usernameTimestampTruncator;
    this.clock = requireNonNull(clock);
    this.derivedUsernameTruncateLength = derivedUsernameTruncateLength;

    if (hasUsernameTimestampPrefix() ^ hasUsernameTimestampTruncator()) {
      throw new RuntimeException("Configured to have only one of (usernameTimestampPrefix, usernameTimestampTruncator)");
    }
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
    if (usernameIsTimestamp()) {
      throw new RuntimeException("Configured to use timestamp as username");
    }

    return generate(identity);
  }

  /**
   * Generates `ExternalServiceCredentials` using a prefix concatenated with a truncated timestamp as the username, following this generator's configuration.
   * @return an instance of {@link ExternalServiceCredentials}
   */
  public ExternalServiceCredentials generateWithTimestampAsUsername() {
    if (!usernameIsTimestamp()) {
      throw new RuntimeException("Not configured to use timestamp as username");
    }

    final String truncatedTimestampSeconds = String.valueOf(usernameTimestampTruncator.apply(clock.instant()).getEpochSecond());
    return generate(usernameTimestampPrefix + DELIMITER + truncatedTimestampSeconds);
  }

  private ExternalServiceCredentials generate(final String identity) {
    final String username = shouldDeriveUsername()
        ? hmac256TruncatedToHexString(userDerivationKey, identity, derivedUsernameTruncateLength)
        : identity;

    final long currentTimeSeconds = currentTimeSeconds();

    final String dataToSign = usernameIsTimestamp() ? username : username + DELIMITER + currentTimeSeconds;

    final String signature = truncateSignature
        ? hmac256TruncatedToHexString(key, dataToSign, TRUNCATED_SIGNATURE_LENGTH)
        : hmac256ToHexString(key, dataToSign);

    final String token = (prependUsername ? dataToSign : currentTimeSeconds) + DELIMITER + signature;

    return new ExternalServiceCredentials(username, token);
  }

  /**
   * In certain cases, identity (as it was passed to `generate` method)
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
    if (StringUtils.countMatches(password, DELIMITER) == 2) {
      if (usernameIsTimestamp()) {
        final int indexOfSecondDelimiter = password.indexOf(DELIMITER, password.indexOf(DELIMITER) + 1);
        return Optional.of(password.substring(0, indexOfSecondDelimiter));
      } else {
        return Optional.of(password.substring(0, password.indexOf(DELIMITER)));
      }
    }
    return Optional.empty();
  }

  /**
   * Given an instance of {@link ExternalServiceCredentials} object, checks that the password
   * matches the username taking into account this generator's configuration.
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
      final String username = usernameIsTimestamp() ? parts[0] + DELIMITER + parts[1] : parts[0];
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

    final String signedData = usernameIsTimestamp() ? credentials.username() : credentials.username() + DELIMITER + timestampSeconds;
    final String expectedSignature = truncateSignature
        ? hmac256TruncatedToHexString(key, signedData, TRUNCATED_SIGNATURE_LENGTH)
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

  private boolean hasUsernameTimestampPrefix() {
    return usernameTimestampPrefix != null;
  }

  private boolean hasUsernameTimestampTruncator() {
    return usernameTimestampTruncator != null;
  }

  private boolean usernameIsTimestamp() {
    return hasUsernameTimestampPrefix() && hasUsernameTimestampTruncator();
  }

  private long currentTimeSeconds() {
    return clock.instant().getEpochSecond();
  }

  public static class Builder {

    private final byte[] key;

    private byte[] userDerivationKey = new byte[0];

    private boolean prependUsername = true;

    private boolean truncateSignature = true;

    private int derivedUsernameTruncateLength = 10;

    private String usernameTimestampPrefix = null;

    private Function<Instant, Instant> usernameTimestampTruncator = null;

    private Clock clock = Clock.systemUTC();


    private Builder(final byte[] key) {
      this.key = requireNonNull(key);
    }

    public Builder withUserDerivationKey(final SecretBytes userDerivationKey) {
      return withUserDerivationKey(userDerivationKey.value());
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

    public Builder withDerivedUsernameTruncateLength(int truncateLength) {
      Validate.inclusiveBetween(10, 32, truncateLength);
      this.derivedUsernameTruncateLength = truncateLength;
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

    public Builder withUsernameTimestampTruncatorAndPrefix(final Function<Instant, Instant> truncator, final String prefix) {
      this.usernameTimestampTruncator = truncator;
      this.usernameTimestampPrefix = prefix;
      return this;
    }

    public ExternalServiceCredentialsGenerator build() {
      return new ExternalServiceCredentialsGenerator(
          key, userDerivationKey, prependUsername, truncateSignature, derivedUsernameTruncateLength, usernameTimestampPrefix, usernameTimestampTruncator, clock);
    }
  }
}
