/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ExternalServiceCredentialsSelector {

  private ExternalServiceCredentialsSelector() {}

  public enum CredentialStatus {
    // The token passes verification, is not expired, and is the most recent token available
    VALID,
    // The token was malformed or the signature was invalid
    FAILED_VERIFICATION,
    // The credential was correctly signed, but exceeded the maximum credential age
    EXPIRED,
    // There was a more recent credential available with the same username
    REPLACED
  }

  public record CredentialInfo(String token, ExternalServiceCredentials credentials, long timestamp, CredentialStatus status) {
    /**
     * @return a copy of this record that indicates it has been replaced with a more up-to-date token
     */
    private CredentialInfo replaced() {
      return new CredentialInfo(token, credentials, timestamp, CredentialStatus.REPLACED);
    }

    public boolean valid() {
      return status == CredentialStatus.VALID;
    }
  }

  /**
   * Validate a list of username:password credentials.
   * A credential is valid if it passes validation by the provided credentialsGenerator AND it is the most recent
   * credential in the provided list for a username.
   *
   * @param tokens A list of credentials, potentially with different usernames
   * @param credentialsGenerator To validate these credentials
   * @param maxAgeSeconds The maximum allowable age of the credential
   * @return A {@link CredentialInfo} for each provided token
   */
  public static List<CredentialInfo> check(
      final List<String> tokens,
      final ExternalServiceCredentialsGenerator credentialsGenerator,
      final long maxAgeSeconds) {

    // the credential for the username with the latest timestamp (so far)
    final Map<String, CredentialInfo> bestForUsername = new HashMap<>();
    final List<CredentialInfo> results = new ArrayList<>();
    for (String token : tokens) {
      // each token is supposed to be in a "${username}:${password}" form,
      // (note that password part may also contain ':' characters)
      final String[] parts = token.split(":", 2);
      if (parts.length != 2) {
        results.add(new CredentialInfo(token,  null, 0L, CredentialStatus.FAILED_VERIFICATION));
        continue;
      }
      final ExternalServiceCredentials credentials = new ExternalServiceCredentials(parts[0], parts[1]);
      final Optional<Long> maybeTimestamp = credentialsGenerator.validateAndGetTimestamp(credentials);
      if (maybeTimestamp.isEmpty()) {
        results.add(new CredentialInfo(token, credentials, 0L, CredentialStatus.FAILED_VERIFICATION));
        continue;
      }
      final long credentialTs = maybeTimestamp.get();
      if (credentialsGenerator.isCredentialExpired(credentialTs, maxAgeSeconds)) {
        results.add(new CredentialInfo(token, credentials, credentialTs, CredentialStatus.EXPIRED));
        continue;
      }

      // now that we validated signature and token age, we will also find the latest of the tokens
      // for each username
      final long timestamp = maybeTimestamp.get();
      final CredentialInfo best = bestForUsername.get(credentials.username());
      if (best == null) {
        bestForUsername.put(credentials.username(), new CredentialInfo(token, credentials, timestamp, CredentialStatus.VALID));
        continue;
      }
      if (best.timestamp() < timestamp) {
        // we found a better credential for the username
        bestForUsername.put(credentials.username(), new CredentialInfo(token, credentials, timestamp, CredentialStatus.VALID));
        // mark the previous best as an invalid credential, since we have a better credential now
        results.add(best.replaced());
      } else {
        // the credential we already had was more recent, this one can be marked invalid
        results.add(new CredentialInfo(token, null, 0L, CredentialStatus.REPLACED));
      }
    }

    // all invalid tokens should be in results, just add the valid ones
    results.addAll(bestForUsername.values());
    return results;
  }

}
