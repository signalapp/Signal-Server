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

  public record CredentialInfo(String token, boolean valid, ExternalServiceCredentials credentials, long timestamp) {
    /**
     * @return a copy of this record with valid=false
     */
    private CredentialInfo invalidate() {
      return new CredentialInfo(token, false, credentials, timestamp);
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
        results.add(new CredentialInfo(token, false, null, 0L));
        continue;
      }
      final ExternalServiceCredentials credentials = new ExternalServiceCredentials(parts[0], parts[1]);
      final Optional<Long> maybeTimestamp = credentialsGenerator.validateAndGetTimestamp(credentials, maxAgeSeconds);
      if (maybeTimestamp.isEmpty()) {
        results.add(new CredentialInfo(token, false, null, 0L));
        continue;
      }

      // now that we validated signature and token age, we will also find the latest of the tokens
      // for each username
      final long timestamp = maybeTimestamp.get();
      final CredentialInfo best = bestForUsername.get(credentials.username());
      if (best == null) {
        bestForUsername.put(credentials.username(), new CredentialInfo(token, true, credentials, timestamp));
        continue;
      }
      if (best.timestamp() < timestamp) {
        // we found a better credential for the username
        bestForUsername.put(credentials.username(), new CredentialInfo(token, true, credentials, timestamp));
        // mark the previous best as an invalid credential, since we have a better credential now
        results.add(best.invalidate());
      } else {
        // the credential we already had was more recent, this one can be marked invalid
        results.add(new CredentialInfo(token, false, null, 0L));
      }
    }

    // all invalid tokens should be in results, just add the valid ones
    results.addAll(bestForUsername.values());
    return results;
  }

}
