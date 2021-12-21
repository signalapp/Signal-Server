/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.auth;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.auth.AuthenticationCredentials;

class AuthenticationCredentialsTest {

  @Test
  void testCreating() {
    AuthenticationCredentials credentials = new AuthenticationCredentials("mypassword");
    assertThat(credentials.getSalt()).isNotEmpty();
    assertThat(credentials.getHashedAuthenticationToken()).isNotEmpty();
    assertThat(credentials.getHashedAuthenticationToken().length()).isEqualTo(40);
  }

  @Test
  void testMatching() {
    AuthenticationCredentials credentials = new AuthenticationCredentials("mypassword");

    AuthenticationCredentials provided = new AuthenticationCredentials(credentials.getHashedAuthenticationToken(), credentials.getSalt());
    assertThat(provided.verify("mypassword")).isTrue();
  }

  @Test
  void testMisMatching() {
    AuthenticationCredentials credentials = new AuthenticationCredentials("mypassword");

    AuthenticationCredentials provided = new AuthenticationCredentials(credentials.getHashedAuthenticationToken(), credentials.getSalt());
    assertThat(provided.verify("wrong")).isFalse();
  }

}
