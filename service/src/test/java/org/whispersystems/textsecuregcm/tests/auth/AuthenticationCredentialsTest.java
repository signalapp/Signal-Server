package org.whispersystems.textsecuregcm.tests.auth;

import org.junit.Test;
import org.whispersystems.textsecuregcm.auth.AuthenticationCredentials;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class AuthenticationCredentialsTest {

  @Test
  public void testCreating() {
    AuthenticationCredentials credentials = new AuthenticationCredentials("mypassword");
    assertThat(credentials.getSalt()).isNotEmpty();
    assertThat(credentials.getHashedAuthenticationToken()).isNotEmpty();
    assertThat(credentials.getHashedAuthenticationToken().length()).isEqualTo(40);
  }

  @Test
  public void testMatching() {
    AuthenticationCredentials credentials = new AuthenticationCredentials("mypassword");

    AuthenticationCredentials provided = new AuthenticationCredentials(credentials.getHashedAuthenticationToken(), credentials.getSalt());
    assertThat(provided.verify("mypassword")).isTrue();
  }

  @Test
  public void testMisMatching() {
    AuthenticationCredentials credentials = new AuthenticationCredentials("mypassword");

    AuthenticationCredentials provided = new AuthenticationCredentials(credentials.getHashedAuthenticationToken(), credentials.getSalt());
    assertThat(provided.verify("wrong")).isFalse();
  }


}
