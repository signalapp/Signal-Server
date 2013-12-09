package org.whispersystems.textsecuregcm.tests.controllers;

import com.sun.jersey.api.client.ClientResponse;
import com.yammer.dropwizard.testing.ResourceTest;
import org.junit.Test;
import org.whispersystems.textsecuregcm.controllers.KeysController;
import org.whispersystems.textsecuregcm.entities.PreKey;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.Keys;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class KeyControllerTest extends ResourceTest {

  private final String EXISTS_NUMBER     = "+14152222222";
  private final String NOT_EXISTS_NUMBER = "+14152222220";

  private final PreKey SAMPLE_KEY = new PreKey(1, EXISTS_NUMBER, 1234, "test1", "test2", false);
  private final Keys   keys       = mock(Keys.class);

  @Override
  protected void setUpResources() {
    addProvider(AuthHelper.getAuthenticator());

    RateLimiters rateLimiters = mock(RateLimiters.class);
    RateLimiter  rateLimiter  = mock(RateLimiter.class );

    when(rateLimiters.getPreKeysLimiter()).thenReturn(rateLimiter);

    when(keys.get(EXISTS_NUMBER)).thenReturn(SAMPLE_KEY);
    when(keys.get(NOT_EXISTS_NUMBER)).thenReturn(null);

    addResource(new KeysController(rateLimiters, keys, null));
  }

  @Test
  public void validRequestTest() throws Exception {
    PreKey result = client().resource(String.format("/v1/keys/%s", EXISTS_NUMBER))
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
        .get(PreKey.class);

    assertThat(result.getKeyId()).isEqualTo(SAMPLE_KEY.getKeyId());
    assertThat(result.getPublicKey()).isEqualTo(SAMPLE_KEY.getPublicKey());
    assertThat(result.getIdentityKey()).isEqualTo(SAMPLE_KEY.getIdentityKey());

    assertThat(result.getId() == 0);
    assertThat(result.getNumber() == null);

    verify(keys).get(EXISTS_NUMBER);
  }

  @Test
  public void invalidRequestTest() throws Exception {
    ClientResponse response = client().resource(String.format("/v1/keys/%s", NOT_EXISTS_NUMBER))
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
        .get(ClientResponse.class);

    assertThat(response.getClientResponseStatus().getStatusCode()).isEqualTo(404);

    verify(keys).get(NOT_EXISTS_NUMBER);
  }

  @Test
  public void unauthorizedRequestTest() throws Exception {
    ClientResponse response =
        client().resource(String.format("/v1/keys/%s", NOT_EXISTS_NUMBER))
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.INVALID_PASSWORD))
            .get(ClientResponse.class);

    assertThat(response.getClientResponseStatus().getStatusCode()).isEqualTo(401);

    response =
        client().resource(String.format("/v1/keys/%s", NOT_EXISTS_NUMBER))
            .get(ClientResponse.class);

    assertThat(response.getClientResponseStatus().getStatusCode()).isEqualTo(401);
  }

}