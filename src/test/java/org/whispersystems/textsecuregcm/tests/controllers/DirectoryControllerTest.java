package org.whispersystems.textsecuregcm.tests.controllers;

import com.sun.jersey.api.client.ClientResponse;
import com.yammer.dropwizard.testing.ResourceTest;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.whispersystems.textsecuregcm.controllers.DirectoryController;
import org.whispersystems.textsecuregcm.entities.ClientContactTokens;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.DirectoryManager;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.Base64;

import javax.ws.rs.core.MediaType;
import java.util.LinkedList;
import java.util.List;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DirectoryControllerTest extends ResourceTest {

  private RateLimiters     rateLimiters     = mock(RateLimiters.class    );
  private RateLimiter      rateLimiter      = mock(RateLimiter.class     );
  private DirectoryManager directoryManager = mock(DirectoryManager.class);

  @Override
  protected void setUpResources() throws Exception {
    addProvider(AuthHelper.getAuthenticator());

    when(rateLimiters.getContactsLimiter()).thenReturn(rateLimiter);
    when(directoryManager.get(anyList())).thenAnswer(new Answer<List<byte[]>>() {
      @Override
      public List<byte[]> answer(InvocationOnMock invocationOnMock) throws Throwable {
        List<byte[]> query    = (List<byte[]>) invocationOnMock.getArguments()[0];
        List<byte[]> response = new LinkedList<>(query);
        response.remove(0);
        return response;
      }
    });

    addResource(new DirectoryController(rateLimiters, directoryManager));
  }

  @Test
  public void testContactIntersection() throws Exception {
    List<String> tokens = new LinkedList<String>() {{
      add(Base64.encodeBytes("foo".getBytes()));
      add(Base64.encodeBytes("bar".getBytes()));
      add(Base64.encodeBytes("baz".getBytes()));
    }};

    List<String> expectedResponse = new LinkedList<>(tokens);
    expectedResponse.remove(0);

    ClientResponse response =
        client().resource("/v1/directory/tokens/")
            .entity(new ClientContactTokens(tokens))
            .type(MediaType.APPLICATION_JSON_TYPE)
            .header("Authorization",
                    AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER,
                                             AuthHelper.VALID_PASSWORD))
            .put(ClientResponse.class);



    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.getEntity(ClientContactTokens.class).getContacts()).isEqualTo(expectedResponse);
  }
}
