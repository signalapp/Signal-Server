package org.whispersystems.textsecuregcm.tests.controllers;

import com.google.common.collect.ImmutableSet;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialGenerator;
import org.whispersystems.textsecuregcm.auth.DisabledPermittedAccount;
import org.whispersystems.textsecuregcm.controllers.DirectoryController;
import org.whispersystems.textsecuregcm.entities.ClientContactTokens;
import org.whispersystems.textsecuregcm.entities.DirectoryFeedbackRequest;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.DirectoryManager;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.Base64;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.Response.Status.Family;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import io.dropwizard.auth.PolymorphicAuthValueFactoryProvider;
import io.dropwizard.testing.junit.ResourceTestRule;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyListOf;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class DirectoryControllerTest {

  private final RateLimiters                       rateLimiters                  = mock(RateLimiters.class                      );
  private final RateLimiter                        rateLimiter                   = mock(RateLimiter.class                       );
  private final RateLimiter                        ipLimiter                     = mock(RateLimiter.class                       );
  private final DirectoryManager                   directoryManager              = mock(DirectoryManager.class                  );
  private final ExternalServiceCredentialGenerator directoryCredentialsGenerator = mock(ExternalServiceCredentialGenerator.class);

  private final ExternalServiceCredentials validCredentials = new ExternalServiceCredentials("username", "password");

  @Rule
  public final ResourceTestRule resources = ResourceTestRule.builder()
                                                            .addProvider(AuthHelper.getAuthFilter())
                                                            .addProvider(new PolymorphicAuthValueFactoryProvider.Binder<>(ImmutableSet.of(Account.class, DisabledPermittedAccount.class)))
                                                            .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
                                                            .addResource(new DirectoryController(rateLimiters,
                                                                                                 directoryManager,
                                                                                                 directoryCredentialsGenerator))
                                                            .build();


  @Before
  public void setup() throws Exception {
    when(rateLimiters.getContactsLimiter()).thenReturn(rateLimiter);
    when(rateLimiters.getContactsIpLimiter()).thenReturn(ipLimiter);
    when(directoryManager.get(anyListOf(byte[].class))).thenAnswer(new Answer<List<byte[]>>() {
      @Override
      public List<byte[]> answer(InvocationOnMock invocationOnMock) throws Throwable {
        List<byte[]> query = (List<byte[]>) invocationOnMock.getArguments()[0];
        List<byte[]> response = new LinkedList<>(query);
        response.remove(0);
        return response;
      }
    });
    when(directoryCredentialsGenerator.generateFor(eq(AuthHelper.VALID_NUMBER))).thenReturn(validCredentials);
  }

  @Test
  public void testFeedbackOk() {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/directory/feedback-v3/ok")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                 .put(Entity.entity(new DirectoryFeedbackRequest(Optional.of("test reason")), MediaType.APPLICATION_JSON_TYPE));
    assertThat(response.getStatusInfo().getFamily()).isEqualTo(Family.SUCCESSFUL);
  }

  @Test
  public void testNotFoundFeedback() {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/directory/feedback-v3/test-not-found")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                 .put(Entity.entity(new DirectoryFeedbackRequest(Optional.of("test reason")), MediaType.APPLICATION_JSON_TYPE));
    assertThat(response.getStatusInfo()).isEqualTo(Status.NOT_FOUND);
  }

  @Test
  public void testFeedbackEmptyRequest() {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/directory/feedback-v3/mismatch")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                 .put(Entity.json(""));
    assertThat(response.getStatusInfo().getFamily()).isEqualTo(Family.SUCCESSFUL);
  }

  @Test
  public void testFeedbackNoReason() {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/directory/feedback-v3/mismatch")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                 .put(Entity.entity(new DirectoryFeedbackRequest(Optional.empty()), MediaType.APPLICATION_JSON_TYPE));
    assertThat(response.getStatusInfo().getFamily()).isEqualTo(Family.SUCCESSFUL);
  }

  @Test
  public void testFeedbackEmptyReason() {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/directory/feedback-v3/mismatch")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                 .put(Entity.entity(new DirectoryFeedbackRequest(Optional.of("")), MediaType.APPLICATION_JSON_TYPE));
    assertThat(response.getStatusInfo().getFamily()).isEqualTo(Family.SUCCESSFUL);
  }

  @Test
  public void testFeedbackTooLargeReason() {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/directory/feedback-v3/mismatch")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                 .put(Entity.entity(new DirectoryFeedbackRequest(Optional.of(new String(new char[102400]))), MediaType.APPLICATION_JSON_TYPE));
    assertThat(response.getStatusInfo().getFamily()).isEqualTo(Family.CLIENT_ERROR);
  }

  @Test
  public void testGetAuthToken() {
    ExternalServiceCredentials token =
            resources.getJerseyTest()
                     .target("/v1/directory/auth")
                     .request()
                     .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                     .get(ExternalServiceCredentials.class);
    assertThat(token.getUsername()).isEqualTo(validCredentials.getUsername());
    assertThat(token.getPassword()).isEqualTo(validCredentials.getPassword());
  }

  @Test
  public void testDisabledGetAuthToken() {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/directory/auth")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.DISABLED_NUMBER, AuthHelper.DISABLED_PASSWORD))
                 .get();
    assertThat(response.getStatus()).isEqualTo(401);
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

    Response response =
        resources.getJerseyTest()
                 .target("/v1/directory/tokens/")
                 .request()
                 .header("Authorization",
                         AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER,
                                                  AuthHelper.VALID_PASSWORD))
                 .header("X-Forwarded-For", "192.168.1.1, 1.1.1.1")
                 .put(Entity.entity(new ClientContactTokens(tokens), MediaType.APPLICATION_JSON_TYPE));


    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.readEntity(ClientContactTokens.class).getContacts()).isEqualTo(expectedResponse);

    verify(ipLimiter).validate("1.1.1.1");
  }
}
