/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.controllers;

import com.google.common.collect.ImmutableSet;
import io.dropwizard.auth.PolymorphicAuthValueFactoryProvider;
import io.dropwizard.testing.junit.ResourceTestRule;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.whispersystems.textsecuregcm.auth.DisabledPermittedAccount;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialGenerator;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.controllers.DirectoryController;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status.Family;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DirectoryControllerTest {

  private final ExternalServiceCredentialGenerator directoryCredentialsGenerator = mock(ExternalServiceCredentialGenerator.class);
  private final ExternalServiceCredentials         validCredentials              = new ExternalServiceCredentials("username", "password");

  @Rule
  public final ResourceTestRule resources = ResourceTestRule.builder()
                                                            .addProvider(AuthHelper.getAuthFilter())
                                                            .addProvider(new PolymorphicAuthValueFactoryProvider.Binder<>(ImmutableSet.of(Account.class, DisabledPermittedAccount.class)))
                                                            .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
                                                            .addResource(new DirectoryController(directoryCredentialsGenerator))
                                                            .build();

  @Before
  public void setup() {
    when(directoryCredentialsGenerator.generateFor(eq(AuthHelper.VALID_NUMBER))).thenReturn(validCredentials);
  }

  @Test
  public void testFeedbackOk() {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/directory/feedback-v3/ok")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                 .put(Entity.json("{\"reason\": \"test reason\"}"));
    assertThat(response.getStatusInfo().getFamily()).isEqualTo(Family.SUCCESSFUL);
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
  public void testContactIntersection() {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/directory/tokens/")
                 .request()
                 .header("Authorization",
                         AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER,
                                                  AuthHelper.VALID_PASSWORD))
                 .header("X-Forwarded-For", "192.168.1.1, 1.1.1.1")
                 .put(Entity.entity(Collections.emptyMap(), MediaType.APPLICATION_JSON_TYPE));


    assertThat(response.getStatus()).isEqualTo(429);
  }
}
