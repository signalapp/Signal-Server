/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.controllers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.google.common.net.HttpHeaders;
import io.dropwizard.auth.PolymorphicAuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import java.util.Collections;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status.Family;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.DisabledPermittedAuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialGenerator;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.controllers.DirectoryController;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;

@ExtendWith(DropwizardExtensionsSupport.class)
class DirectoryControllerTest {

  private static final ExternalServiceCredentialGenerator directoryCredentialsGenerator = mock(ExternalServiceCredentialGenerator.class);
  private static final ExternalServiceCredentials         validCredentials              = new ExternalServiceCredentials("username", "password");

  private static final ResourceExtension resources = ResourceExtension.builder()
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(new PolymorphicAuthValueFactoryProvider.Binder<>(
          ImmutableSet.of(AuthenticatedAccount.class, DisabledPermittedAuthenticatedAccount.class)))
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(new DirectoryController(directoryCredentialsGenerator))
      .build();

  @BeforeEach
  void setup() {
    when(directoryCredentialsGenerator.generateFor(eq(AuthHelper.VALID_NUMBER))).thenReturn(validCredentials);
  }

  @Test
  void testFeedbackOk() {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/directory/feedback-v3/ok")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                 .put(Entity.json("{\"reason\": \"test reason\"}"));
    assertThat(response.getStatusInfo().getFamily()).isEqualTo(Family.SUCCESSFUL);
  }

  @Test
  void testGetAuthToken() {
    ExternalServiceCredentials token =
            resources.getJerseyTest()
                     .target("/v1/directory/auth")
                     .request()
                     .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                     .get(ExternalServiceCredentials.class);
    assertThat(token.username()).isEqualTo(validCredentials.username());
    assertThat(token.password()).isEqualTo(validCredentials.password());
  }

  @Test
  void testDisabledGetAuthToken() {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/directory/auth")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.DISABLED_UUID, AuthHelper.DISABLED_PASSWORD))
                 .get();
    assertThat(response.getStatus()).isEqualTo(401);
  }


  @Test
  void testContactIntersection() {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/directory/tokens/")
                 .request()
                 .header("Authorization",
                         AuthHelper.getAuthHeader(AuthHelper.VALID_UUID,
                                                  AuthHelper.VALID_PASSWORD))
                 .header(HttpHeaders.X_FORWARDED_FOR, "192.168.1.1, 1.1.1.1")
                 .put(Entity.entity(Collections.emptyMap(), MediaType.APPLICATION_JSON_TYPE));


    assertThat(response.getStatus()).isEqualTo(429);
  }
}
