/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.when;
import static org.whispersystems.textsecuregcm.util.MockUtils.randomSecretBytes;

import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import jakarta.ws.rs.core.Response;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.configuration.SecureStorageServiceConfiguration;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.MockUtils;
import org.whispersystems.textsecuregcm.util.SystemMapper;

@ExtendWith(DropwizardExtensionsSupport.class)
class SecureStorageControllerTest {

  private static final SecureStorageServiceConfiguration STORAGE_CFG = MockUtils.buildMock(
      SecureStorageServiceConfiguration.class,
      cfg -> when(cfg.userAuthenticationTokenSharedSecret()).thenReturn(randomSecretBytes(32)));

  private static final ExternalServiceCredentialsGenerator STORAGE_CREDENTIAL_GENERATOR = SecureStorageController
      .credentialsGenerator(STORAGE_CFG);

  private static final ResourceExtension resources = ResourceExtension.builder()
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(new AuthValueFactoryProvider.Binder<>(AuthenticatedDevice.class))
      .setMapper(SystemMapper.jsonMapper())
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(new SecureStorageController(STORAGE_CREDENTIAL_GENERATOR))
      .build();


  @Test
  void testGetCredentials() throws Exception {
    ExternalServiceCredentials credentials = resources.getJerseyTest()
                                                      .target("/v1/storage/auth")
                                                      .request()
                                                      .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                                                      .get(ExternalServiceCredentials.class);

    assertThat(credentials.password()).isNotEmpty();
    assertThat(credentials.username()).isNotEmpty();
  }

  @Test
  void testGetCredentialsBadAuth() throws Exception {
    Response response = resources.getJerseyTest()
                                 .target("/v1/storage/auth")
                                 .request()
                                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.INVALID_UUID, AuthHelper.INVALID_PASSWORD))
                                 .get();

    assertThat(response.getStatus()).isEqualTo(401);
  }
}
