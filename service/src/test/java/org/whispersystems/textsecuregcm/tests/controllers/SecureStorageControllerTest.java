/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.controllers;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.google.common.collect.ImmutableSet;
import io.dropwizard.auth.PolymorphicAuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.whispersystems.textsecuregcm.auth.DisabledPermittedAccount;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialGenerator;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.controllers.SecureStorageController;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.SystemMapper;

@ExtendWith(DropwizardExtensionsSupport.class)
class SecureStorageControllerTest {

  private static final ExternalServiceCredentialGenerator storageCredentialGenerator = new ExternalServiceCredentialGenerator(new byte[32], new byte[32], false);

  private static final ResourceExtension resources = ResourceExtension.builder()
                                                                      .addProvider(AuthHelper.getAuthFilter())
                                                                      .addProvider(new PolymorphicAuthValueFactoryProvider.Binder<>(ImmutableSet.of(Account.class, DisabledPermittedAccount.class)))
                                                                      .setMapper(SystemMapper.getMapper())
                                                                      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
                                                                      .addResource(new SecureStorageController(storageCredentialGenerator))
                                                                      .build();


  @Test
  void testGetCredentials() throws Exception {
    ExternalServiceCredentials credentials = resources.getJerseyTest()
                                                      .target("/v1/storage/auth")
                                                      .request()
                                                      .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                                                      .get(ExternalServiceCredentials.class);

    assertThat(credentials.getPassword()).isNotEmpty();
    assertThat(credentials.getUsername()).isNotEmpty();
  }

  @Test
  void testGetCredentialsBadAuth() throws Exception {
    Response response = resources.getJerseyTest()
                                 .target("/v1/storage/auth")
                                 .request()
                                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.INVVALID_NUMBER, AuthHelper.INVALID_PASSWORD))
                                 .get();

    assertThat(response.getStatus()).isEqualTo(401);
  }


}
