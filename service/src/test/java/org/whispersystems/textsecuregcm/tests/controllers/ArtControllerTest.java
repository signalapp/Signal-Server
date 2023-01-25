/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.controllers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import io.dropwizard.auth.PolymorphicAuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.DisabledPermittedAuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.configuration.ArtServiceConfiguration;
import org.whispersystems.textsecuregcm.controllers.ArtController;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.MockHelper;
import org.whispersystems.textsecuregcm.util.SystemMapper;

@ExtendWith(DropwizardExtensionsSupport.class)
class ArtControllerTest {

  private static final ArtServiceConfiguration ART_SERVICE_CONFIGURATION = MockHelper.buildMock(
      ArtServiceConfiguration.class,
      cfg -> {
        Mockito.when(cfg.getUserAuthenticationTokenSharedSecret()).thenReturn(new byte[32]);
        Mockito.when(cfg.getUserAuthenticationTokenUserIdSecret()).thenReturn(new byte[32]);
      });
  private static final ExternalServiceCredentialsGenerator artCredentialsGenerator = ArtController.credentialsGenerator(ART_SERVICE_CONFIGURATION);
  private static final RateLimiter  rateLimiter  = mock(RateLimiter.class);
  private static final RateLimiters rateLimiters = mock(RateLimiters.class);

  private static final ResourceExtension resources = ResourceExtension.builder()
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(new PolymorphicAuthValueFactoryProvider.Binder<>(
          ImmutableSet.of(AuthenticatedAccount.class, DisabledPermittedAuthenticatedAccount.class)))
      .setMapper(SystemMapper.getMapper())
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(new ArtController(rateLimiters, artCredentialsGenerator))
      .build();

  @Test
  void testGetAuthToken() {
    when(rateLimiters.getArtPackLimiter()).thenReturn(rateLimiter);

    ExternalServiceCredentials token =
        resources.getJerseyTest()
            .target("/v1/art/auth")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .get(ExternalServiceCredentials.class);

    assertThat(token.password()).isNotEmpty();
    assertThat(token.username()).isNotEmpty();
  }
}
