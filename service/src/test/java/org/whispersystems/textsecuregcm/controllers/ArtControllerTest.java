/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.whispersystems.textsecuregcm.util.MockUtils.randomSecretBytes;

import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import java.time.Duration;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.configuration.ArtServiceConfiguration;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.MockUtils;
import org.whispersystems.textsecuregcm.util.SystemMapper;

@ExtendWith(DropwizardExtensionsSupport.class)
class ArtControllerTest {
  private static final ArtServiceConfiguration ART_SERVICE_CONFIGURATION = new ArtServiceConfiguration(
      randomSecretBytes(32), randomSecretBytes(32), Duration.ofDays(1));
  private static final ExternalServiceCredentialsGenerator artCredentialsGenerator = ArtController.credentialsGenerator(ART_SERVICE_CONFIGURATION);
  private static final RateLimiters rateLimiters = mock(RateLimiters.class);

  private static final ResourceExtension resources = ResourceExtension.builder()
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(new AuthValueFactoryProvider.Binder<>(AuthenticatedAccount.class))
      .setMapper(SystemMapper.jsonMapper())
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(new ArtController(rateLimiters, artCredentialsGenerator))
      .build();

  @Test
  void testGetAuthToken() {
    MockUtils.updateRateLimiterResponseToAllow(rateLimiters, RateLimiters.For.EXTERNAL_SERVICE_CREDENTIALS, AuthHelper.VALID_UUID);
    final ExternalServiceCredentials token =
        resources.getJerseyTest()
            .target("/v1/art/auth")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .get(ExternalServiceCredentials.class);

    assertThat(token.password()).isNotEmpty();
    assertThat(token.username()).isNotEmpty();
  }
}
