package org.whispersystems.textsecuregcm.tests.controllers;

import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.configuration.CallLinkConfiguration;
import org.whispersystems.textsecuregcm.controllers.CallLinkController;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.MockUtils;
import org.whispersystems.textsecuregcm.util.SystemMapper;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@ExtendWith(DropwizardExtensionsSupport.class)
public class CallLinkControllerTest {
  private static final CallLinkConfiguration callLinkConfiguration = MockUtils.buildMock(
      CallLinkConfiguration.class,
      cfg -> Mockito.when(cfg.userAuthenticationTokenSharedSecret()).thenReturn(new byte[32])
  );
  private static final ExternalServiceCredentialsGenerator callLinkCredentialsGenerator = CallLinkController.credentialsGenerator(callLinkConfiguration);

  private static final ResourceExtension resources = ResourceExtension.builder()
      .setMapper(SystemMapper.jsonMapper())
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(new CallLinkController(callLinkCredentialsGenerator))
      .build();

  @Test
  void testGetAuth() {
    ExternalServiceCredentials credentials = resources.getJerseyTest()
        .target("/v1/call-link/auth")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(ExternalServiceCredentials.class);

    assertThat(credentials.username()).isNotEmpty();
    assertThat(credentials.password()).isNotEmpty();
  }
}
