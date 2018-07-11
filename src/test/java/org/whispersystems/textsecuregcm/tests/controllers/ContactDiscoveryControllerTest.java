package org.whispersystems.textsecuregcm.tests.controllers;

import io.dropwizard.testing.junit.ResourceTestRule;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.whispersystems.dropwizard.simpleauth.AuthValueFactoryProvider;
import org.whispersystems.textsecuregcm.auth.AuthorizationToken;
import org.whispersystems.textsecuregcm.configuration.ContactDiscoveryConfiguration;
import org.whispersystems.textsecuregcm.controllers.ContactDiscoveryController;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.SystemMapper;

import static org.mockito.Mockito.*;

public class ContactDiscoveryControllerTest {
  private static ContactDiscoveryConfiguration cdsConfig = mock(ContactDiscoveryConfiguration.class);

  static {
    try {
      when(cdsConfig.getUserAuthenticationTokenSharedSecret()).thenReturn(new byte[100]);
      when(cdsConfig.getUserAuthenticationTokenUserIdSecret()).thenReturn(new byte[100]);
    } catch (Exception e) {
      throw new AssertionError(e);
    }
  }
  private static ContactDiscoveryController createContactDiscoveryController(ContactDiscoveryConfiguration cdsConfig) {
    try {
      return new ContactDiscoveryController(cdsConfig);
    } catch (Exception e) {
      throw new AssertionError(e);
    }
  }

  @ClassRule
  public static final ResourceTestRule resources = ResourceTestRule.builder()
          .addProvider(AuthHelper.getAuthFilter())
          .addProvider(new AuthValueFactoryProvider.Binder())
          .setMapper(SystemMapper.getMapper())
          .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
          .addResource(createContactDiscoveryController(cdsConfig))
          .build();

  @Test
  public void testGetAuthToken() {
    resources.getJerseyTest()
            .target("/v1/contact-discovery/auth-token")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
            .get(AuthorizationToken.class);
  }
}
