package org.whispersystems.textsecuregcm.tests.controllers;

import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.ClassRule;
import org.junit.Test;
import org.whispersystems.dropwizard.simpleauth.AuthValueFactoryProvider;
import org.whispersystems.textsecuregcm.configuration.AttachmentsConfiguration;
import org.whispersystems.textsecuregcm.controllers.AttachmentController;
import org.whispersystems.textsecuregcm.entities.AttachmentDescriptor;
import org.whispersystems.textsecuregcm.entities.AttachmentUri;
import org.whispersystems.textsecuregcm.federation.FederatedClientManager;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.s3.UrlSigner;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.SystemMapper;

import java.net.MalformedURLException;

import io.dropwizard.testing.junit.ResourceTestRule;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AttachmentControllerTest {

  private static AttachmentsConfiguration configuration          = mock(AttachmentsConfiguration.class);
  private static FederatedClientManager   federatedClientManager = mock(FederatedClientManager.class  );
  private static RateLimiters             rateLimiters           = mock(RateLimiters.class            );
  private static RateLimiter              rateLimiter            = mock(RateLimiter.class             );

  private static UrlSigner urlSigner;

  static {
    when(configuration.getAccessKey()).thenReturn("accessKey");
    when(configuration.getAccessSecret()).thenReturn("accessSecret");
    when(configuration.getBucket()).thenReturn("attachment-bucket");

    when(rateLimiters.getAttachmentLimiter()).thenReturn(rateLimiter);
    urlSigner = new UrlSigner(configuration);
  }

  @ClassRule
  public static final ResourceTestRule resources = ResourceTestRule.builder()
                                                                   .addProvider(AuthHelper.getAuthFilter())
                                                                   .addProvider(new AuthValueFactoryProvider.Binder())
                                                                   .setMapper(SystemMapper.getMapper())
                                                                   .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
                                                                   .addResource(new AttachmentController(rateLimiters, federatedClientManager, urlSigner))
                                                                   .build();

  @Test
  public void testAcceleratedPut() {
    AttachmentDescriptor descriptor = resources.getJerseyTest()
                                               .target("/v1/attachments/")
                                               .request()
                                               .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                                               .get(AttachmentDescriptor.class);

    assertThat(descriptor.getLocation()).startsWith("https://attachment-bucket.s3-accelerate.amazonaws.com");
    assertThat(descriptor.getId()).isGreaterThan(0);
    assertThat(descriptor.getIdString()).isNotBlank();
  }

  @Test
  public void testUnacceleratedPut() {
    AttachmentDescriptor descriptor = resources.getJerseyTest()
                                               .target("/v1/attachments/")
                                               .request()
                                               .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER_TWO, AuthHelper.VALID_PASSWORD_TWO))
                                               .get(AttachmentDescriptor.class);

    assertThat(descriptor.getLocation()).startsWith("https://s3.amazonaws.com");
    assertThat(descriptor.getId()).isGreaterThan(0);
    assertThat(descriptor.getIdString()).isNotBlank();
  }

  @Test
  public void testAcceleratedGet() throws MalformedURLException {
    AttachmentUri uri = resources.getJerseyTest()
                                        .target("/v1/attachments/1234")
                                        .request()
                                        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                                        .get(AttachmentUri.class);

    assertThat(uri.getLocation().getHost()).isEqualTo("attachment-bucket.s3-accelerate.amazonaws.com");
  }

  @Test
  public void testUnacceleratedGet() throws MalformedURLException {
    AttachmentUri uri = resources.getJerseyTest()
                                 .target("/v1/attachments/1234")
                                 .request()
                                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER_TWO, AuthHelper.VALID_PASSWORD_TWO))
                                 .get(AttachmentUri.class);

    assertThat(uri.getLocation().getHost()).isEqualTo("s3.amazonaws.com");
  }

}
