package org.whispersystems.textsecuregcm.tests.controllers;

import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.ClassRule;
import org.junit.Test;
import org.whispersystems.textsecuregcm.controllers.AttachmentControllerV1;
import org.whispersystems.textsecuregcm.controllers.AttachmentControllerV2;
import org.whispersystems.textsecuregcm.entities.AttachmentDescriptorV1;
import org.whispersystems.textsecuregcm.entities.AttachmentDescriptorV2;
import org.whispersystems.textsecuregcm.entities.AttachmentUri;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.SystemMapper;

import java.net.MalformedURLException;

import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.testing.junit.ResourceTestRule;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AttachmentControllerTest {

  private static RateLimiters             rateLimiters  = mock(RateLimiters.class            );
  private static RateLimiter              rateLimiter   = mock(RateLimiter.class             );

  static {
    when(rateLimiters.getAttachmentLimiter()).thenReturn(rateLimiter);
  }

  @ClassRule
  public static final ResourceTestRule resources = ResourceTestRule.builder()
                                                                   .addProvider(AuthHelper.getAuthFilter())
                                                                   .addProvider(new AuthValueFactoryProvider.Binder<>(Account.class))
                                                                   .setMapper(SystemMapper.getMapper())
                                                                   .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
                                                                   .addResource(new AttachmentControllerV1(rateLimiters, "accessKey", "accessSecret", "attachment-bucket"))
                                                                   .addResource(new AttachmentControllerV2(rateLimiters, "accessKey", "accessSecret", "us-east-1", "attachmentv2-bucket"))
                                                                   .build();

  @Test
  public void testV2Form() {
    AttachmentDescriptorV2 descriptor = resources.getJerseyTest()
                                                 .target("/v2/attachments/form/upload")
                                                 .request()
                                                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                                                 .get(AttachmentDescriptorV2.class);

    assertThat(descriptor.getKey()).isEqualTo(descriptor.getAttachmentIdString());
    assertThat(descriptor.getAcl()).isEqualTo("private");
    assertThat(descriptor.getAlgorithm()).isEqualTo("AWS4-HMAC-SHA256");
    assertThat(descriptor.getAttachmentId()).isGreaterThan(0);
    assertThat(String.valueOf(descriptor.getAttachmentId())).isEqualTo(descriptor.getAttachmentIdString());

    String[] credentialParts = descriptor.getCredential().split("/");

    assertThat(credentialParts[0]).isEqualTo("accessKey");
    assertThat(credentialParts[2]).isEqualTo("us-east-1");
    assertThat(credentialParts[3]).isEqualTo("s3");
    assertThat(credentialParts[4]).isEqualTo("aws4_request");

    assertThat(descriptor.getDate()).isNotBlank();
    assertThat(descriptor.getPolicy()).isNotBlank();
    assertThat(descriptor.getSignature()).isNotBlank();
  }

  @Test
  public void testAcceleratedPut() {
    AttachmentDescriptorV1 descriptor = resources.getJerseyTest()
                                                 .target("/v1/attachments/")
                                                 .request()
                                                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                                                 .get(AttachmentDescriptorV1.class);

    assertThat(descriptor.getLocation()).startsWith("https://attachment-bucket.s3-accelerate.amazonaws.com");
    assertThat(descriptor.getId()).isGreaterThan(0);
    assertThat(descriptor.getIdString()).isNotBlank();
  }

  @Test
  public void testUnacceleratedPut() {
    AttachmentDescriptorV1 descriptor = resources.getJerseyTest()
                                                 .target("/v1/attachments/")
                                                 .request()
                                                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER_TWO, AuthHelper.VALID_PASSWORD_TWO))
                                                 .get(AttachmentDescriptorV1.class);

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
