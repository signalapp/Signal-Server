package org.whispersystems.textsecuregcm.tests.controllers;

import com.google.common.collect.ImmutableSet;
import io.dropwizard.auth.PolymorphicAuthValueFactoryProvider;
import io.dropwizard.testing.junit.ResourceTestRule;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Condition;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMWriter;
import org.bouncycastle.openssl.PKCS8Generator;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.whispersystems.textsecuregcm.auth.DisabledPermittedAccount;
import org.whispersystems.textsecuregcm.controllers.AttachmentControllerV1;
import org.whispersystems.textsecuregcm.controllers.AttachmentControllerV2;
import org.whispersystems.textsecuregcm.controllers.AttachmentControllerV3;
import org.whispersystems.textsecuregcm.entities.AttachmentDescriptorV1;
import org.whispersystems.textsecuregcm.entities.AttachmentDescriptorV2;
import org.whispersystems.textsecuregcm.entities.AttachmentDescriptorV3;
import org.whispersystems.textsecuregcm.entities.AttachmentUri;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.Base64;
import org.whispersystems.textsecuregcm.util.SystemMapper;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.Security;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AttachmentControllerTest {

  private static RateLimiters             rateLimiters  = mock(RateLimiters.class            );
  private static RateLimiter              rateLimiter   = mock(RateLimiter.class             );

  static {
    when(rateLimiters.getAttachmentLimiter()).thenReturn(rateLimiter);
  }

  public static final String RSA_PRIVATE_KEY_PEM;

  static {
    try {
      final Provider          provider         = new BouncyCastleProvider();
      final KeyPairGenerator  keyPairGenerator = KeyPairGenerator.getInstance("RSA", provider);
      keyPairGenerator.initialize(1024);
      final KeyPair           keyPair          = keyPairGenerator.generateKeyPair();
      final StringWriter      stringWriter     = new StringWriter();
      final PEMWriter         pemWriter        = new PEMWriter(stringWriter);
      final PKCS8Generator pkcs8Generator      = new PKCS8Generator(keyPair.getPrivate());
      pemWriter.writeObject(pkcs8Generator);
      pemWriter.close();
      RSA_PRIVATE_KEY_PEM = stringWriter.toString();
    } catch (NoSuchAlgorithmException | IOException e) {
      throw new AssertionError(e);
    }
  }

  @ClassRule
  public static final ResourceTestRule resources;

  static {
    try {
      Security.insertProviderAt(new BouncyCastleProvider(), 0);
      resources = ResourceTestRule.builder()
              .addProvider(AuthHelper.getAuthFilter())
              .addProvider(new PolymorphicAuthValueFactoryProvider.Binder<>(ImmutableSet.of(Account.class, DisabledPermittedAccount.class)))
              .setMapper(SystemMapper.getMapper())
              .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
              .addResource(new AttachmentControllerV1(rateLimiters, "accessKey", "accessSecret", "attachment-bucket"))
              .addResource(new AttachmentControllerV2(rateLimiters, "accessKey", "accessSecret", "us-east-1", "attachmentv2-bucket"))
              .addResource(new AttachmentControllerV3(rateLimiters, "some-cdn.signal.org", "signal@example.com", 1000, "/attach-here", RSA_PRIVATE_KEY_PEM))
              .build();
      Security.removeProvider(BouncyCastleProvider.PROVIDER_NAME);
    } catch (IOException | InvalidKeyException e) {
      throw new AssertionError(e);
    }
  }

  @BeforeClass
  public static void setup() {
    Security.insertProviderAt(new BouncyCastleProvider(), 0);
  }

  @AfterClass
  public static void tearDown() {
    Security.removeProvider(BouncyCastleProvider.PROVIDER_NAME);
  }

  @Test
  public void testV3Form() {
    AttachmentDescriptorV3 descriptor = resources.getJerseyTest()
            .target("/v3/attachments/form/upload")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
            .get(AttachmentDescriptorV3.class);

    assertThat(descriptor.getKey()).isNotBlank();
    assertThat(descriptor.getCdn()).isEqualTo(2);
    assertThat(descriptor.getHeaders()).hasSize(3);
    assertThat(descriptor.getHeaders()).extractingByKey("host").isEqualTo("some-cdn.signal.org");
    assertThat(descriptor.getHeaders()).extractingByKey("x-goog-resumable").isEqualTo("start");
    assertThat(descriptor.getHeaders()).extractingByKey("x-goog-content-length-range").isEqualTo("1,1000");
    assertThat(descriptor.getSignedUploadLocation()).isNotEmpty();
    assertThat(descriptor.getSignedUploadLocation()).contains("X-Goog-Signature");
    assertThat(descriptor.getSignedUploadLocation()).is(new Condition<>(x -> {
      try {
        new URL(x);
      } catch (MalformedURLException e) {
        return false;
      }
      return true;
    }, "convertible to a URL", (Object[]) null));

    final URL signedUploadLocation;
    try {
      signedUploadLocation = new URL(descriptor.getSignedUploadLocation());
    } catch (MalformedURLException e) {
      throw new AssertionError(e);
    }
    assertThat(signedUploadLocation.getHost()).isEqualTo("some-cdn.signal.org");
    assertThat(signedUploadLocation.getPath()).startsWith("/attach-here/");
    final Map<String, String> queryParamMap = new HashMap<>();
    final String[] queryTerms = signedUploadLocation.getQuery().split("&");
    for (final String queryTerm : queryTerms) {
      final String[] keyValueArray = queryTerm.split("=", 2);
      queryParamMap.put(
              URLDecoder.decode(keyValueArray[0], StandardCharsets.UTF_8),
              URLDecoder.decode(keyValueArray[1], StandardCharsets.UTF_8));
    }

    assertThat(queryParamMap).extractingByKey("X-Goog-Algorithm").isEqualTo("GOOG4-RSA-SHA256");
    assertThat(queryParamMap).extractingByKey("X-Goog-Expires").isEqualTo("90000");
    assertThat(queryParamMap).extractingByKey("X-Goog-SignedHeaders").isEqualTo("host;x-goog-content-length-range;x-goog-resumable");
    assertThat(queryParamMap).extractingByKey("X-Goog-Date", Assertions.as(InstanceOfAssertFactories.STRING)).isNotEmpty();

    final String credential = queryParamMap.get("X-Goog-Credential");
    String[] credentialParts = credential.split("/");
    assertThat(credentialParts).hasSize(5);
    assertThat(credentialParts[0]).isEqualTo("signal@example.com");
    assertThat(credentialParts[2]).isEqualTo("auto");
    assertThat(credentialParts[3]).isEqualTo("storage");
    assertThat(credentialParts[4]).isEqualTo("goog4_request");
  }

  @Test
  public void testV3FormDisabled() {
    Response response = resources.getJerseyTest()
            .target("/v3/attachments/form/upload")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.DISABLED_NUMBER, AuthHelper.DISABLED_PASSWORD))
            .get();

    assertThat(response.getStatus()).isEqualTo(401);
  }

  @Test
  public void testV2Form() throws IOException {
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

    assertThat(new String(Base64.decode(descriptor.getPolicy()))).contains("[\"content-length-range\", 1, 104857600]");
  }

  @Test
  public void testV2FormDisabled() {
    Response response = resources.getJerseyTest()
                                 .target("/v2/attachments/form/upload")
                                 .request()
                                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.DISABLED_NUMBER, AuthHelper.DISABLED_PASSWORD))
                                 .get();

    assertThat(response.getStatus()).isEqualTo(401);
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
