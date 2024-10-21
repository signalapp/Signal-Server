/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.whispersystems.textsecuregcm.attachments.GcsAttachmentGenerator;
import org.whispersystems.textsecuregcm.attachments.TusAttachmentGenerator;
import org.whispersystems.textsecuregcm.attachments.TusConfiguration;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretBytes;
import org.whispersystems.textsecuregcm.entities.AttachmentDescriptorV3;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.MockUtils;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

@ExtendWith(DropwizardExtensionsSupport.class)
class AttachmentControllerV4Test {

  private static final RateLimiter RATE_LIMITER = mock(RateLimiter.class);

  private static final RateLimiters RATE_LIMITERS = MockUtils.buildMock(RateLimiters.class, rateLimiters ->
      when(rateLimiters.getAttachmentLimiter()).thenReturn(RATE_LIMITER));


  private static final String CDN3_ENABLED_CREDS = AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD);
  private static final String CDN3_DISABLED_CREDS = AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_TWO, AuthHelper.VALID_PASSWORD_TWO);
  private static final ExperimentEnrollmentManager EXPERIMENT_MANAGER = MockUtils.buildMock(ExperimentEnrollmentManager.class, mgr -> {
    when(mgr.isEnrolled(AuthHelper.VALID_UUID, AttachmentControllerV4.CDN3_EXPERIMENT_NAME)).thenReturn(true);
    when(mgr.isEnrolled(AuthHelper.VALID_UUID_TWO, AttachmentControllerV4.CDN3_EXPERIMENT_NAME)).thenReturn(false);
  });

  private static final byte[] TUS_SECRET = TestRandomUtil.nextBytes(32);
  private static final String TUS_URL = "https://example.com/uploads";

  public static final String RSA_PRIVATE_KEY_PEM;

  static {
    try {
      final KeyPairGenerator  keyPairGenerator = KeyPairGenerator.getInstance("RSA");
      keyPairGenerator.initialize(1024);
      final KeyPair           keyPair          = keyPairGenerator.generateKeyPair();

      RSA_PRIVATE_KEY_PEM = "-----BEGIN PRIVATE KEY-----\n" +
          Base64.getMimeEncoder().encodeToString(keyPair.getPrivate().getEncoded()) + "\n" +
          "-----END PRIVATE KEY-----";
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    }
  }


  private static final ResourceExtension resources;

  static {
    try {
      final GcsAttachmentGenerator gcsAttachmentGenerator = new GcsAttachmentGenerator("some-cdn.signal.org",
          "signal@example.com", 1000, "/attach-here", RSA_PRIVATE_KEY_PEM);
      resources = ResourceExtension.builder()
          .addProvider(AuthHelper.getAuthFilter())
          .addProvider(new AuthValueFactoryProvider.Binder<>(AuthenticatedDevice.class))
          .setMapper(SystemMapper.jsonMapper())
          .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
          .addProvider(new AttachmentControllerV4(RATE_LIMITERS,
              gcsAttachmentGenerator,
              new TusAttachmentGenerator(new TusConfiguration(new SecretBytes(TUS_SECRET), TUS_URL)),
              EXPERIMENT_MANAGER))
          .build();
    } catch (IOException | InvalidKeyException | InvalidKeySpecException e) {
      throw new AssertionError(e);
    }
  }

  @Test
  void testV4TusForm() {
    AttachmentDescriptorV3 descriptor = resources.getJerseyTest()
        .target("/v4/attachments/form/upload")
        .request()
        .header("Authorization", CDN3_ENABLED_CREDS)
        .get(AttachmentDescriptorV3.class);
    assertThat(descriptor.cdn()).isEqualTo(3);
    assertThat(descriptor.key()).isNotBlank();
    assertThat(descriptor.signedUploadLocation()).isEqualTo(TUS_URL + "/" + "attachments");
    final String filenameb64 = descriptor.headers().get("Upload-Metadata").split(" ")[1];
    final String filename = new String(Base64.getDecoder().decode(filenameb64));
    assertThat(descriptor.key()).isEqualTo(filename);
  }

  @Test
  void testV4GcsForm() throws MalformedURLException {
    AttachmentDescriptorV3 descriptor = resources.getJerseyTest()
        .target("/v4/attachments/form/upload")
        .request()
        .header("Authorization", CDN3_DISABLED_CREDS)
        .get(AttachmentDescriptorV3.class);
    assertThat(descriptor.cdn()).isEqualTo(2);
    assertValidCdn2Response(descriptor);
  }

  private static void assertValidCdn2Response(final AttachmentDescriptorV3 descriptor) throws MalformedURLException {
    assertThat(descriptor.key()).isNotBlank();
    assertThat(descriptor.cdn()).isEqualTo(2);
    assertThat(descriptor.headers()).hasSize(3);
    assertThat(descriptor.headers()).extractingByKey("host").isEqualTo("some-cdn.signal.org");
    assertThat(descriptor.headers()).extractingByKey("x-goog-resumable").isEqualTo("start");
    assertThat(descriptor.headers()).extractingByKey("x-goog-content-length-range").isEqualTo("1,1000");
    assertThat(descriptor.signedUploadLocation()).isNotEmpty();
    assertThat(descriptor.signedUploadLocation()).contains("X-Goog-Signature");
    //noinspection ResultOfMethodCallIgnored
    assertThatNoException().isThrownBy(() -> URI.create(descriptor.signedUploadLocation()));

    final URL signedUploadLocation = URI.create(descriptor.signedUploadLocation()).toURL();
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
}
