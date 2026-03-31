/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;
import static org.whispersystems.textsecuregcm.grpc.GrpcTestUtils.assertRateLimitExceeded;

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
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.signal.chat.attachments.AttachmentsGrpc;
import org.signal.chat.attachments.GetUploadFormRequest;
import org.signal.chat.attachments.GetUploadFormResponse;
import org.signal.chat.common.UploadForm;
import org.whispersystems.textsecuregcm.attachments.AttachmentUtil;
import org.whispersystems.textsecuregcm.attachments.GcsAttachmentGenerator;
import org.whispersystems.textsecuregcm.attachments.TusAttachmentGenerator;
import org.whispersystems.textsecuregcm.attachments.TusConfiguration;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretBytes;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.util.MockUtils;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

class AttachmentsGrpcServiceTest extends
    SimpleBaseGrpcTest<AttachmentsGrpcService, AttachmentsGrpc.AttachmentsBlockingStub> {

  private static final byte[] TUS_SECRET = TestRandomUtil.nextBytes(32);
  private static final String TUS_URL = "https://example.com/uploads";
  private static final int MAX_UPLOAD_LENGTH = 1000;

  @Mock
  private ExperimentEnrollmentManager experimentEnrollmentManager;
  @Mock
  private RateLimiter rateLimiter;

  @Override
  protected AttachmentsGrpcService createServiceBeforeEachTest() {
    try {
      final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
      keyPairGenerator.initialize(1024);
      final KeyPair keyPair = keyPairGenerator.generateKeyPair();

      final String gcsPrivateKeyPem = "-----BEGIN PRIVATE KEY-----\n" +
          Base64.getMimeEncoder().encodeToString(keyPair.getPrivate().getEncoded()) + "\n" +
          "-----END PRIVATE KEY-----";
      final GcsAttachmentGenerator gcsAttachmentGenerator = new GcsAttachmentGenerator(
          "some-cdn.signal.org", "signal@example.com", 1000, "/attach-here", gcsPrivateKeyPem);
      final TusAttachmentGenerator tusAttachmentGenerator =
          new TusAttachmentGenerator(new TusConfiguration(new SecretBytes(TUS_SECRET), TUS_URL, MAX_UPLOAD_LENGTH));

      return new AttachmentsGrpcService(
          experimentEnrollmentManager,
          MockUtils.buildMock(RateLimiters.class, rateLimiters ->
              when(rateLimiters.getAttachmentLimiter()).thenReturn(rateLimiter)),
          gcsAttachmentGenerator,
          tusAttachmentGenerator);
    } catch (NoSuchAlgorithmException | IOException | InvalidKeyException | InvalidKeySpecException e) {
      throw new AssertionError(e);
    }
  }

  @ParameterizedTest
  @ValueSource(longs = {-1, 0})
  void invalidUploadLength(final long uploadLength) {
    GrpcTestUtils.assertStatusInvalidArgument(() -> authenticatedServiceStub()
        .getUploadForm(GetUploadFormRequest.newBuilder().setUploadLength(uploadLength).build()));
  }

  @Test
  void uploadLengthTooBig() {
    assertThat(authenticatedServiceStub()
        .getUploadForm(GetUploadFormRequest.newBuilder().setUploadLength(MAX_UPLOAD_LENGTH + 1).build())
        .hasExceedsMaxUploadLength()).isTrue();
  }

  @Test
  void getUploadFormCdn3() {
    when(experimentEnrollmentManager.isEnrolled(AUTHENTICATED_ACI, AttachmentUtil.CDN3_EXPERIMENT_NAME))
        .thenReturn(true);

    final GetUploadFormResponse response = authenticatedServiceStub()
        .getUploadForm(GetUploadFormRequest.newBuilder().setUploadLength(1).build());

    final UploadForm uploadForm = response.getUploadForm();
    assertThat(uploadForm.getCdn()).isEqualTo(3);
    assertThat(uploadForm.getKey()).isNotBlank();
    assertThat(uploadForm.getSignedUploadLocation()).isEqualTo(TUS_URL + "/attachments");

    final String filenameb64 = uploadForm.getHeadersMap().get("Upload-Metadata").split(" ")[1];
    final String filename = new String(Base64.getDecoder().decode(filenameb64));
    assertThat(uploadForm.getKey()).isEqualTo(filename);
  }

  @Test
  void getUploadFormCdn2() throws MalformedURLException {
    final long uploadLength = 13;
    when(experimentEnrollmentManager.isEnrolled(AUTHENTICATED_ACI, AttachmentUtil.CDN3_EXPERIMENT_NAME))
        .thenReturn(false);

    final GetUploadFormResponse response = authenticatedServiceStub()
        .getUploadForm(GetUploadFormRequest.newBuilder().setUploadLength(uploadLength).build());

    final UploadForm uploadForm = response.getUploadForm();
    assertThat(uploadForm.getCdn()).isEqualTo(2);
    assertThat(uploadForm.getKey()).isNotBlank();
    assertThat(uploadForm.getHeadersMap()).containsExactlyInAnyOrderEntriesOf(Map.of(
        "host", "some-cdn.signal.org",
        "x-goog-resumable", "start",
        "x-goog-content-length-range", "1," + uploadLength));
    assertThat(uploadForm.getSignedUploadLocation()).isNotEmpty();

    final URL signedUploadLocation = URI.create(uploadForm.getSignedUploadLocation()).toURL();
    assertThat(signedUploadLocation.getHost()).isEqualTo("some-cdn.signal.org");
    assertThat(signedUploadLocation.getPath()).startsWith("/attach-here/");
    final Map<String, String> queryParamMap = Arrays.stream(signedUploadLocation.getQuery().split("&"))
        .map(queryTerm -> queryTerm.split("=", 2))
        .collect(Collectors.toMap(
            arr -> URLDecoder.decode(arr[0], StandardCharsets.UTF_8),
            arr -> URLDecoder.decode(arr[1], StandardCharsets.UTF_8)));

    assertThat(queryParamMap).hasSize(6);
    assertThat(queryParamMap).containsAllEntriesOf(Map.of(
        "X-Goog-Algorithm", "GOOG4-RSA-SHA256",
        "X-Goog-Expires", "90000",
        "X-Goog-SignedHeaders", "host;x-goog-content-length-range;x-goog-resumable"));
    assertThat(queryParamMap)
        .extractingByKey("X-Goog-Date", Assertions.as(InstanceOfAssertFactories.STRING))
        .isNotEmpty();
    assertThat(queryParamMap)
        .extractingByKey("X-Goog-Signature", Assertions.as(InstanceOfAssertFactories.STRING))
        .isNotEmpty();

    final String credential = queryParamMap.get("X-Goog-Credential");
    final String[] credentialParts = credential.split("/");
    assertThat(credentialParts).hasSize(5);
    assertThat(credentialParts[0]).isEqualTo("signal@example.com");
    assertThat(credentialParts[2]).isEqualTo("auto");
    assertThat(credentialParts[3]).isEqualTo("storage");
    assertThat(credentialParts[4]).isEqualTo("goog4_request");
  }

  @Test
  void getUploadFormRateLimited() throws RateLimitExceededException {
    final Duration retryAfter = Duration.ofMinutes(5);
    doThrow(new RateLimitExceededException(retryAfter)).when(rateLimiter).validate(any(UUID.class));

    assertRateLimitExceeded(retryAfter, () ->
        authenticatedServiceStub().getUploadForm(GetUploadFormRequest.newBuilder().setUploadLength(1).build()));
  }
}
