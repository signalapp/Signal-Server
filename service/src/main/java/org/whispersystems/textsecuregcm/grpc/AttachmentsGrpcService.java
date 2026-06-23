/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import java.security.SecureRandom;
import java.time.Clock;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Map;
import java.util.stream.IntStream;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import org.signal.chat.attachments.GetStickerUploadFormRequest;
import org.signal.chat.attachments.GetStickerUploadFormResponse;
import org.signal.chat.attachments.GetUploadFormRequest;
import org.signal.chat.attachments.GetUploadFormResponse;
import org.signal.chat.attachments.SimpleAttachmentsGrpc;
import org.signal.chat.common.S3UploadForm;
import org.signal.chat.common.UploadForm;
import org.signal.chat.errors.FailedPrecondition;
import org.whispersystems.textsecuregcm.attachments.AttachmentGenerator;
import org.whispersystems.textsecuregcm.attachments.AttachmentUtil;
import org.whispersystems.textsecuregcm.attachments.GcsAttachmentGenerator;
import org.whispersystems.textsecuregcm.attachments.TusAttachmentGenerator;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticationUtil;
import org.whispersystems.textsecuregcm.controllers.AttachmentControllerV4;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.controllers.StickerController;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.s3.PostPolicyGenerator;

public class AttachmentsGrpcService extends SimpleAttachmentsGrpc.AttachmentsImplBase {

  private final ExperimentEnrollmentManager experimentEnrollmentManager;
  private final RateLimiter countRateLimiter;
  private final RateLimiter bytesRateLimiter;
  private final RateLimiter stickerPackLimiter;
  private final long maxUploadLength;
  private final Map<Integer, AttachmentGenerator> attachmentGenerators;
  private final PostPolicyGenerator stickerPolicyGenerator;
  private final Clock clock;
  private final SecureRandom secureRandom;

  private static final S3UploadForm PROTOTYPE_STICKER_UPLOAD_FORM = S3UploadForm.newBuilder()
      .setAcl(PostPolicyGenerator.ACL)
      .setAlgorithm(PostPolicyGenerator.ALGORITHM)
      .build();

  private static final String ATTACHMENT_SIZE_NAME =
      MetricsUtil.name(AttachmentsGrpcService.class, "attachmentSize");

  public AttachmentsGrpcService(
      final ExperimentEnrollmentManager experimentEnrollmentManager,
      final RateLimiters rateLimiters,
      final GcsAttachmentGenerator gcsAttachmentGenerator,
      final TusAttachmentGenerator tusAttachmentGenerator,
      final PostPolicyGenerator stickerPolicyGenerator,
      final long maxUploadLength,
      final Clock clock) {
    this.experimentEnrollmentManager = experimentEnrollmentManager;
    this.countRateLimiter = rateLimiters.getAttachmentLimiter();
    this.bytesRateLimiter = rateLimiters.getAttachmentBytesLimiter();
    this.stickerPackLimiter = rateLimiters.getStickerPackLimiter();
    this.stickerPolicyGenerator = stickerPolicyGenerator;
    this.maxUploadLength = maxUploadLength;
    this.clock = clock;
    this.secureRandom = new SecureRandom();
    this.attachmentGenerators = Map.of(
        2, gcsAttachmentGenerator,
        3, tusAttachmentGenerator);
  }

  @Override
  public GetUploadFormResponse getUploadForm(final GetUploadFormRequest request) throws RateLimitExceededException {
    if (request.getUploadLength() > maxUploadLength) {
      return GetUploadFormResponse.newBuilder()
          .setExceedsMaxUploadLength(FailedPrecondition.getDefaultInstance())
          .build();
    }
    final AuthenticatedDevice auth = AuthenticationUtil.requireAuthenticatedDevice();

    countRateLimiter.validate(auth.accountIdentifier());
    try {
      // Ideally we'd check these two rate limits transactionally and only update them if both permits were acquired.
      // However, just undoing the first modification if the second one fails is close enough for our purposes
      bytesRateLimiter.validate(auth.accountIdentifier(), request.getUploadLength());
    } catch (RateLimitExceededException e) {
      countRateLimiter.restorePermits(auth.accountIdentifier(), 1);
      throw e;
    }

    DistributionSummary.builder(ATTACHMENT_SIZE_NAME)
        .tags(Tags.of(UserAgentTagUtil.getPlatformTag(RequestAttributesUtil.getUserAgent().orElse(null))))
        .register(Metrics.globalRegistry)
        .record(request.getUploadLength());

    final String key = AttachmentUtil.generateAttachmentKey(secureRandom);
    final boolean useCdn3 = this.experimentEnrollmentManager.isEnrolled(auth.accountIdentifier(),
        AttachmentUtil.CDN3_EXPERIMENT_NAME);
    final int cdn = useCdn3 ? 3 : 2;
    final AttachmentGenerator.Descriptor descriptor =
        this.attachmentGenerators.get(cdn).generateAttachment(key, request.getUploadLength());
    return GetUploadFormResponse.newBuilder().setUploadForm(UploadForm.newBuilder()
        .setCdn(cdn)
        .setKey(key)
        .putAllHeaders(descriptor.headers())
        .setSignedUploadLocation(descriptor.signedUploadLocation()))
        .build();
  }

  @Override
  public GetStickerUploadFormResponse getStickerUploadForm(final GetStickerUploadFormRequest request)
      throws RateLimitExceededException {

    stickerPackLimiter.validate(AuthenticationUtil.requireAuthenticatedDevice().accountIdentifier());

    final Instant currentTime = clock.instant();
    final String packId;
    {
      final byte[] packIdBytes = new byte[16];
      secureRandom.nextBytes(packIdBytes);

      packId = HexFormat.of().formatHex(packIdBytes);
    }

    final String packLocation = "stickers/" + packId;

    final GetStickerUploadFormResponse.Builder responseBuilder = GetStickerUploadFormResponse.newBuilder()
        .setPackId(packId);

    {
      final String manifestKey = packLocation + "/manifest.proto";

      final PostPolicyGenerator.SignedPostPolicy manifestPolicy =
          stickerPolicyGenerator.createFor(manifestKey, StickerController.MAXIMUM_STICKER_MANIFEST_SIZE_BYTES, currentTime);

      responseBuilder
          .setManifestUploadForm(PROTOTYPE_STICKER_UPLOAD_FORM.toBuilder()
              .setKey(manifestKey)
              .setCredential(manifestPolicy.credential())
              .setDate(manifestPolicy.formattedTimestamp())
              .setPolicy(manifestPolicy.encodedPolicy())
              .setSignature(manifestPolicy.signature())
              .build());
    }

    IntStream.range(0, request.getStickerCount())
        .mapToObj(i -> {
          final String stickerKey = packLocation + "/full/" + i;
          final PostPolicyGenerator.SignedPostPolicy stickerPolicy =
              stickerPolicyGenerator.createFor(stickerKey, StickerController.MAXIMUM_STICKER_SIZE_BYTES, currentTime);

          return PROTOTYPE_STICKER_UPLOAD_FORM.toBuilder()
              .setKey(stickerKey)
              .setCredential(stickerPolicy.credential())
              .setDate(stickerPolicy.formattedTimestamp())
              .setPolicy(stickerPolicy.encodedPolicy())
              .setSignature(stickerPolicy.signature())
              .build();
        })
        .forEach(responseBuilder::addStickerUploadForms);

    return responseBuilder.build();
  }
}
