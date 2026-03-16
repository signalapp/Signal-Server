/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import java.security.SecureRandom;
import java.util.Map;
import org.signal.chat.attachments.GetUploadFormRequest;
import org.signal.chat.attachments.GetUploadFormResponse;
import org.signal.chat.attachments.SimpleAttachmentsGrpc;
import org.signal.chat.common.UploadForm;
import org.whispersystems.textsecuregcm.attachments.AttachmentGenerator;
import org.whispersystems.textsecuregcm.attachments.GcsAttachmentGenerator;
import org.whispersystems.textsecuregcm.attachments.TusAttachmentGenerator;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticationUtil;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.attachments.AttachmentUtil;

public class AttachmentsGrpcService extends SimpleAttachmentsGrpc.AttachmentsImplBase {

  private final ExperimentEnrollmentManager experimentEnrollmentManager;
  private final RateLimiter rateLimiter;
  private final Map<Integer, AttachmentGenerator> attachmentGenerators;
  private final SecureRandom secureRandom;

  public AttachmentsGrpcService(
      final ExperimentEnrollmentManager experimentEnrollmentManager,
      final RateLimiters rateLimiters,
      final GcsAttachmentGenerator gcsAttachmentGenerator,
      final TusAttachmentGenerator tusAttachmentGenerator) {
    this.experimentEnrollmentManager = experimentEnrollmentManager;
    this.rateLimiter = rateLimiters.getAttachmentLimiter();
    this.secureRandom = new SecureRandom();
    this.attachmentGenerators = Map.of(
        2, gcsAttachmentGenerator,
        3, tusAttachmentGenerator);
  }

  @Override
  public GetUploadFormResponse getUploadForm(final GetUploadFormRequest request) throws RateLimitExceededException {
    final AuthenticatedDevice auth = AuthenticationUtil.requireAuthenticatedDevice();
    rateLimiter.validate(auth.accountIdentifier());
    final String key = AttachmentUtil.generateAttachmentKey(secureRandom);
    final boolean useCdn3 = this.experimentEnrollmentManager.isEnrolled(auth.accountIdentifier(),
        AttachmentUtil.CDN3_EXPERIMENT_NAME);
    final int cdn = useCdn3 ? 3 : 2;
    final AttachmentGenerator.Descriptor descriptor = this.attachmentGenerators.get(cdn).generateAttachment(key);
    return GetUploadFormResponse.newBuilder().setUploadForm(UploadForm.newBuilder()
        .setCdn(cdn)
        .setKey(key)
        .putAllHeaders(descriptor.headers())
        .setSignedUploadLocation(descriptor.signedUploadLocation()))
        .build();
  }
}
