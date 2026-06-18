/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import io.dropwizard.auth.Auth;
import io.dropwizard.util.DataSize;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Instant;
import java.util.HexFormat;
import java.util.LinkedList;
import java.util.List;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.entities.StickerPackFormUploadAttributes;
import org.whispersystems.textsecuregcm.entities.StickerPackFormUploadAttributes.StickerPackFormUploadItem;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.s3.PostPolicyGenerator;

@Path("/v1/sticker")
@Tag(name = "Stickers")
public class StickerController {

  private final RateLimiters        rateLimiters;
  private final PostPolicyGenerator policyGenerator;
  private final Clock               clock;

  public static final int MAXIMUM_STICKER_SIZE_BYTES = (int) DataSize.kibibytes(300 + 1).toBytes(); // add 1 kiB for encryption overhead
  public static final int MAXIMUM_STICKER_MANIFEST_SIZE_BYTES = (int) DataSize.kibibytes(10).toBytes();

  public StickerController(final RateLimiters rateLimiters,
      final PostPolicyGenerator postPolicyGenerator,
      final Clock clock) {
    this.rateLimiters    = rateLimiters;
    this.policyGenerator = postPolicyGenerator;
    this.clock           = clock;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/pack/form/{count}")
  public StickerPackFormUploadAttributes getStickersForm(@Auth AuthenticatedDevice auth,
      @PathParam("count") @Min(1) @Max(201) int stickerCount)
      throws RateLimitExceededException {
    rateLimiters.getStickerPackLimiter().validate(auth.accountIdentifier());

    final Instant currentTime = clock.instant();
    final String packId = generatePackId();
    final String packLocation = "stickers/" + packId;
    final String manifestKey = packLocation + "/manifest.proto";
    final PostPolicyGenerator.SignedPostPolicy manifestPolicy =
        policyGenerator.createFor(manifestKey, MAXIMUM_STICKER_MANIFEST_SIZE_BYTES, currentTime);

    final StickerPackFormUploadItem manifest = new StickerPackFormUploadItem(-1, manifestKey, manifestPolicy.credential(),
        PostPolicyGenerator.ACL, PostPolicyGenerator.ALGORITHM,
        manifestPolicy.formattedTimestamp(), manifestPolicy.encodedPolicy(), manifestPolicy.signature());

    final List<StickerPackFormUploadItem> stickers = new LinkedList<>();

    for (int i = 0; i < stickerCount; i++) {
      final String stickerKey = packLocation + "/full/" + i;
      final PostPolicyGenerator.SignedPostPolicy stickerPolicy =
          policyGenerator.createFor(stickerKey, MAXIMUM_STICKER_SIZE_BYTES, currentTime);
      stickers.add(new StickerPackFormUploadItem(i, stickerKey, stickerPolicy.credential(), PostPolicyGenerator.ACL, PostPolicyGenerator.ALGORITHM,
          manifestPolicy.formattedTimestamp(), stickerPolicy.encodedPolicy(), stickerPolicy.signature()));
    }

    return new StickerPackFormUploadAttributes(packId, manifest, stickers);
  }

  private String generatePackId() {
    final byte[] object = new byte[16];
    new SecureRandom().nextBytes(object);

    return HexFormat.of().formatHex(object);
  }

}
