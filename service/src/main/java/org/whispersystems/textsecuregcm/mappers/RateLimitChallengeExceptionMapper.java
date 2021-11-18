/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.mappers;

import java.util.UUID;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import org.whispersystems.textsecuregcm.entities.RateLimitChallenge;
import org.whispersystems.textsecuregcm.limits.RateLimitChallengeException;
import org.whispersystems.textsecuregcm.limits.RateLimitChallengeOptionManager;

public class RateLimitChallengeExceptionMapper implements ExceptionMapper<RateLimitChallengeException> {

  private final RateLimitChallengeOptionManager rateLimitChallengeOptionManager;

  public RateLimitChallengeExceptionMapper(final RateLimitChallengeOptionManager rateLimitChallengeOptionManager) {
    this.rateLimitChallengeOptionManager = rateLimitChallengeOptionManager;
  }

  @Override
  public Response toResponse(final RateLimitChallengeException exception) {
    return Response.status(428)
        .entity(new RateLimitChallenge(UUID.randomUUID().toString(),
            rateLimitChallengeOptionManager.getChallengeOptions(exception.getAccount())))
        .header("Retry-After", exception.getRetryAfter().toSeconds())
        .build();
  }

}
