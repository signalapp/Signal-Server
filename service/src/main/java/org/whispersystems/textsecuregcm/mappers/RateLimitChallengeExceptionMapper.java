package org.whispersystems.textsecuregcm.mappers;

import org.whispersystems.textsecuregcm.entities.RateLimitChallenge;
import org.whispersystems.textsecuregcm.limits.RateLimitChallengeManager;
import org.whispersystems.textsecuregcm.limits.RateLimitChallengeException;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import java.util.UUID;

public class RateLimitChallengeExceptionMapper implements ExceptionMapper<RateLimitChallengeException> {

  private final RateLimitChallengeManager rateLimitChallengeManager;

  public RateLimitChallengeExceptionMapper(final RateLimitChallengeManager rateLimitChallengeManager) {
    this.rateLimitChallengeManager = rateLimitChallengeManager;
  }

  @Override
  public Response toResponse(final RateLimitChallengeException exception) {
    return Response.status(428)
        .entity(new RateLimitChallenge(UUID.randomUUID().toString(), rateLimitChallengeManager.getChallengeOptions(exception.getAccount())))
        .header("Retry-After", exception.getRetryAfter().toSeconds())
        .build();
  }
}
