package org.whispersystems.textsecuregcm.grpc;

import java.io.IOException;
import org.signal.chat.challenge.AnswerChallengeRequest;
import org.signal.chat.challenge.AnswerChallengeResponse;
import org.signal.chat.challenge.SimpleChallengeGrpc;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticationUtil;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.limits.RateLimitChallengeManager;
import org.whispersystems.textsecuregcm.spam.ChallengeConstraintChecker;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;

public class ChallengeGrpcService extends SimpleChallengeGrpc.ChallengeImplBase {

  private final AccountsManager accountsManager;
  private final RateLimitChallengeManager rateLimitChallengeManager;
  private final ChallengeConstraintChecker challengeConstraintChecker;

  public ChallengeGrpcService(final AccountsManager accountsManager,
      final RateLimitChallengeManager rateLimitChallengeManager,
      final ChallengeConstraintChecker challengeConstraintChecker) {
    this.accountsManager = accountsManager;
    this.rateLimitChallengeManager = rateLimitChallengeManager;
    this.challengeConstraintChecker = challengeConstraintChecker;
  }

  @Override
  public AnswerChallengeResponse handleChallengeResponse(final AnswerChallengeRequest request)
      throws RateLimitExceededException, IOException {

    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();
    final Account account = accountsManager.getByAccountIdentifier(authenticatedDevice.accountIdentifier())
        .orElseThrow(() -> GrpcExceptions.invalidCredentials("invalid credentials"));

    final ChallengeConstraintChecker.ChallengeConstraints constraints = challengeConstraintChecker.challengeConstraintsGrpc(
        account);

    final boolean success = switch (request.getRequestCase()) {
      case PUSH -> constraints.pushPermitted() && rateLimitChallengeManager.answerPushChallenge(account,
          request.getPush().getChallenge());
      case CAPTCHA -> rateLimitChallengeManager.answerCaptchaChallenge(
          account,
          request.getCaptcha().getCaptcha(),
          RequestAttributesUtil.getRemoteAddress().getHostAddress(),
          RequestAttributesUtil.getUserAgent().orElse(null),
          constraints.captchaScoreThreshold());
      case REQUEST_NOT_SET -> throw GrpcExceptions.fieldViolation("request", "Must set request type");
    };

    return AnswerChallengeResponse.newBuilder()
        .setSuccess(success)
        .build();
  }
}
