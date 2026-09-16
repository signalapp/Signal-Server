package org.whispersystems.textsecuregcm.grpc;

import java.io.IOException;
import com.google.protobuf.Empty;
import org.signal.chat.challenge.AnswerChallengeRequest;
import org.signal.chat.challenge.AnswerChallengeResponse;
import org.signal.chat.challenge.RequestPushChallengeRequest;
import org.signal.chat.challenge.RequestPushChallengeResponse;
import org.signal.chat.challenge.SimpleChallengeGrpc;
import org.signal.chat.errors.FailedPrecondition;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticationUtil;
import org.whispersystems.textsecuregcm.captcha.InvalidCaptchaArgumentException;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.limits.RateLimitChallengeManager;
import org.whispersystems.textsecuregcm.push.NotPushRegisteredException;
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
  public AnswerChallengeResponse answerChallenge(final AnswerChallengeRequest request)
      throws RateLimitExceededException, IOException {

    final Account account = requireAuthenticatedAccount();
    final ChallengeConstraintChecker.ChallengeConstraints constraints =
        challengeConstraintChecker.challengeConstraintsGrpc(account);

    final boolean success = switch (request.getRequestCase()) {
      case PUSH -> {
        if (!constraints.pushPermitted()) {
          throw GrpcExceptions.rateLimitExceeded(null);
        }

        yield rateLimitChallengeManager.answerPushChallenge(account, request.getPush().getChallenge());
      }
      case CAPTCHA -> {
        try {
          yield rateLimitChallengeManager.answerCaptchaChallenge(
              account,
              request.getCaptcha().getCaptcha(),
              RequestAttributesUtil.getRemoteAddress().getHostAddress(),
              RequestAttributesUtil.getUserAgent().orElse(null),
              constraints.captchaScoreThreshold());
        } catch (final InvalidCaptchaArgumentException e) {
          throw GrpcExceptions.invalidArguments(e.getMessage());
        }
      }
      case REQUEST_NOT_SET -> throw GrpcExceptions.fieldViolation("request", "Must set request type");
    };

    return AnswerChallengeResponse.newBuilder()
        .setSuccess(success)
        .build();
  }

  @Override
  public RequestPushChallengeResponse requestPushChallenge(final RequestPushChallengeRequest request) {
    final Account account = requireAuthenticatedAccount();
    final ChallengeConstraintChecker.ChallengeConstraints constraints =
        challengeConstraintChecker.challengeConstraintsGrpc(account);

    if (!constraints.pushPermitted()) {
      throw GrpcExceptions.rateLimitExceeded(null);
    }

    try {
      rateLimitChallengeManager.sendPushChallenge(account);
    } catch (NotPushRegisteredException _) {
      return RequestPushChallengeResponse.newBuilder()
          .setNoPushToken(FailedPrecondition.getDefaultInstance())
          .build();
    }

    return RequestPushChallengeResponse.newBuilder()
        .setSentPushChallenge(Empty.getDefaultInstance())
        .build();
  }

  private Account requireAuthenticatedAccount() {
    return accountsManager.getByAccountIdentifier(AuthenticationUtil.requireAuthenticatedDevice().accountIdentifier())
        .orElseThrow(() -> GrpcExceptions.invalidCredentials("invalid credentials"));
  }
}
