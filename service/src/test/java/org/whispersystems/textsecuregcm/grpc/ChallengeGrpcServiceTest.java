package org.whispersystems.textsecuregcm.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.signal.chat.challenge.AnswerChallengeRequest;
import org.signal.chat.challenge.AnswerChallengeResponse;
import org.signal.chat.challenge.ChallengeGrpc;
import org.signal.chat.challenge.RequestPushChallengeRequest;
import org.signal.chat.challenge.RequestPushChallengeResponse;
import org.whispersystems.textsecuregcm.captcha.InvalidCaptchaArgumentException;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.limits.RateLimitChallengeManager;
import org.whispersystems.textsecuregcm.push.NotPushRegisteredException;
import org.whispersystems.textsecuregcm.spam.ChallengeConstraintChecker;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;

public class ChallengeGrpcServiceTest extends
    SimpleBaseGrpcTest<ChallengeGrpcService, ChallengeGrpc.ChallengeBlockingStub> {

  @Mock
  private AccountsManager accountsManager;

  @Mock
  private Account account;

  @Mock
  private RateLimitChallengeManager rateLimitChallengeManager;

  @Mock
  private ChallengeConstraintChecker challengeConstraintChecker;

  @Override
  protected ChallengeGrpcService createServiceBeforeEachTest() {
    when(account.getAccountIdentifier()).thenReturn(AUTHENTICATED_ACI);
    when(accountsManager.getByAccountIdentifier(AUTHENTICATED_ACI)).thenReturn(Optional.of(account));
    return new ChallengeGrpcService(
        accountsManager,
        rateLimitChallengeManager,
        challengeConstraintChecker
    );
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void handlePushChallengeResponse(final boolean success) throws RateLimitExceededException {
    when(challengeConstraintChecker.challengeConstraintsGrpc(account))
        .thenReturn(new ChallengeConstraintChecker.ChallengeConstraints(true, Optional.empty()));
    when(rateLimitChallengeManager.answerPushChallenge(account, "foo")).thenReturn(success);

    final AnswerChallengeResponse response = authenticatedServiceStub().answerChallenge(
        AnswerChallengeRequest.newBuilder()
            .setToken("token")
            .setPush(AnswerChallengeRequest.AnswerPushChallengeRequest.newBuilder()
                .setChallenge("foo")
                .build())
            .build());

    assertEquals(success, response.getSuccess());
  }

  @Test
  void handlePushChallengeResponseNotPermitted() throws RateLimitExceededException {
    when(challengeConstraintChecker.challengeConstraintsGrpc(account))
        .thenReturn(new ChallengeConstraintChecker.ChallengeConstraints(false, Optional.empty()));
    when(rateLimitChallengeManager.answerPushChallenge(account, "foo")).thenReturn(true);

    final StatusRuntimeException statusRuntimeException = assertThrows(StatusRuntimeException.class,
        () -> authenticatedServiceStub().answerChallenge(
            AnswerChallengeRequest.newBuilder()
                .setToken("token")
                .setPush(AnswerChallengeRequest.AnswerPushChallengeRequest.newBuilder()
                    .setChallenge("foo")
                    .build())
                .build()));

    assertEquals(Status.RESOURCE_EXHAUSTED.getCode(), statusRuntimeException.getStatus().getCode());

    verify(rateLimitChallengeManager, never()).answerPushChallenge(any(), any());
  }

  @Test
  void handlePushChallengeResponseRateLimitExceeded() throws RateLimitExceededException {
    when(challengeConstraintChecker.challengeConstraintsGrpc(account)).thenReturn(
        new ChallengeConstraintChecker.ChallengeConstraints(true, Optional.empty()));
    final Duration retryAfter = Duration.ofMinutes(1);
    doThrow(new RateLimitExceededException(retryAfter)).when(rateLimitChallengeManager)
        .answerPushChallenge(account, "foo");

    //noinspection ResultOfMethodCallIgnored
    GrpcTestUtils.assertRateLimitExceeded(retryAfter,
        () -> authenticatedServiceStub().answerChallenge(AnswerChallengeRequest.newBuilder()
            .setToken("token")
            .setPush(AnswerChallengeRequest.AnswerPushChallengeRequest.newBuilder()
                .setChallenge("foo")
                .build())
            .build()));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void handleCaptchaChallengeResponse(final boolean captchaSuccess)
      throws RateLimitExceededException, IOException, InvalidCaptchaArgumentException {
    when(challengeConstraintChecker.challengeConstraintsGrpc(account)).thenReturn(
        new ChallengeConstraintChecker.ChallengeConstraints(true, Optional.empty()));
    when(rateLimitChallengeManager.answerCaptchaChallenge(any(), any(), any(), any(), any())).thenReturn(
        captchaSuccess);
    final AnswerChallengeResponse response = authenticatedServiceStub().answerChallenge(
        AnswerChallengeRequest.newBuilder()
            .setToken("token")
            .setCaptcha(AnswerChallengeRequest.AnswerCaptchaChallengeRequest.newBuilder()
                .setCaptcha("captcha")
                .build())
            .build());
    assertEquals(captchaSuccess, response.getSuccess());
  }

  @Test
  void handleCaptchaChallengeResponseRateLimitExceeded()
      throws RateLimitExceededException, IOException, InvalidCaptchaArgumentException {
    when(challengeConstraintChecker.challengeConstraintsGrpc(account)).thenReturn(
        new ChallengeConstraintChecker.ChallengeConstraints(true, Optional.empty()));
    final Duration retryAfter = Duration.ofMinutes(1);
    doThrow(new RateLimitExceededException(retryAfter)).when(rateLimitChallengeManager)
        .answerCaptchaChallenge(any(), any(), any(), any(), any());

    //noinspection ResultOfMethodCallIgnored
    GrpcTestUtils.assertRateLimitExceeded(retryAfter,
        () -> authenticatedServiceStub().answerChallenge(AnswerChallengeRequest.newBuilder()
            .setToken("token")
            .setCaptcha(AnswerChallengeRequest.AnswerCaptchaChallengeRequest.newBuilder()
                .setCaptcha("captcha")
                .build())
            .build()));
  }

  @Test
  void emptyRequestType() {
    when(challengeConstraintChecker.challengeConstraintsGrpc(account)).thenReturn(
        new ChallengeConstraintChecker.ChallengeConstraints(true, Optional.empty()));
    GrpcTestUtils.assertStatusInvalidArgument(
        () -> authenticatedServiceStub().answerChallenge(AnswerChallengeRequest.newBuilder().build()));
  }

  @Test
  void emptyToken() {
    when(challengeConstraintChecker.challengeConstraintsGrpc(account)).thenReturn(
        new ChallengeConstraintChecker.ChallengeConstraints(true, Optional.empty()));
    GrpcTestUtils.assertStatusInvalidArgument(
        () -> authenticatedServiceStub().answerChallenge(AnswerChallengeRequest.newBuilder()
            .setPush(AnswerChallengeRequest.AnswerPushChallengeRequest.newBuilder()
                .build())
            .build()));
  }

  @Test
  void emptyPushChallenge() {
    when(challengeConstraintChecker.challengeConstraintsGrpc(account)).thenReturn(
        new ChallengeConstraintChecker.ChallengeConstraints(true, Optional.empty()));
    GrpcTestUtils.assertStatusInvalidArgument(
        () -> authenticatedServiceStub().answerChallenge(AnswerChallengeRequest.newBuilder()
            .setToken("token")
            .setPush(AnswerChallengeRequest.AnswerPushChallengeRequest.newBuilder()
                .build())
            .build()));
  }

  @Test
  void emptyCaptcha() {
    when(challengeConstraintChecker.challengeConstraintsGrpc(account)).thenReturn(
        new ChallengeConstraintChecker.ChallengeConstraints(true, Optional.empty()));
    GrpcTestUtils.assertStatusInvalidArgument(
        () -> authenticatedServiceStub().answerChallenge(AnswerChallengeRequest.newBuilder()
            .setToken("token")
            .setCaptcha(AnswerChallengeRequest.AnswerCaptchaChallengeRequest.newBuilder()
                .build())
            .build()));
  }

  @Test
  void requestPushChallenge() throws NotPushRegisteredException {
    when(challengeConstraintChecker.challengeConstraintsGrpc(account)).thenReturn(
        new ChallengeConstraintChecker.ChallengeConstraints(true, Optional.empty()));

    final RequestPushChallengeResponse response =
        authenticatedServiceStub().requestPushChallenge(RequestPushChallengeRequest.getDefaultInstance());

    assertEquals(RequestPushChallengeResponse.ResponseCase.SENT_PUSH_CHALLENGE, response.getResponseCase());
    verify(rateLimitChallengeManager).sendPushChallenge(account);
  }

  @Test
  void requestPushChallengeNotPermitted() throws NotPushRegisteredException {
    when(challengeConstraintChecker.challengeConstraintsGrpc(account)).thenReturn(
        new ChallengeConstraintChecker.ChallengeConstraints(false, Optional.empty()));

    final StatusRuntimeException statusRuntimeException = assertThrows(StatusRuntimeException.class, () ->
        authenticatedServiceStub().requestPushChallenge(RequestPushChallengeRequest.getDefaultInstance()));

    assertEquals(Status.RESOURCE_EXHAUSTED.getCode(), statusRuntimeException.getStatus().getCode());

    verify(rateLimitChallengeManager, never()).sendPushChallenge(any());
  }

  @Test
  void requestPushChallengeNoPushToken() throws NotPushRegisteredException {
    when(challengeConstraintChecker.challengeConstraintsGrpc(account)).thenReturn(
        new ChallengeConstraintChecker.ChallengeConstraints(true, Optional.empty()));

    doThrow(NotPushRegisteredException.class)
        .when(rateLimitChallengeManager).sendPushChallenge(any());

    final RequestPushChallengeResponse response =
        authenticatedServiceStub().requestPushChallenge(RequestPushChallengeRequest.getDefaultInstance());

    assertEquals(RequestPushChallengeResponse.ResponseCase.NO_PUSH_TOKEN, response.getResponseCase());
  }
}
