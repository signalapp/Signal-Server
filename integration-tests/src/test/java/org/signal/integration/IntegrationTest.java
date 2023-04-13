/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.integration;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.entities.AccountIdentityResponse;
import org.whispersystems.textsecuregcm.entities.CreateVerificationSessionRequest;
import org.whispersystems.textsecuregcm.entities.IncomingMessage;
import org.whispersystems.textsecuregcm.entities.IncomingMessageList;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntityList;
import org.whispersystems.textsecuregcm.entities.SendMessageResponse;
import org.whispersystems.textsecuregcm.entities.SubmitVerificationCodeRequest;
import org.whispersystems.textsecuregcm.entities.UpdateVerificationSessionRequest;
import org.whispersystems.textsecuregcm.entities.VerificationCodeRequest;
import org.whispersystems.textsecuregcm.entities.VerificationSessionResponse;
import org.whispersystems.textsecuregcm.storage.Device;

public class IntegrationTest {

  @Test
  public void testCreateAccount() throws Exception {
    final TestUser user = Operations.newRegisteredUser("+19995550101");
    try {
      final Pair<Integer, AccountIdentityResponse> execute = Operations.apiGet("/v1/accounts/whoami")
          .authorized(user)
          .execute(AccountIdentityResponse.class);
      assertEquals(200, execute.getLeft());
    } finally {
      Operations.deleteUser(user);
    }
  }

  @Test
  public void testRegistration() throws Exception {
    final UpdateVerificationSessionRequest originalRequest = new UpdateVerificationSessionRequest(
        "test", UpdateVerificationSessionRequest.PushTokenType.FCM, null, null, null, null);
    final CreateVerificationSessionRequest input = new CreateVerificationSessionRequest("+19995550102", originalRequest);

    final VerificationSessionResponse verificationSessionResponse = Operations
        .apiPost("/v1/verification/session", input)
        .executeExpectSuccess(VerificationSessionResponse.class);
    System.out.println("session created: " + verificationSessionResponse);

    final String sessionId = verificationSessionResponse.id();
    final String pushChallenge = Operations.peekVerificationSessionPushChallenge(sessionId);

    // supply push challenge
    final UpdateVerificationSessionRequest updatedRequest = new UpdateVerificationSessionRequest(
        "test", UpdateVerificationSessionRequest.PushTokenType.FCM, pushChallenge, null, null, null);
    final VerificationSessionResponse pushChallengeSupplied = Operations
        .apiPatch("/v1/verification/session/%s".formatted(sessionId), updatedRequest)
        .executeExpectSuccess(VerificationSessionResponse.class);
    System.out.println("push challenge supplied: " + pushChallengeSupplied);

    Assertions.assertTrue(pushChallengeSupplied.allowedToRequestCode());

    // request code
    final VerificationCodeRequest verificationCodeRequest = new VerificationCodeRequest(
        VerificationCodeRequest.Transport.SMS, "android-ng");

    final VerificationSessionResponse codeRequested = Operations
        .apiPost("/v1/verification/session/%s/code".formatted(sessionId), verificationCodeRequest)
        .executeExpectSuccess(VerificationSessionResponse.class);
    System.out.println("code requested: " + codeRequested);

    // verify code
    final SubmitVerificationCodeRequest submitVerificationCodeRequest = new SubmitVerificationCodeRequest("265402");
    final VerificationSessionResponse codeVerified = Operations
        .apiPut("/v1/verification/session/%s/code".formatted(sessionId), submitVerificationCodeRequest)
        .executeExpectSuccess(VerificationSessionResponse.class);
    System.out.println("sms code supplied: " + codeVerified);
  }

  @Test
  public void testSendMessageUnsealed() throws Exception {
    final TestUser userA = Operations.newRegisteredUser("+19995550102");
    final TestUser userB = Operations.newRegisteredUser("+19995550103");

    try {
      final byte[] expectedContent = "Hello, World!".getBytes(StandardCharsets.UTF_8);
      final String contentBase64 = Base64.getEncoder().encodeToString(expectedContent);
      final IncomingMessage message = new IncomingMessage(1, Device.MASTER_ID, userB.registrationId(), contentBase64);
      final IncomingMessageList messages = new IncomingMessageList(List.of(message), false, true, System.currentTimeMillis());

      System.out.println("Sending message");
      final Pair<Integer, SendMessageResponse> sendMessage = Operations
          .apiPut("/v1/messages/%s".formatted(userB.aciUuid().toString()), messages)
          .authorized(userA)
          .execute(SendMessageResponse.class);
      System.out.println("Message sent: " + sendMessage);

      System.out.println("Receive message");
      final Pair<Integer, OutgoingMessageEntityList> receiveMessages = Operations.apiGet("/v1/messages")
          .authorized(userB)
          .execute(OutgoingMessageEntityList.class);
      System.out.println("Message received: " + receiveMessages);

      final byte[] actualContent = receiveMessages.getRight().messages().get(0).content();
      assertArrayEquals(expectedContent, actualContent);
    } finally {
      Operations.deleteUser(userA);
      Operations.deleteUser(userB);
    }
  }
}
