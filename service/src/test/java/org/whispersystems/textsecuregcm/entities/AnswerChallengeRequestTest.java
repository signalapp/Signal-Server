/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.exc.InvalidTypeIdException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.util.SystemMapper;

class AnswerChallengeRequestTest {

  @ParameterizedTest
  @ValueSource(strings = {"captcha"})
  void parse(final String type) throws JsonProcessingException {
    {
      final String pushChallengeJson = """
          {
            "type": "rateLimitPushChallenge",
            "challenge": "Hello I am a push challenge token"
          }
          """;

      final AnswerChallengeRequest answerChallengeRequest =
          SystemMapper.jsonMapper().readValue(pushChallengeJson, AnswerChallengeRequest.class);

      assertTrue(answerChallengeRequest instanceof AnswerPushChallengeRequest);
      assertEquals("Hello I am a push challenge token",
          ((AnswerPushChallengeRequest) answerChallengeRequest).getChallenge());
    }

    {
      final String captchaChallengeJson = """
          {
            "type": "%s",
            "token": "A server-generated token",
            "captcha": "The value of the solved captcha token"
          }
          """.formatted(type);

      final AnswerChallengeRequest answerChallengeRequest =
          SystemMapper.jsonMapper().readValue(captchaChallengeJson, AnswerChallengeRequest.class);

      assertTrue(answerChallengeRequest instanceof AnswerCaptchaChallengeRequest);

      assertEquals("A server-generated token",
          ((AnswerCaptchaChallengeRequest) answerChallengeRequest).getToken());

      assertEquals("The value of the solved captcha token",
          ((AnswerCaptchaChallengeRequest) answerChallengeRequest).getCaptcha());
    }

    {
      final String unrecognizedTypeJson = """
          {
            "type": "unrecognized",
            "token": "A server-generated token",
            "captcha": "The value of the solved captcha token"
          }
          """;

      assertThrows(InvalidTypeIdException.class,
          () -> SystemMapper.jsonMapper().readValue(unrecognizedTypeJson, AnswerChallengeRequest.class));
    }
  }
}
