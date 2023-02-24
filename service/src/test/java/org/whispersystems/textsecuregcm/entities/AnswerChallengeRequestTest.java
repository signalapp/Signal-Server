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
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.util.SystemMapper;

class AnswerChallengeRequestTest {

  @Test
  void parse() throws JsonProcessingException {
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
      final String recaptchaChallengeJson = """
          {
            "type": "recaptcha",
            "token": "A server-generated token",
            "captcha": "The value of the solved captcha token"
          }
          """;

      final AnswerChallengeRequest answerChallengeRequest =
          SystemMapper.jsonMapper().readValue(recaptchaChallengeJson, AnswerChallengeRequest.class);

      assertTrue(answerChallengeRequest instanceof AnswerRecaptchaChallengeRequest);

      assertEquals("A server-generated token",
          ((AnswerRecaptchaChallengeRequest) answerChallengeRequest).getToken());

      assertEquals("The value of the solved captcha token",
          ((AnswerRecaptchaChallengeRequest) answerChallengeRequest).getCaptcha());
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
