/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.exc.InvalidTypeIdException;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.util.SystemMapper;

import static org.junit.jupiter.api.Assertions.*;

class AnswerChallengeRequestTest {

  @Test
  void parse() throws JsonProcessingException {
    {
      final String pushChallengeJson = "{\n"
          + "  \"type\": \"rateLimitPushChallenge\",\n"
          + "  \"challenge\": \"Hello I am a push challenge token\"\n"
          + "}";

      final AnswerChallengeRequest answerChallengeRequest =
          SystemMapper.getMapper().readValue(pushChallengeJson, AnswerChallengeRequest.class);

      assertTrue(answerChallengeRequest instanceof AnswerPushChallengeRequest);
      assertEquals("Hello I am a push challenge token",
          ((AnswerPushChallengeRequest) answerChallengeRequest).getChallenge());
    }

    {
      final String recaptchaChallengeJson = "{\n"
          + "  \"type\": \"recaptcha\",\n"
          + "  \"token\": \"A server-generated token\",\n"
          + "  \"captcha\": \"The value of the solved captcha token\"\n"
          + "}";

      final AnswerChallengeRequest answerChallengeRequest =
          SystemMapper.getMapper().readValue(recaptchaChallengeJson, AnswerChallengeRequest.class);

      assertTrue(answerChallengeRequest instanceof AnswerRecaptchaChallengeRequest);

      assertEquals("A server-generated token",
          ((AnswerRecaptchaChallengeRequest) answerChallengeRequest).getToken());

      assertEquals("The value of the solved captcha token",
          ((AnswerRecaptchaChallengeRequest) answerChallengeRequest).getCaptcha());
    }

    {
      final String unrecognizedTypeJson = "{\n"
          + "  \"type\": \"unrecognized\",\n"
          + "  \"token\": \"A server-generated token\",\n"
          + "  \"captcha\": \"The value of the solved captcha token\"\n"
          + "}";

      assertThrows(InvalidTypeIdException.class,
          () -> SystemMapper.getMapper().readValue(unrecognizedTypeJson, AnswerChallengeRequest.class));
    }
  }
}
