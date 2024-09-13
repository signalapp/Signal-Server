/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.captcha;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.util.SystemMapper;

class HCaptchaResponseTest {

  @Test
  void testParse() throws Exception {

    final Instant challengeTs = Instant.parse("2024-09-13T21:36:15Z");

    final HCaptchaResponse response =
        SystemMapper.jsonMapper().readValue("""
            {
              "success": "true",
              "challenge_ts": "2024-09-13T21:36:15.000000Z",
              "hostname": "example.com",
              "error-codes": ["one", "two"],
              "score": 0.5,
              "score_reason": ["three", "four"]
            }
            """, HCaptchaResponse.class);

    assertEquals(challengeTs, response.challengeTs);
    assertEquals(List.of("one", "two"), response.errorCodes);
    assertEquals(List.of("three", "four"), response.scoreReasons);
  }

}
