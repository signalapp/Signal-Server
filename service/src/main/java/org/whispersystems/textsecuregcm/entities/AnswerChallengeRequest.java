/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = AnswerPushChallengeRequest.class, name = "rateLimitPushChallenge"),
    @JsonSubTypes.Type(value = AnswerCaptchaChallengeRequest.class, name = "captcha")
})
public abstract class AnswerChallengeRequest {
}
