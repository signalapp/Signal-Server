/*
 * Copyright 2021-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.recaptcha;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.recaptchaenterprise.v1.RecaptchaEnterpriseServiceClient;
import com.google.cloud.recaptchaenterprise.v1.RecaptchaEnterpriseServiceSettings;
import com.google.common.annotations.VisibleForTesting;
import com.google.recaptchaenterprise.v1.Assessment;
import com.google.recaptchaenterprise.v1.Event;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.ws.rs.BadRequestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnterpriseRecaptchaClient implements RecaptchaClient {

  @VisibleForTesting
  static final String SEPARATOR = ".";
  private static final Logger logger = LoggerFactory.getLogger(EnterpriseRecaptchaClient.class);

  private final double scoreFloor;
  private final String projectPath;
  private final RecaptchaEnterpriseServiceClient client;

  public EnterpriseRecaptchaClient(
      final double scoreFloor,
      @Nonnull final String projectPath,
      @Nonnull final String recaptchaCredentialConfigurationJson) {
    try {
      this.scoreFloor = scoreFloor;
      this.projectPath = Objects.requireNonNull(projectPath);
      this.client = RecaptchaEnterpriseServiceClient.create(RecaptchaEnterpriseServiceSettings.newBuilder()
          .setCredentialsProvider(FixedCredentialsProvider.create(GoogleCredentials.fromStream(
              new ByteArrayInputStream(recaptchaCredentialConfigurationJson.getBytes(StandardCharsets.UTF_8)))))
          .build());
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  /**
   * Parses the token and action (if any) from {@code input}. The expected input format is: {@code [action:]token}.
   * <p>
   * For action to be optional, there is a strong assumption that the token will never contain a {@value  SEPARATOR}.
   * Observation suggests {@code token} is base-64 encoded. In practice, an action should always be present, but we
   * don’t need to be strict.
   */
  static String[] parseInputToken(final String input) {
    String[] keyActionAndToken = input.split("\\" + SEPARATOR, 3);

    if (keyActionAndToken.length == 1) {
      throw new BadRequestException("too few parts");
    }

    if (keyActionAndToken.length == 2) {
      // there was no ":" delimiter; assume we only have a token
      return new String[]{keyActionAndToken[0], null, keyActionAndToken[1]};
    }

    return keyActionAndToken;
  }

  @Override
  public boolean verify(final String input, final String ip) {
    final String[] parts = parseInputToken(input);

    final String sitekey = parts[0];
    final String expectedAction = parts[1];
    final String token = parts[2];

    Event.Builder eventBuilder = Event.newBuilder()
        .setSiteKey(sitekey)
        .setToken(token)
        .setUserIpAddress(ip);

    if (expectedAction != null) {
      eventBuilder.setExpectedAction(expectedAction);
    }

    final Event event = eventBuilder.build();
    final Assessment assessment = client.createAssessment(projectPath, Assessment.newBuilder().setEvent(event).build());

    return assessment.getTokenProperties().getValid() && assessment.getRiskAnalysis().getScore() >= scoreFloor;
  }
}
