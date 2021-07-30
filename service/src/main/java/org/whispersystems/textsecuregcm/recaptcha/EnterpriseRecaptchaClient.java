/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.recaptcha;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.recaptchaenterprise.v1.RecaptchaEnterpriseServiceClient;
import com.google.cloud.recaptchaenterprise.v1.RecaptchaEnterpriseServiceSettings;
import com.google.recaptchaenterprise.v1.Assessment;
import com.google.recaptchaenterprise.v1.Event;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnterpriseRecaptchaClient implements RecaptchaClient {
  private static final Logger logger = LoggerFactory.getLogger(EnterpriseRecaptchaClient.class);

  private final double scoreFloor;
  private final String siteKey;
  private final String projectPath;
  private final RecaptchaEnterpriseServiceClient client;

  public EnterpriseRecaptchaClient(
      final double scoreFloor,
      @Nonnull final String siteKey,
      @Nonnull final String projectPath,
      @Nonnull final String recaptchaCredentialConfigurationJson) {
    try {
      this.scoreFloor = scoreFloor;
      this.siteKey = Objects.requireNonNull(siteKey);
      this.projectPath = Objects.requireNonNull(projectPath);
      this.client = RecaptchaEnterpriseServiceClient.create(RecaptchaEnterpriseServiceSettings.newBuilder()
          .setCredentialsProvider(FixedCredentialsProvider.create(GoogleCredentials.fromStream(
              new ByteArrayInputStream(recaptchaCredentialConfigurationJson.getBytes(StandardCharsets.UTF_8)))))
          .build());
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  @Override
  public boolean verify(final String token, final String ip) {
    Event event = Event.newBuilder()
        .setExpectedAction("challenge")
        .setSiteKey(siteKey)
        .setToken(token)
        .setUserIpAddress(ip)
        .build();
    final Assessment assessment = client.createAssessment(projectPath, Assessment.newBuilder().setEvent(event).build());

    return assessment.getTokenProperties().getValid() && assessment.getRiskAnalysis().getScore() >= scoreFloor;
  }
}
