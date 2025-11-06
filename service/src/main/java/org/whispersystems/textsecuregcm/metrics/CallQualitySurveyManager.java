/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import com.google.cloud.pubsub.v1.PublisherInterface;
import com.google.pubsub.v1.PubsubMessage;
import io.micrometer.core.instrument.Metrics;
import java.time.Clock;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.signal.calling.survey.CallQualitySurveyResponsePubSubMessage;
import org.signal.chat.calling.quality.SubmitCallQualitySurveyRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.asn.AsnInfoProvider;
import org.whispersystems.textsecuregcm.util.GoogleApiUtil;
import org.whispersystems.textsecuregcm.util.ua.UnrecognizedUserAgentException;
import org.whispersystems.textsecuregcm.util.ua.UserAgent;
import org.whispersystems.textsecuregcm.util.ua.UserAgentUtil;

public class CallQualitySurveyManager {

  private final Supplier<AsnInfoProvider> asnInfoProviderSupplier;
  private final PublisherInterface pubSubPublisher;
  private final Clock clock;
  private final Executor pubSubCallbackExecutor;

  private final String PUB_SUB_MESSAGE_COUNTER_NAME = MetricsUtil.name(CallQualitySurveyManager.class, "pubSubMessage");

  private static final Logger logger = LoggerFactory.getLogger(CallQualitySurveyManager.class);

  public CallQualitySurveyManager(final Supplier<AsnInfoProvider> asnInfoProviderSupplier,
      final PublisherInterface pubSubPublisher,
      final Clock clock,
      final Executor pubSubCallbackExecutor) {

    this.asnInfoProviderSupplier = asnInfoProviderSupplier;
    this.pubSubPublisher = pubSubPublisher;
    this.clock = clock;
    this.pubSubCallbackExecutor = pubSubCallbackExecutor;
  }

  public void submitCallQualitySurvey(final SubmitCallQualitySurveyRequest submitCallQualitySurveyRequest,
      final String remoteAddress,
      final String userAgentString) {

    final CallQualitySurveyResponsePubSubMessage.Builder pubSubMessageBuilder =
        CallQualitySurveyResponsePubSubMessage.newBuilder()
            .setResponseId(UUID.randomUUID().toString())
            .setSubmissionTimestamp(clock.millis() * 1000)
            .setUserSatisfied(submitCallQualitySurveyRequest.getUserSatisfied())
            .setStartTimestamp(submitCallQualitySurveyRequest.getStartTimestamp())
            .setEndTimestamp(submitCallQualitySurveyRequest.getEndTimestamp())
            .setCallType(submitCallQualitySurveyRequest.getCallType())
            .setSuccess(submitCallQualitySurveyRequest.getSuccess())
            .setCallEndReason(submitCallQualitySurveyRequest.getCallEndReason());

    try {
      final UserAgent userAgent = UserAgentUtil.parseUserAgentString(userAgentString);

      pubSubMessageBuilder.setClientPlatform(userAgent.platform().name().toLowerCase(Locale.ROOT));
      pubSubMessageBuilder.setClientVersion(userAgent.version().toString());

      if (StringUtils.isNotBlank(userAgent.additionalSpecifiers())) {
        pubSubMessageBuilder.setClientUaAdditionalSpecifiers(userAgent.additionalSpecifiers());
      }
    } catch (final UnrecognizedUserAgentException _) {
    }

    asnInfoProviderSupplier.get().lookup(remoteAddress)
        .ifPresent(asnInfo -> pubSubMessageBuilder.setAsnRegion(asnInfo.regionCode()));

    pubSubMessageBuilder.addAllCallQualityIssues(submitCallQualitySurveyRequest.getCallQualityIssuesList());

    if (submitCallQualitySurveyRequest.hasAdditionalIssuesDescription()) {
      pubSubMessageBuilder.setAdditionalIssuesDescription(submitCallQualitySurveyRequest.getAdditionalIssuesDescription());
    }

    if (submitCallQualitySurveyRequest.hasDebugLogUrl()) {
      pubSubMessageBuilder.setDebugLogUrl(submitCallQualitySurveyRequest.getDebugLogUrl());
    }

    if (submitCallQualitySurveyRequest.hasRttMedianConnection()) {
      pubSubMessageBuilder.setRttMedianConnection(submitCallQualitySurveyRequest.getRttMedianConnection());
    }

    if (submitCallQualitySurveyRequest.hasRttMedianMedia()) {
      pubSubMessageBuilder.setRttMedianMedia(submitCallQualitySurveyRequest.getRttMedianMedia());
    }

    if (submitCallQualitySurveyRequest.hasJitterMedianRecv()) {
      pubSubMessageBuilder.setJitterMedianRecv(submitCallQualitySurveyRequest.getJitterMedianRecv());
    }

    if (submitCallQualitySurveyRequest.hasJitterMedianSend()) {
      pubSubMessageBuilder.setJitterMedianSend(submitCallQualitySurveyRequest.getJitterMedianSend());
    }

    if (submitCallQualitySurveyRequest.hasPacketLossFractionRecv()) {
      pubSubMessageBuilder.setPacketLossFractionRecv(submitCallQualitySurveyRequest.getPacketLossFractionRecv());
    }

    if (submitCallQualitySurveyRequest.hasPacketLossFractionSend()) {
      pubSubMessageBuilder.setPacketLossFractionSend(submitCallQualitySurveyRequest.getPacketLossFractionSend());
    }

    if (submitCallQualitySurveyRequest.hasCallTelemetry()) {
      pubSubMessageBuilder.setCallTelemetry(submitCallQualitySurveyRequest.getCallTelemetry());
    }

    GoogleApiUtil.toCompletableFuture(pubSubPublisher.publish(PubsubMessage.newBuilder()
            .setData(pubSubMessageBuilder.build().toByteString())
            .build()), pubSubCallbackExecutor)
        .whenComplete((_, throwable) -> {
          if (throwable != null) {
            logger.warn("Failed to publish call quality survey pub/sub message", throwable);
          }

          Metrics.counter(PUB_SUB_MESSAGE_COUNTER_NAME, "success", String.valueOf(throwable == null))
              .increment();
        });
  }
}
