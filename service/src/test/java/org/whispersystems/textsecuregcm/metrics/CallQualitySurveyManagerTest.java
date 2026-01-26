/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.PublisherInterface;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.pubsub.v1.PubsubMessage;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.platform.commons.util.StringUtils;
import org.mockito.ArgumentCaptor;
import org.signal.calling.survey.CallQualitySurveyResponsePubSubMessage;
import org.signal.chat.calling.quality.SubmitCallQualitySurveyRequest;
import org.whispersystems.textsecuregcm.asn.AsnInfo;
import org.whispersystems.textsecuregcm.asn.AsnInfoProvider;
import org.whispersystems.textsecuregcm.util.TestClock;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

class CallQualitySurveyManagerTest {

  private AsnInfoProvider asnInfoProvider;
  private PublisherInterface pubsubPublisher;

  private CallQualitySurveyManager callQualitySurveyManager;

  private static final TestClock CLOCK = TestClock.pinned(Instant.now());

  private static final String USER_AGENT = "Signal-iOS/7.78.0.1041 iOS/18.3.2 libsignal/0.80.3";
  private static final String REMOTE_ADDRESS = "127.0.0.1";

  @BeforeEach
  void setUp() {
    asnInfoProvider = mock(AsnInfoProvider.class);
    pubsubPublisher = mock(PublisherInterface.class);

    callQualitySurveyManager = new CallQualitySurveyManager(() -> asnInfoProvider, pubsubPublisher, CLOCK, Runnable::run);
  }

  @Test
  void submitCallQualitySurvey() throws InvalidProtocolBufferException {
    final long asn = 1234;
    final String asnRegion = "US";

    final byte[] telemetryBytes = TestRandomUtil.nextBytes(32);

    final float connectionRttMedian = ThreadLocalRandom.current().nextFloat();
    final float audioRttMedian = ThreadLocalRandom.current().nextFloat();
    final float videoRttMedian = ThreadLocalRandom.current().nextFloat();
    final float audioRecvJitterMedian = ThreadLocalRandom.current().nextFloat();
    final float videoRecvJitterMedian = ThreadLocalRandom.current().nextFloat();
    final float audioSendJitterMedian = ThreadLocalRandom.current().nextFloat();
    final float videoSendJitterMedian = ThreadLocalRandom.current().nextFloat();
    final float audioRecvPacketLossFraction = ThreadLocalRandom.current().nextFloat();
    final float videoRecvPacketLossFraction = ThreadLocalRandom.current().nextFloat();
    final float audioSendPacketLossFraction = ThreadLocalRandom.current().nextFloat();
    final float videoSendPacketLossFraction = ThreadLocalRandom.current().nextFloat();

    when(asnInfoProvider.lookup(REMOTE_ADDRESS)).thenReturn(Optional.of(new AsnInfo(asn, asnRegion)));

    final SubmitCallQualitySurveyRequest request = SubmitCallQualitySurveyRequest.newBuilder()
        .setUserSatisfied(false)
        .addCallQualityIssues("too_hot")
        .addCallQualityIssues("too_cold")
        .setAdditionalIssuesDescription("But this one is just right")
        .setDebugLogUrl("https://example.com/")
        .setStartTimestamp(123456789)
        .setEndTimestamp(987654321)
        .setCallType("direct_video")
        .setSuccess(true)
        .setCallEndReason("caller_hang_up")
        .setConnectionRttMedian(connectionRttMedian)
        .setAudioRttMedian(audioRttMedian)
        .setVideoRttMedian(videoRttMedian)
        .setAudioRecvJitterMedian(audioRecvJitterMedian)
        .setVideoRecvJitterMedian(videoRecvJitterMedian)
        .setAudioSendJitterMedian(audioSendJitterMedian)
        .setVideoSendJitterMedian(videoSendJitterMedian)
        .setAudioRecvPacketLossFraction(audioRecvPacketLossFraction)
        .setVideoRecvPacketLossFraction(videoRecvPacketLossFraction)
        .setAudioSendPacketLossFraction(audioSendPacketLossFraction)
        .setVideoSendPacketLossFraction(videoSendPacketLossFraction)
        .setCallTelemetry(ByteString.copyFrom(telemetryBytes))
        .build();

    //noinspection unchecked
    when(pubsubPublisher.publish(any())).thenReturn(mock(ApiFuture.class));

    assertDoesNotThrow(() -> callQualitySurveyManager.submitCallQualitySurvey(request, REMOTE_ADDRESS, USER_AGENT));

    final ArgumentCaptor<PubsubMessage> pubsubMessageCaptor = ArgumentCaptor.forClass(PubsubMessage.class);

    verify(pubsubPublisher).publish(pubsubMessageCaptor.capture());

    final CallQualitySurveyResponsePubSubMessage callQualitySurveyResponsePubSubMessage =
        CallQualitySurveyResponsePubSubMessage.parseFrom(pubsubMessageCaptor.getValue().getData());

    assertEquals(4, UUID.fromString(callQualitySurveyResponsePubSubMessage.getResponseId()).version());
    assertEquals("ios", callQualitySurveyResponsePubSubMessage.getClientPlatform());
    assertEquals("7.78.0.1041", callQualitySurveyResponsePubSubMessage.getClientVersion());
    assertEquals("iOS/18.3.2 libsignal/0.80.3", callQualitySurveyResponsePubSubMessage.getClientUaAdditionalSpecifiers());
    assertEquals(asnRegion, callQualitySurveyResponsePubSubMessage.getAsnRegion());
    assertFalse(callQualitySurveyResponsePubSubMessage.getUserSatisfied());
    assertEquals(List.of("too_hot", "too_cold"), callQualitySurveyResponsePubSubMessage.getCallQualityIssuesList());
    assertEquals("But this one is just right", callQualitySurveyResponsePubSubMessage.getAdditionalIssuesDescription());
    assertEquals("https://example.com/", callQualitySurveyResponsePubSubMessage.getDebugLogUrl());
    assertEquals(123456789L * 1_000, callQualitySurveyResponsePubSubMessage.getStartTimestamp());
    assertEquals(987654321L * 1_000, callQualitySurveyResponsePubSubMessage.getEndTimestamp());
    assertEquals("direct_video", callQualitySurveyResponsePubSubMessage.getCallType());
    assertTrue(callQualitySurveyResponsePubSubMessage.getSuccess());
    assertEquals("caller_hang_up", callQualitySurveyResponsePubSubMessage.getCallEndReason());
    assertEquals(connectionRttMedian, callQualitySurveyResponsePubSubMessage.getConnectionRttMedian());
    assertEquals(audioRttMedian, callQualitySurveyResponsePubSubMessage.getAudioRttMedian());
    assertEquals(videoRttMedian, callQualitySurveyResponsePubSubMessage.getVideoRttMedian());
    assertEquals(audioRecvJitterMedian, callQualitySurveyResponsePubSubMessage.getAudioRecvJitterMedian());
    assertEquals(videoRecvJitterMedian, callQualitySurveyResponsePubSubMessage.getVideoRecvJitterMedian());
    assertEquals(audioSendJitterMedian, callQualitySurveyResponsePubSubMessage.getAudioSendJitterMedian());
    assertEquals(videoSendJitterMedian, callQualitySurveyResponsePubSubMessage.getVideoSendJitterMedian());
    assertEquals(audioRecvPacketLossFraction, callQualitySurveyResponsePubSubMessage.getAudioRecvPacketLossFraction());
    assertEquals(videoRecvPacketLossFraction, callQualitySurveyResponsePubSubMessage.getVideoRecvPacketLossFraction());
    assertEquals(audioSendPacketLossFraction, callQualitySurveyResponsePubSubMessage.getAudioSendPacketLossFraction());
    assertEquals(videoSendPacketLossFraction, callQualitySurveyResponsePubSubMessage.getVideoSendPacketLossFraction());
    assertArrayEquals(telemetryBytes, callQualitySurveyResponsePubSubMessage.getCallTelemetry().toByteArray());
  }

  @ParameterizedTest
  @MethodSource
  void validateRequest(final SubmitCallQualitySurveyRequest request, final boolean expectValid) {
    final Executable validateRequest = () -> CallQualitySurveyManager.validateRequest(request);

    if (expectValid) {
      assertDoesNotThrow(validateRequest);
    } else {
      final CallQualityInvalidArgumentsException invalidArgumentsException =
          assertThrows(CallQualityInvalidArgumentsException.class, validateRequest);

      assertTrue(StringUtils.isNotBlank(invalidArgumentsException.getMessage()));
    }
  }

  private static List<Arguments> validateRequest() {
    final SubmitCallQualitySurveyRequest validRequest = SubmitCallQualitySurveyRequest.newBuilder()
        .setStartTimestamp(Instant.now().toEpochMilli())
        .setEndTimestamp(Instant.now().plusSeconds(60).toEpochMilli())
        .setCallType("test")
        .setCallEndReason("test")
        .build();

    return List.of(
        Arguments.argumentSet("Valid survey response", validRequest, true),
        Arguments.argumentSet("No start timestamp", validRequest.toBuilder().clearStartTimestamp().build(), false),
        Arguments.argumentSet("No end timestamp", validRequest.toBuilder().clearEndTimestamp().build(), false),
        Arguments.argumentSet("No call type", validRequest.toBuilder().clearCallType().build(), false),
        Arguments.argumentSet("No call end reason", validRequest.toBuilder().clearCallEndReason().build(), false)
    );
  }
}
