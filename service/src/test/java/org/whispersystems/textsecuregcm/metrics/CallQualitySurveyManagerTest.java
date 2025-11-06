/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

    final float rttMedianConnection = ThreadLocalRandom.current().nextFloat();
    final float rttMedianMedia = ThreadLocalRandom.current().nextFloat();
    final float jitterMedianRecv = ThreadLocalRandom.current().nextFloat();
    final float jitterMedianSend = ThreadLocalRandom.current().nextFloat();
    final float packetLossFractionRecv = ThreadLocalRandom.current().nextFloat();
    final float packetLossFractionSend = ThreadLocalRandom.current().nextFloat();

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
        .setRttMedianConnection(rttMedianConnection)
        .setRttMedianMedia(rttMedianMedia)
        .setJitterMedianRecv(jitterMedianRecv)
        .setJitterMedianSend(jitterMedianSend)
        .setPacketLossFractionRecv(packetLossFractionRecv)
        .setPacketLossFractionSend(packetLossFractionSend)
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
    assertEquals(123456789, callQualitySurveyResponsePubSubMessage.getStartTimestamp());
    assertEquals(987654321, callQualitySurveyResponsePubSubMessage.getEndTimestamp());
    assertEquals("direct_video", callQualitySurveyResponsePubSubMessage.getCallType());
    assertTrue(callQualitySurveyResponsePubSubMessage.getSuccess());
    assertEquals("caller_hang_up", callQualitySurveyResponsePubSubMessage.getCallEndReason());
    assertEquals(rttMedianConnection, callQualitySurveyResponsePubSubMessage.getRttMedianConnection());
    assertEquals(rttMedianMedia, callQualitySurveyResponsePubSubMessage.getRttMedianMedia());
    assertEquals(jitterMedianRecv, callQualitySurveyResponsePubSubMessage.getJitterMedianRecv());
    assertEquals(jitterMedianSend, callQualitySurveyResponsePubSubMessage.getJitterMedianSend());
    assertEquals(packetLossFractionRecv, callQualitySurveyResponsePubSubMessage.getPacketLossFractionRecv());
    assertEquals(packetLossFractionSend, callQualitySurveyResponsePubSubMessage.getPacketLossFractionSend());
    assertArrayEquals(telemetryBytes, callQualitySurveyResponsePubSubMessage.getCallTelemetry().toByteArray());
  }
}
