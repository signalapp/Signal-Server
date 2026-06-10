/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.http.HttpResponse;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.http.FaultTolerantHttpClient;
import org.whispersystems.textsecuregcm.push.PushNotification.PushToken;
import org.whispersystems.textsecuregcm.tests.util.SynchronousExecutorService;
import org.whispersystems.textsecuregcm.util.SystemMapper;

import com.fasterxml.jackson.core.JsonProcessingException;

class WebPushSenderTest {

  private ExecutorService executorService;
  private FaultTolerantHttpClient httpClient;
  private WebPushSender webPushSender;

  @BeforeEach
  void setUp() throws IOException {
    executorService = new SynchronousExecutorService();
    httpClient = mock(FaultTolerantHttpClient.class);
    webPushSender = new WebPushSender(executorService, httpClient);
  }

  @AfterEach
  void tearDown() throws InterruptedException {
    executorService.shutdown();

    //noinspection ResultOfMethodCallIgnored
    executorService.awaitTermination(1, TimeUnit.SECONDS);
  }

  @Test
  void testSendMessage() throws JsonProcessingException {
    final WebPushSubscription webPushSub = SystemMapper.jsonMapper().readValue("""
        {
          "endpoint": "https://domain.tld/random1",
          "auth": "BTBZMqHH6r4Tts7J_aSIgg",
          "publicKey": "BCVxsr7N_eNgVRqvHtD0zTZsEc6-VV-JvLexhqUzORcxaOzi6-AYWXvTBHm4bjyPjs7Vd8pZGH6SRpkNtoIAiw4"
        }
      """, WebPushSubscription.class);
    final PushNotification pushNotification = new PushNotification(new PushToken.WEBPUSH(webPushSub), PushNotification.NotificationType.NOTIFICATION, null, null, null, true, null);

    final HttpResponse<byte[]> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(201);

    final CompletableFuture<HttpResponse<byte[]>> sendFuture = CompletableFuture.completedFuture(response);
    when(httpClient.sendAsync(any(), eq(HttpResponse.BodyHandlers.ofByteArray()))).thenReturn(sendFuture);

    final SendPushNotificationResult result = webPushSender.sendNotification(pushNotification).join();

    verify(httpClient).sendAsync(any(), eq(HttpResponse.BodyHandlers.ofByteArray()));
    assertTrue(result.accepted());
    assertTrue(result.errorCode().isEmpty());
    assertFalse(result.unregistered());
  }

  @Test
  void testSendMessageRejected() throws JsonProcessingException {
    final WebPushSubscription webPushSub = SystemMapper.jsonMapper().readValue("""
        {
          "endpoint": "https://domain.tld/random1",
          "auth": "BTBZMqHH6r4Tts7J_aSIgg",
          "publicKey": "BCVxsr7N_eNgVRqvHtD0zTZsEc6-VV-JvLexhqUzORcxaOzi6-AYWXvTBHm4bjyPjs7Vd8pZGH6SRpkNtoIAiw4"
        }
      """, WebPushSubscription.class);
    final PushNotification pushNotification = new PushNotification(new PushToken.WEBPUSH(webPushSub), PushNotification.NotificationType.NOTIFICATION, null, null, null, true, null);

    final HttpResponse<byte[]> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(500);

    final CompletableFuture<HttpResponse<byte[]>> sendFuture = CompletableFuture.completedFuture(response);
    when(httpClient.sendAsync(any(), eq(HttpResponse.BodyHandlers.ofByteArray()))).thenReturn(sendFuture);

    final SendPushNotificationResult result = webPushSender.sendNotification(pushNotification).join();

    verify(httpClient).sendAsync(any(), eq(HttpResponse.BodyHandlers.ofByteArray()));
    assertFalse(result.accepted());
    assertEquals(Optional.of("500"), result.errorCode());
    assertFalse(result.unregistered());
  }

  @ParameterizedTest
  @ValueSource(ints = {404, 403, 401, 410})
  void testSendMessageUnregistered(final int statusCode) throws JsonProcessingException {
    final WebPushSubscription webPushSub = SystemMapper.jsonMapper().readValue("""
        {
          "endpoint": "https://domain.tld/random1",
          "auth": "BTBZMqHH6r4Tts7J_aSIgg",
          "publicKey": "BCVxsr7N_eNgVRqvHtD0zTZsEc6-VV-JvLexhqUzORcxaOzi6-AYWXvTBHm4bjyPjs7Vd8pZGH6SRpkNtoIAiw4"
        }
      """, WebPushSubscription.class);
    final PushNotification pushNotification = new PushNotification(new PushToken.WEBPUSH(webPushSub), PushNotification.NotificationType.NOTIFICATION, null, null, null, true, null);

    final HttpResponse<byte[]> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(statusCode);

    final CompletableFuture<HttpResponse<byte[]>> sendFuture = CompletableFuture.completedFuture(response);
    when(httpClient.sendAsync(any(), eq(HttpResponse.BodyHandlers.ofByteArray()))).thenReturn(sendFuture);


    final SendPushNotificationResult result = webPushSender.sendNotification(pushNotification).join();

    verify(httpClient).sendAsync(any(), eq(HttpResponse.BodyHandlers.ofByteArray()));
    assertFalse(result.accepted());
    assertEquals(Optional.of(String.valueOf(statusCode)), result.errorCode());
    assertTrue(result.unregistered());
  }

  // TODO test 429
}
