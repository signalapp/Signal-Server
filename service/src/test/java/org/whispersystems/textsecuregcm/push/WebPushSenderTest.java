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
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.http.HttpHeaders;
import java.net.http.HttpResponse;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.http.FaultTolerantHttpClient;
import org.whispersystems.textsecuregcm.push.PushNotification.PushToken;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;
import org.whispersystems.textsecuregcm.tests.util.SynchronousExecutorService;
import org.whispersystems.textsecuregcm.util.SystemMapper;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.lettuce.core.api.sync.RedisStringCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;

class WebPushSenderTest {

  private ExecutorService executorService;
  private FaultTolerantHttpClient httpClient;
  private FaultTolerantRedisClusterClient redisClient;
  private WebPushSender webPushSender;

  @BeforeEach
  void setUp() throws IOException {
    executorService = new SynchronousExecutorService();
    httpClient = mock(FaultTolerantHttpClient.class);
    redisClient = mock(FaultTolerantRedisClusterClient.class);
    webPushSender = new WebPushSender(executorService, redisClient, httpClient);
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

    // Redis must be mocked before sendNotification
    final RedisStringCommands<String, String> cmd = mockRedis(redisClient);
    final HttpResponse<byte[]> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(201);

    final CompletableFuture<HttpResponse<byte[]>> sendFuture = CompletableFuture.completedFuture(response);
    when(httpClient.sendAsync(any(), eq(HttpResponse.BodyHandlers.ofByteArray()))).thenReturn(sendFuture);

    final SendPushNotificationResult result = webPushSender.sendNotification(pushNotification).join();

    verify(httpClient).sendAsync(any(), eq(HttpResponse.BodyHandlers.ofByteArray()));
    verify(cmd, never()).set(any(), eq(WebPushSender.RATE_LIMITED), any());
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

    // Redis must be mocked before sendNotification
    final RedisStringCommands<String, String> cmd = mockRedis(redisClient);
    final HttpResponse<byte[]> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(500);

    final CompletableFuture<HttpResponse<byte[]>> sendFuture = CompletableFuture.completedFuture(response);
    when(httpClient.sendAsync(any(), eq(HttpResponse.BodyHandlers.ofByteArray()))).thenReturn(sendFuture);

    final SendPushNotificationResult result = webPushSender.sendNotification(pushNotification).join();

    verify(httpClient).sendAsync(any(), eq(HttpResponse.BodyHandlers.ofByteArray()));
    verify(cmd, never()).set(any(), eq(WebPushSender.RATE_LIMITED), any());
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

    // Redis must be mocked before sendNotification
    final RedisStringCommands<String, String> cmd = mockRedis(redisClient);
    final HttpResponse<byte[]> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(statusCode);

    final CompletableFuture<HttpResponse<byte[]>> sendFuture = CompletableFuture.completedFuture(response);
    when(httpClient.sendAsync(any(), eq(HttpResponse.BodyHandlers.ofByteArray()))).thenReturn(sendFuture);


    final SendPushNotificationResult result = webPushSender.sendNotification(pushNotification).join();

    verify(httpClient).sendAsync(any(), eq(HttpResponse.BodyHandlers.ofByteArray()));
    verify(cmd, never()).set(any(), eq(WebPushSender.RATE_LIMITED), any());
    assertFalse(result.accepted());
    assertEquals(Optional.of(String.valueOf(statusCode)), result.errorCode());
    assertTrue(result.unregistered());
  }

  @Test
  void testSendMessageRateLimited() throws JsonProcessingException {
    final WebPushSubscription webPushSub = SystemMapper.jsonMapper().readValue("""
        {
          "endpoint": "https://domain.tld/random1",
          "auth": "BTBZMqHH6r4Tts7J_aSIgg",
          "publicKey": "BCVxsr7N_eNgVRqvHtD0zTZsEc6-VV-JvLexhqUzORcxaOzi6-AYWXvTBHm4bjyPjs7Vd8pZGH6SRpkNtoIAiw4"
        }
      """, WebPushSubscription.class);
    final PushNotification pushNotification = new PushNotification(new PushToken.WEBPUSH(webPushSub), PushNotification.NotificationType.NOTIFICATION, null, null, null, true);

    // Redis must be mocked before sendNotification
    final RedisStringCommands<String, String> cmd = mockRedis(redisClient);
    final HttpResponse<byte[]> response = mock(HttpResponse.class);
    final HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.firstValue(any())).thenReturn(Optional.empty());
    when(response.statusCode()).thenReturn(429);
    when(response.headers()).thenReturn(headers);

    final CompletableFuture<HttpResponse<byte[]>> sendFuture = CompletableFuture.completedFuture(response);
    when(httpClient.sendAsync(any(), eq(HttpResponse.BodyHandlers.ofByteArray()))).thenReturn(sendFuture);

    final SendPushNotificationResult result = webPushSender.sendNotification(pushNotification).join();

    verify(httpClient).sendAsync(any(), eq(HttpResponse.BodyHandlers.ofByteArray()));
    verify(cmd, atLeastOnce()).set(any(), eq(WebPushSender.RATE_LIMITED), any());
    assertFalse(result.accepted());
    assertEquals(Optional.of("Rate limited"), result.errorCode());
    assertFalse(result.unregistered());
  }

  @Test
  void testSendMessageAlreadyRateLimited() throws JsonProcessingException {
    final WebPushSubscription webPushSub = SystemMapper.jsonMapper().readValue("""
        {
          "endpoint": "https://domain.tld/random1",
          "auth": "BTBZMqHH6r4Tts7J_aSIgg",
          "publicKey": "BCVxsr7N_eNgVRqvHtD0zTZsEc6-VV-JvLexhqUzORcxaOzi6-AYWXvTBHm4bjyPjs7Vd8pZGH6SRpkNtoIAiw4"
        }
      """, WebPushSubscription.class);
    final PushNotification pushNotification = new PushNotification(new PushToken.WEBPUSH(webPushSub), PushNotification.NotificationType.NOTIFICATION, null, null, null, true);

    final RedisStringCommands<String, String> cmd = mockRedis(redisClient);
    when(cmd.get(any())).thenReturn(WebPushSender.RATE_LIMITED);

    final SendPushNotificationResult result = webPushSender.sendNotification(pushNotification).join();

    verifyNoInteractions(httpClient);
    verify(cmd, never()).set(any(), eq(WebPushSender.RATE_LIMITED), any());
    assertFalse(result.accepted());
    assertEquals(Optional.of("Rate limited"), result.errorCode());
    assertFalse(result.unregistered());
  }

  RedisAdvancedClusterCommands<String, String> mockRedis(FaultTolerantRedisClusterClient redisClient) {
    final RedisAdvancedClusterCommands<String, String> cmd = mock(RedisAdvancedClusterCommands.class);
    final StatefulRedisClusterConnection cluster = mock(StatefulRedisClusterConnection.class);
    when(cluster.sync()).thenReturn(cmd);
    when(redisClient.withCluster(any())).then( inv -> {
      final Function<StatefulRedisClusterConnection<String, String>, String> arg = inv.getArgument(0);
      return arg.apply(cluster);
    });
    return cmd;
  }
}
