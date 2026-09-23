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

import java.net.http.HttpHeaders;
import java.net.http.HttpResponse;
import java.security.GeneralSecurityException;
import java.util.Base64;
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
import org.whispersystems.textsecuregcm.configuration.WebPushConfiguration;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretBytes;
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

  private FaultTolerantHttpClient httpClient;
  private FaultTolerantRedisClusterClient redisClient;
  private WebPushConfiguration config;
  private WebPushSender webPushSender;

  private static final String VAPID_PRIVATE_KEY = "MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQg8dRIiQwMkW/hdtdytU4NJmQWDjUTOv3ZV/nDQVGHGy2hRANCAATpV1b2ETFP0CCL6woRsdG0SnilqV7NMoiisocq5P/xl0GO8T97N3w7a/b4oWNkWKvxJ1hN5Q75tZauC0sXHOS5";

  @BeforeEach
  void setUp() throws Exception {
    httpClient = mock(FaultTolerantHttpClient.class);
    redisClient = mock(FaultTolerantRedisClusterClient.class);
    config = new WebPushConfiguration(new SecretBytes(Base64.getDecoder().decode(VAPID_PRIVATE_KEY)), "mailto:test@example.tld");
    webPushSender = new WebPushSender(
      redisClient,
      config.vapidStaticKeyPair(),
      config.vapidSub(),
      httpClient
    );
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
    final PushNotification pushNotification = new PushNotification(new PushToken.WEBPUSH(webPushSub, true), PushNotification.NotificationType.NOTIFICATION, null, null, null, true, null);

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
  void testSendMessageInactive() throws JsonProcessingException {
    final WebPushSubscription webPushSub = SystemMapper.jsonMapper().readValue("""
        {
          "endpoint": "https://domain.tld/random1",
          "auth": "BTBZMqHH6r4Tts7J_aSIgg",
          "publicKey": "BCVxsr7N_eNgVRqvHtD0zTZsEc6-VV-JvLexhqUzORcxaOzi6-AYWXvTBHm4bjyPjs7Vd8pZGH6SRpkNtoIAiw4"
        }
      """, WebPushSubscription.class);
    final PushNotification pushNotification = new PushNotification(new PushToken.WEBPUSH(webPushSub, false), PushNotification.NotificationType.NOTIFICATION, null, null, null, true, null);

    final SendPushNotificationResult result = webPushSender.sendNotification(pushNotification).join();

    verifyNoInteractions(httpClient);
    assertFalse(result.accepted());
    assertFalse(result.errorCode().isEmpty());
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
    final PushNotification pushNotification = new PushNotification(new PushToken.WEBPUSH(webPushSub, true), PushNotification.NotificationType.NOTIFICATION, null, null, null, true, null);

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
    final PushNotification pushNotification = new PushNotification(new PushToken.WEBPUSH(webPushSub, true), PushNotification.NotificationType.NOTIFICATION, null, null, null, true, null);

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
    final PushNotification pushNotification = new PushNotification(new PushToken.WEBPUSH(webPushSub, true), PushNotification.NotificationType.NOTIFICATION, null, null, null, true, null);

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
    verify(cmd, atLeastOnce()).set(eq("WP_END::https://domain.tld/random1"), eq(WebPushSender.RATE_LIMITED), any());
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
    final PushNotification pushNotification = new PushNotification(new PushToken.WEBPUSH(webPushSub, true), PushNotification.NotificationType.NOTIFICATION, null, null, null, true, null);

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

  @Test
  void testVapidHeader() throws GeneralSecurityException, JsonProcessingException {
    final String vapidHeader = WebPushSender.genAuthorization(config.vapidStaticKeyPair(), "https://domain.tld", "mailto:mail@example.localhost", 1000000);
    // Replace URL-safe Base64 encoded signature, as it changes everytime. That's enough to test the header is in the good format
    final String toCompare = vapidHeader.replaceAll("\\.[A-Za-z0-9-_]+,", ".AAAABBBBCCCCDDDD,");
    assertEquals("vapid t=eyJ0eXAiOiJKV1QiLCJhbGciOiJFUzI1NiJ9.eyJhdWQiOiJodHRwczovL2RvbWFpbi50bGQiLCJzdWIiOiJtYWlsdG86bWFpbEBleGFtcGxlLmxvY2FsaG9zdCIsImV4cCI6MTAwMDkwMH0.AAAABBBBCCCCDDDD,k=BOlXVvYRMU_QIIvrChGx0bRKeKWpXs0yiKKyhyrk__GXQY7xP3s3fDtr9vihY2RYq_EnWE3lDvm1lq4LSxcc5Lk", toCompare);
  }
}
