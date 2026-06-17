/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.http.FaultTolerantHttpClient;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;
import org.whispersystems.textsecuregcm.util.ResilienceUtil;
import org.whispersystems.textsecuregcm.util.SystemMapper;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HttpHeaders;
import com.google.crypto.tink.apps.webpush.WebPushHybridEncrypt;

import io.lettuce.core.SetArgs;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;

public class WebPushSender implements PushNotificationSender {

  private final FaultTolerantHttpClient httpClient;

  private static final Timer SEND_NOTIFICATION_TIMER = Metrics.timer(name(WebPushSender.class, "sendNotification"));
  private static final String RETRY_NAME = ResilienceUtil.name(WebPushSender.class);
  @VisibleForTesting
  static final String RATE_LIMITED = "rate-limited";
  /** Cache authorization header for 10 min */
  private static final Duration AUTH_CACHE_DURATION = Duration.ofMinutes(10);
  /**
   * If the push server doesn't send a `Retry-After` header with 429,
   * we wait for 5 min by default
   */
  private static final int DEFAULT_RETRY_AFTER = 300;

  private static final Logger logger = LoggerFactory.getLogger(WebPushSender.class);
  private final FaultTolerantRedisClusterClient redisClient;

  private class RateLimitedException extends Exception {}

  // TODO: Add keystore for the VAPID key
  public WebPushSender (ExecutorService executor, FaultTolerantRedisClusterClient redisClient) throws IOException {
    this.httpClient = FaultTolerantHttpClient.newBuilder("webpush", executor)
      .withRedirect(HttpClient.Redirect.NEVER)
      .build();
    this.redisClient = redisClient;
  }

  @VisibleForTesting
  public WebPushSender (ExecutorService executor, FaultTolerantRedisClusterClient redisClient, FaultTolerantHttpClient httpClient) {
    this.httpClient = httpClient;
    this.redisClient = redisClient;
  }

  @Override
  public CompletableFuture<SendPushNotificationResult> sendNotification(PushNotification pushNotification) {
    final WebPushSubscription sub = (WebPushSubscription) pushNotification.pushToken().value();
    final String aud = String.format("https://%s", sub.endpoint().getAuthority());

    String authorization;
    try {
      authorization = getCachedAuthorization(aud);
    } catch (RateLimitedException e) {
      return CompletableFuture.completedFuture(
        new SendPushNotificationResult(false, Optional.of("Rate limited"), false, Optional.empty())
      );
    }

    final Map<String, String> map = new HashMap<String, String>();

    final String key = switch (pushNotification.notificationType()) {
      case NOTIFICATION -> "newMessageAlert";
      case ATTEMPT_LOGIN_NOTIFICATION_HIGH_PRIORITY -> "attemptLoginContext";
      case CHALLENGE -> "challenge";
      case RATE_LIMIT_CHALLENGE -> "rateLimitChallenge";
    };

    map.put(key, pushNotification.data() != null ? pushNotification.data() : "");
    map.put("urgency", pushNotification.urgent() ? "high" : "low");

    final byte[] body;
    try {
      final WebPushHybridEncrypt engine = new WebPushHybridEncrypt.Builder()
        .withAuthSecret(sub.userAuth())
        .withRecipientPublicKey(sub.userPublicKey())
        .build();

      final String clearBody = SystemMapper.jsonMapper().writeValueAsString(map);
      body = engine.encrypt(clearBody.getBytes(), null);
    } catch (Exception e) {
      logger.warn("Error while encrypting web push notification", e);
      return CompletableFuture.completedFuture(new SendPushNotificationResult(false, Optional.of("Error while encrypting notification"), false, Optional.empty()));
    }

    final Timer.Sample sample = Timer.start();

    final HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
      .uri(sub.endpoint())
      .method("POST", HttpRequest.BodyPublishers.ofByteArray(body))
      .header(HttpHeaders.CONTENT_TYPE, "application/octet-stream")
      // We tell the push server to store the push notification at most 7 days, if the user agent
      // doesn't fetch the notification
      .header("TTL", "604800")
      .header(HttpHeaders.CONTENT_ENCODING, "aes128gcm")
      // The urgency is defined by RFC8030: https://www.rfc-editor.org/info/rfc8030/#section-5.3
      .header("Urgency", pushNotification.urgent() ? "high" : "normal");

    // The Topic header is defined by webpush to permit the
    // application server (us) to update a notification, to
    // avoid to store multiple times a same notification if it
    // hasn't been received by the user agent yet.
    //
    // So, if the notification doesn't contain any data, the push server
    // don't need to store all of them => we add a topic.
    //
    // https://www.rfc-editor.org/info/rfc8030/#section-5.4
    if (pushNotification.data() == null) {
      requestBuilder.header("Topic", key);
    }

    return httpClient.sendAsync(requestBuilder.build(), HttpResponse.BodyHandlers.ofByteArray()).whenComplete((ignored, throwable) -> sample.stop(SEND_NOTIFICATION_TIMER))
      .thenApply(response -> {
        int code = response.statusCode();
        return switch (Integer.valueOf(code)) {
           case Integer s when s >= 200 && s < 300 ->
               new SendPushNotificationResult(true, Optional.empty(), false, Optional.empty());
           case 404, 403, 401, 410 ->
               new SendPushNotificationResult(false, Optional.of(String.valueOf(code)), true, Optional.of(Instant.now()));
           case 429 -> {
             int retryAfterS;
             try {
               retryAfterS = response.headers().firstValue("Retry-After").map(v -> Integer.parseInt(v)).orElse(DEFAULT_RETRY_AFTER);
             } catch (NumberFormatException e) {
               retryAfterS = DEFAULT_RETRY_AFTER;
             }
             cacheRateLimit(aud, retryAfterS);
             yield new SendPushNotificationResult(false, Optional.of("Rate limited"), false, Optional.empty());
           }
           default ->
               new SendPushNotificationResult(false, Optional.of(String.valueOf(code)), false, Optional.empty());
        };
       });
  }

  /**
   * @throws RateLimitedException if the aud server previously returned a 429 (Too Many Requests)
   * @return the cached authorization header
   */
  private @Nullable String getCachedAuthorization(final String aud) throws RateLimitedException {
    final String cached = ResilienceUtil.getGeneralRedisRetry(RETRY_NAME)
    .executeSupplier(() ->
      redisClient.withCluster(cluster -> cluster.sync().get(aud))
    );
    if (cached == RATE_LIMITED) {
      throw new RateLimitedException();
    }
    return cached;
  }

  private void cacheRateLimit(final String aud, final int retryAfterS) {
    final SetArgs args = new SetArgs().ex(retryAfterS);
    ResilienceUtil.getGeneralRedisRetry(RETRY_NAME)
    .executeSupplier(() ->
      redisClient.withCluster(cluster -> cluster.sync().set(aud, RATE_LIMITED, args))
    );
  }
}
