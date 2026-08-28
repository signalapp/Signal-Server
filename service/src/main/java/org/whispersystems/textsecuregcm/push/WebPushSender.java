/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.Signature;
import java.security.interfaces.ECPublicKey;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.signal.libsignal.protocol.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.http.FaultTolerantHttpClient;
import org.whispersystems.textsecuregcm.push.PushNotification.PushToken;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;
import org.whispersystems.textsecuregcm.util.ResilienceUtil;
import org.whispersystems.textsecuregcm.util.SystemMapper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.net.HttpHeaders;
import com.google.crypto.tink.apps.webpush.WebPushHybridEncrypt;
import com.google.crypto.tink.subtle.EllipticCurves;

import io.lettuce.core.SetArgs;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import net.logstash.logback.util.StringUtils;

public class WebPushSender implements PushNotificationSender {

  private static final Logger logger = LoggerFactory.getLogger(WebPushSender.class);
  private final FaultTolerantRedisClusterClient redisClient;
  private final KeyPair vapidKp;
  private final String vapidSub;

  /**
   * Cache aud -> HttpClient
   *
   * We use one http client per origin,
   * to get a circuit breaker per origin.
   * That way, an origin timing out doesn't prevent push notifications
   * to other push servers.
   */
  private final LoadingCache<String, FaultTolerantHttpClient> httpClients;

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

  private class RateLimitedException extends Exception {}

  public WebPushSender (ExecutorService executor, FaultTolerantRedisClusterClient redisClient, KeyPair vapidKp, String vapidSub) throws IOException {
    CacheLoader<String, FaultTolerantHttpClient> loader;
    loader = new CacheLoader<String, FaultTolerantHttpClient>() {
        @Override
        public FaultTolerantHttpClient load(String key) {
          return FaultTolerantHttpClient.newBuilder("webpush:" + key, executor)
            .withRedirect(HttpClient.Redirect.NEVER)
      .     build();
        }
    };
    this.httpClients = CacheBuilder.newBuilder()
      .expireAfterAccess(1, TimeUnit.MINUTES)
      .build(loader);
    this.redisClient = redisClient;
    this.vapidKp = vapidKp;
    this.vapidSub = vapidSub;
  }

  @VisibleForTesting
  public WebPushSender (FaultTolerantRedisClusterClient redisClient, KeyPair vapidKp, String vapidSub, FaultTolerantHttpClient httpClient) {
    CacheLoader<String, FaultTolerantHttpClient> loader;
    loader = new CacheLoader<String, FaultTolerantHttpClient>() {
        @Override
        public FaultTolerantHttpClient load(String key) {
          return httpClient;
        }
    };
    this.httpClients = CacheBuilder.newBuilder()
      .expireAfterAccess(1, TimeUnit.MINUTES)
      .build(loader);
    this.redisClient = redisClient;
    this.vapidKp = vapidKp;
    this.vapidSub = vapidSub;
  }

  @Override
  public CompletableFuture<SendPushNotificationResult> sendNotification(PushNotification pushNotification) {
    final PushToken.WEBPUSH pushToken = (PushToken.WEBPUSH) pushNotification.pushToken();
    final WebPushSubscription sub = pushToken.value();

    if (!pushToken.activated()) {
      return CompletableFuture.completedFuture(
        new SendPushNotificationResult(false, Optional.of("Subscription not yet activated"), false, Optional.empty())
      );
    }

    final String aud = String.format("https://%s", sub.endpoint().getAuthority());

    String authorization;
    try {
      authorization = getCachedAuthorization(aud, sub.endpoint());
    } catch (RateLimitedException e) {
      return CompletableFuture.completedFuture(
        new SendPushNotificationResult(false, Optional.of("Rate limited"), false, Optional.empty())
      );
    }

    if (StringUtils.isBlank(authorization)) {
      try {
        authorization = genAuthorization(vapidKp, aud, vapidSub);
      } catch (Exception e) {
        logger.warn("Error while making vapid authorization", e);
        return CompletableFuture.completedFuture(
          new SendPushNotificationResult(false, Optional.of("Cannot make VAPID auth"), false, Optional.empty())
        );
      }
      cacheAuthorization(aud, authorization);
    }

    final Map<String, String> map = new HashMap<String, String>();

    final String key = switch (pushNotification.notificationType()) {
      case NOTIFICATION -> "newMessageAlert";
      case ATTEMPT_LOGIN_NOTIFICATION_HIGH_PRIORITY -> "attemptLoginContext";
      case CHALLENGE -> "challenge";
      case RATE_LIMIT_CHALLENGE -> "rateLimitChallenge";
      case ACTIVATION_TOKEN -> "activationToken";
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
      .header("Urgency", pushNotification.urgent() ? "high" : "normal")
      .header("Authorization", authorization);

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

    FaultTolerantHttpClient httpClient;
    try {
      httpClient = httpClients.get(aud);
    } catch (Exception e) {
      logger.warn("Error while getting httpClient for " + aud, e);
      return CompletableFuture.completedFuture(new SendPushNotificationResult(false, Optional.of("Error while getting httpClient for " + aud), false, Optional.empty()));
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
             cacheRateLimit(sub.endpoint(), retryAfterS);
             yield new SendPushNotificationResult(false, Optional.of("Rate limited"), false, Optional.empty());
           }
           default ->
               new SendPushNotificationResult(false, Optional.of(String.valueOf(code)), false, Optional.empty());
        };
       });
  }

  /**
   * Key to cache rate limited endpoints
   */
  private static String rateLimitKey(final String endpoint) {
    return "WebPush::RateLimit::" + endpoint;
  }

  /**
   * Key to cache VAPID header for push servers
   */
  private static String headerKey(final String aud) {
    return "WebPush::Auth::" + aud;
  }

  /**
   * @throws RateLimitedException if the aud server previously returned a 429 (Too Many Requests)
   * @return the cached authorization header
   */
  private @Nullable String getCachedAuthorization(final String aud, final URI endpoint) throws RateLimitedException {
    final Pair<String, String> cached = ResilienceUtil.getGeneralRedisRetry(RETRY_NAME)
    .executeSupplier(() ->
      redisClient.withCluster(cluster -> {
        RedisAdvancedClusterCommands<String, String> cmd = cluster.sync();
        String rateLimited = cmd.get(rateLimitKey(endpoint.toString()));
        String cachedHeader = cmd.get(headerKey(aud));
        return new Pair<String, String>(rateLimited, cachedHeader);
      })
    );
    if (cached.first() == RATE_LIMITED) {
      throw new RateLimitedException();
    }
    return cached.second();
  }

  private void cacheRateLimit(final URI endpoint, final int retryAfterS) {
    final SetArgs args = new SetArgs().ex(retryAfterS);
    ResilienceUtil.getGeneralRedisRetry(RETRY_NAME)
    .executeSupplier(() ->
      redisClient.withCluster(cluster -> cluster.sync().set(rateLimitKey(endpoint.toString()), RATE_LIMITED, args))
    );
  }

  private void cacheAuthorization(final String aud, final String authHeader) {
    // NX: only set the value if the key doesn't already exists
    final SetArgs args = new SetArgs().ex(AUTH_CACHE_DURATION).nx();
    ResilienceUtil.getGeneralRedisRetry(RETRY_NAME)
    .executeSupplier(() ->
      redisClient.withCluster(cluster -> cluster.sync().set(headerKey(aud), authHeader, args))
    );
  }

  private static String genAuthorization(final KeyPair kp, final String aud, final String sub) throws JsonProcessingException, GeneralSecurityException {
    return genAuthorization(kp, aud, sub, (int) (System.currentTimeMillis() / 1000));
  }

  @VisibleForTesting
  static String genAuthorization(final KeyPair kp, final String aud, final String sub, final int currentTimeSec) throws JsonProcessingException, GeneralSecurityException {
    final Map<String, String> headerMap = new HashMap<String, String>();
    headerMap.put("alg", "ES256");
    headerMap.put("typ", "JWT");
    final byte[] header = Base64.getUrlEncoder().withoutPadding().encode(
      SystemMapper.jsonMapper().writeValueAsString(headerMap).getBytes()
    );

    // The header expire after 15 min
    final int exp = currentTimeSec + 900;
    final Map<String, Object> bodyMap = new HashMap<String, Object>();
    bodyMap.put("aud", aud);
    bodyMap.put("exp", exp);
    bodyMap.put("sub", sub);
    final byte[] body = Base64.getUrlEncoder().withoutPadding().encode(
      SystemMapper.jsonMapper().writeValueAsString(bodyMap).getBytes()
    );

    final byte[] toSign = ByteBuffer.allocate(header.length + body.length + 1)
      .put(header)
      .put((byte) '.')
      .put(body)
      .array();

    final byte[] signature = Base64.getUrlEncoder().withoutPadding().encode(
      sign(kp, toSign)
    );
    final String jwt = new String(
      ByteBuffer.allocate(toSign.length + signature.length + 1)
        .put(toSign)
        .put((byte) '.')
        .put(signature)
        .array()
    );
    final String k = Base64.getUrlEncoder().withoutPadding().encodeToString(
      EllipticCurves.pointEncode(
        EllipticCurves.CurveType.NIST_P256,
        EllipticCurves.PointFormatType.UNCOMPRESSED,
        ((ECPublicKey) kp.getPublic()).getW()
      )
    );
    return String.format("vapid t=%s,k=%s", jwt, k);
  }

  private static byte[] sign(final KeyPair kp, final byte[] data) throws GeneralSecurityException {
    final Signature engine = Signature.getInstance("SHA256withECDSA");
    engine.initSign(kp.getPrivate());
    engine.update(data);
    byte[] signature = engine.sign();
    return EllipticCurves.ecdsaDer2Ieee(signature, 64);
  }
}
