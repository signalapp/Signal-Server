package org.whispersystems.textsecuregcm.limits;

import com.google.common.annotations.VisibleForTesting;
import io.lettuce.core.ScriptOutputType;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.redis.ClusterLuaScript;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;

public class RedisMessageDeliveryLoopMonitor implements MessageDeliveryLoopMonitor {

  private final ClusterLuaScript getDeliveryAttemptsScript;

  private static final Duration DELIVERY_ATTEMPTS_COUNTER_TTL = Duration.ofHours(1);
  private static final int DELIVERY_LOOP_THRESHOLD = 5;

  private static final Logger logger = LoggerFactory.getLogger(MessageDeliveryLoopMonitor.class);

  public RedisMessageDeliveryLoopMonitor(final FaultTolerantRedisClusterClient rateLimitCluster) {
    try {
      getDeliveryAttemptsScript =
          ClusterLuaScript.fromResource(rateLimitCluster, "lua/get_delivery_attempt_count.lua", ScriptOutputType.INTEGER);
    } catch (final IOException e) {
      throw new UncheckedIOException("Failed to load 'get delivery attempt count' script", e);
    }
  }

  /**
   * Records an attempt to deliver a message with the given GUID to the given account/device pair and returns the number
   * of consecutive attempts to deliver the same message and logs a warning if the message appears to be in a delivery
   * loop. This method is intended to detect cases where a message remains at the head of a device's queue after
   * repeated attempts to deliver the message, and so the given message GUID should be the first message of a "page"
   * sent to clients.
   *
   * @param accountIdentifier the identifier of the destination account
   * @param deviceId the destination device's ID within the given account
   * @param messageGuid the GUID of the message
   * @param userAgent the User-Agent header supplied by the caller
   * @param context a human-readable string identifying the mechanism of message delivery (e.g. "rest" or "websocket")
   */
  public void recordDeliveryAttempt(final UUID accountIdentifier,
      final byte deviceId,
      final UUID messageGuid,
      final String userAgent,
      final String context) {

    incrementDeliveryAttemptCount(accountIdentifier, deviceId, messageGuid)
        .thenAccept(deliveryAttemptCount -> {
              if (deliveryAttemptCount == DELIVERY_LOOP_THRESHOLD) {
                logger.warn("Detected loop delivering message {} via {} to {}:{} ({})",
                    messageGuid, context, accountIdentifier, deviceId, userAgent);
              }
            });
  }

  @VisibleForTesting
  CompletableFuture<Long> incrementDeliveryAttemptCount(final UUID accountIdentifier, final byte deviceId, final UUID messageGuid) {
    final String firstMessageGuidKey = "firstMessageGuid::{" + accountIdentifier + ":" + deviceId + "}";
    final String deliveryAttemptsKey = "firstMessageDeliveryAttempts::{" + accountIdentifier + ":" + deviceId + "}";

    return getDeliveryAttemptsScript.executeAsync(
        List.of(firstMessageGuidKey, deliveryAttemptsKey),
        List.of(messageGuid.toString(), String.valueOf(DELIVERY_ATTEMPTS_COUNTER_TTL.toSeconds())))
        .thenApply(result -> (long) result);
  }

}
