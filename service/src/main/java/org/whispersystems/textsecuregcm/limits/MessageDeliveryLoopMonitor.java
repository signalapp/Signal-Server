package org.whispersystems.textsecuregcm.limits;

import java.util.UUID;

public interface MessageDeliveryLoopMonitor {
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
  void recordDeliveryAttempt(UUID accountIdentifier, byte deviceId, UUID messageGuid, String userAgent, String context);
}
