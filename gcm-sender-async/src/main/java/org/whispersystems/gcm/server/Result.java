/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.gcm.server;

/**
 * The result of a GCM send operation.
 */
public class Result {

  private final String canonicalRegistrationId;
  private final String messageId;
  private final String error;

  Result(String canonicalRegistrationId, String messageId, String error) {
    this.canonicalRegistrationId = canonicalRegistrationId;
    this.messageId               = messageId;
    this.error                   = error;
  }

  /**
   * Returns the "canonical" GCM registration ID for this destination.
   * See GCM documentation for details.
   * @return The canonical GCM registration ID.
   */
  public String getCanonicalRegistrationId() {
    return canonicalRegistrationId;
  }

  /**
   * @return If a "canonical" GCM registration ID is present in the response.
   */
  public boolean hasCanonicalRegistrationId() {
    return canonicalRegistrationId != null && !canonicalRegistrationId.isEmpty();
  }

  /**
   * @return The assigned GCM message ID, if successful.
   */
  public String getMessageId() {
    return messageId;
  }

  /**
   * @return The raw error string, if present.
   */
  public String getError() {
    return error;
  }

  /**
   * @return If the send was a success.
   */
  public boolean isSuccess() {
    return messageId != null && !messageId.isEmpty() && (error == null || error.isEmpty());
  }

  /**
   * @return If the destination GCM registration ID is no longer registered.
   */
  public boolean isUnregistered() {
    return "NotRegistered".equals(error);
  }

  /**
   * @return If messages to this device are being throttled.
   */
  public boolean isThrottled() {
    return "DeviceMessageRateExceeded".equals(error);
  }

  /**
   * @return If the destination GCM registration ID is invalid.
   */
  public boolean isInvalidRegistrationId() {
    return "InvalidRegistration".equals(error);
  }

}
