/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.registration;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * An error from an SMS/voice provider (“sender”) downstream of Registration Service is mapped to a {@link Reason}, and
 * may be permanent.
 */
public class RegistrationServiceSenderException extends Exception {

  private final Reason reason;
  private final boolean permanent;

  public static RegistrationServiceSenderException illegalArgument(final boolean permanent) {
    return new RegistrationServiceSenderException(Reason.ILLEGAL_ARGUMENT, permanent);
  }

  public static RegistrationServiceSenderException rejected(final boolean permanent) {
    return new RegistrationServiceSenderException(Reason.PROVIDER_REJECTED, permanent);
  }

  public static RegistrationServiceSenderException unknown(final boolean permanent) {
    return new RegistrationServiceSenderException(Reason.PROVIDER_UNAVAILABLE, permanent);
  }

  private RegistrationServiceSenderException(final Reason reason, final boolean permanent) {
    super(null, null, true, false);
    this.reason = reason;
    this.permanent = permanent;
  }

  public Reason getReason() {
    return reason;
  }

  public boolean isPermanent() {
    return permanent;
  }

  public enum Reason {

    @JsonProperty("providerUnavailable")
    PROVIDER_UNAVAILABLE,
    @JsonProperty("providerRejected")
    PROVIDER_REJECTED,
    @JsonProperty("illegalArgument")
    ILLEGAL_ARGUMENT
  }
}
