/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import java.util.Base64;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.donation.DonationPermit;

public record DonationPermitHeader(DonationPermit permit) {

  public static DonationPermitHeader valueOf(String header) {
    try {
      return new DonationPermitHeader(new DonationPermit(Base64.getDecoder().decode(header)));
    } catch (InvalidInputException | IllegalArgumentException e) {
      // Base64 throws IllegalArgumentException; DonationPermit ctor throws InvalidInputException
      throw new WebApplicationException(e, Response.Status.UNAUTHORIZED);
    }
  }
}
