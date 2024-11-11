/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import java.util.Base64;

public class Anonymous {

  private final byte[] unidentifiedSenderAccessKey;

  public Anonymous(String header) {
    try {
      this.unidentifiedSenderAccessKey = Base64.getDecoder().decode(header);
    } catch (IllegalArgumentException e) {
      throw new WebApplicationException(e, Response.Status.UNAUTHORIZED);
    }
  }

  public byte[] getAccessKey() {
    return unidentifiedSenderAccessKey;
  }
}
