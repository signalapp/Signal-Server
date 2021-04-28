/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import java.util.Base64;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

public class CombinedUnidentifiedSenderAccessKeys {
  private final byte[] combinedUnidentifiedSenderAccessKeys;

  public CombinedUnidentifiedSenderAccessKeys(String header) {
    try {
      this.combinedUnidentifiedSenderAccessKeys = Base64.getDecoder().decode(header);
      if (this.combinedUnidentifiedSenderAccessKeys == null || this.combinedUnidentifiedSenderAccessKeys.length != 16) {
        throw new WebApplicationException("Invalid combined unidentified sender access keys", Status.UNAUTHORIZED);
      }
    } catch (IllegalArgumentException e) {
      throw new WebApplicationException(e, Response.Status.UNAUTHORIZED);
    }
  }

  public byte[] getAccessKeys() {
    return combinedUnidentifiedSenderAccessKeys;
  }
}
