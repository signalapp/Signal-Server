/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import java.io.IOException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.whispersystems.textsecuregcm.util.Base64;

public class CombinedUnidentifiedSenderAccessKeys {
  private final byte[] combinedUnidentifiedSenderAccessKeys;

  public CombinedUnidentifiedSenderAccessKeys(String header) {
    try {
      this.combinedUnidentifiedSenderAccessKeys = Base64.decode(header);
      if (this.combinedUnidentifiedSenderAccessKeys == null || this.combinedUnidentifiedSenderAccessKeys.length != 16) {
        throw new WebApplicationException("Invalid combined unidentified sender access keys", Status.UNAUTHORIZED);
      }
    } catch (IOException e) {
      throw new WebApplicationException(e, Response.Status.UNAUTHORIZED);
    }
  }

  public byte[] getAccessKeys() {
    return combinedUnidentifiedSenderAccessKeys;
  }
}
