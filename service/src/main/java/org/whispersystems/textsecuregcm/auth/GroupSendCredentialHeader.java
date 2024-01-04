/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import java.util.Base64;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response.Status;

import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.groupsend.GroupSendCredentialPresentation;

public record GroupSendCredentialHeader(GroupSendCredentialPresentation presentation) {

  public static GroupSendCredentialHeader valueOf(String header) {
    try {
      return new GroupSendCredentialHeader(new GroupSendCredentialPresentation(Base64.getDecoder().decode(header)));
    } catch (InvalidInputException | IllegalArgumentException e) {
      // Base64 throws IllegalArgumentException; GroupSendCredentialPresentation ctor throws InvalidInputException
      throw new WebApplicationException(e, Status.UNAUTHORIZED);
    }
  }

}
