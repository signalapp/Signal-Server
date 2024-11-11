/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.auth;


import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response.Status;

public class InvalidAuthorizationHeaderException extends WebApplicationException {
  public InvalidAuthorizationHeaderException(String s) {
    super(s, Status.UNAUTHORIZED);
  }

  public InvalidAuthorizationHeaderException(Exception e) {
    super(e, Status.UNAUTHORIZED);
  }
}
