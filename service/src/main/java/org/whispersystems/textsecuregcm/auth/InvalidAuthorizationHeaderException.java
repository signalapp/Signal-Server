/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.auth;


import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response.Status;

public class InvalidAuthorizationHeaderException extends WebApplicationException {
  public InvalidAuthorizationHeaderException(String s) {
    super(s, Status.UNAUTHORIZED);
  }

  public InvalidAuthorizationHeaderException(Exception e) {
    super(e, Status.UNAUTHORIZED);
  }
}
