package org.whispersystems.textsecuregcm.auth;

import org.whispersystems.textsecuregcm.util.Base64;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import java.io.IOException;

public class Anonymous {

  private final byte[] unidentifiedSenderAccessKey;

  public Anonymous(String header) {
    try {
      this.unidentifiedSenderAccessKey = Base64.decode(header);
    } catch (IOException e) {
      throw new WebApplicationException(e, Response.Status.UNAUTHORIZED);
    }
  }

  public byte[] getAccessKey() {
    return unidentifiedSenderAccessKey;
  }
}
