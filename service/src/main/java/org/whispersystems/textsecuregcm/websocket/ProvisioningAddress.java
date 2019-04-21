package org.whispersystems.textsecuregcm.websocket;

import org.whispersystems.textsecuregcm.util.Base64;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

public class ProvisioningAddress extends WebsocketAddress {

  public ProvisioningAddress(String address, int id) throws InvalidWebsocketAddressException {
    super(address, id);
  }

  public ProvisioningAddress(String serialized) throws InvalidWebsocketAddressException {
    super(serialized);
  }

  public String getAddress() {
    return getNumber();
  }

  public static ProvisioningAddress generate() {
    try {
      byte[] random = new byte[16];
      new SecureRandom().nextBytes(random);

      return new ProvisioningAddress(Base64.encodeBytesWithoutPadding(random)
                                           .replace('+', '-').replace('/', '_'), 0);
    } catch (InvalidWebsocketAddressException e) {
      throw new AssertionError(e);
    }
  }
}
