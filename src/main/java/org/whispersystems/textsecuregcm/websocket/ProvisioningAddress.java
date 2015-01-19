package org.whispersystems.textsecuregcm.websocket;

import org.whispersystems.textsecuregcm.util.Base64;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

public class ProvisioningAddress extends WebsocketAddress {

  private static final String PREFIX = ">>ephemeral-";

  private final String address;

  public ProvisioningAddress(String address) throws InvalidWebsocketAddressException {
    super(address, 0);
    this.address = address;

    if (address == null || !address.startsWith(PREFIX)) {
      throw new InvalidWebsocketAddressException(address);
    }
  }

  public String getAddress() {
    return address;
  }

  public static ProvisioningAddress generate() {
    try {
      byte[] random = new byte[16];
      SecureRandom.getInstance("SHA1PRNG").nextBytes(random);

      return new ProvisioningAddress(PREFIX + Base64.encodeBytesWithoutPadding(random)
                                                    .replace('+', '-').replace('/', '_'));
    } catch (NoSuchAlgorithmException | InvalidWebsocketAddressException e) {
      throw new AssertionError(e);
    }
  }
}
