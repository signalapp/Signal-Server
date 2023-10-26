/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.websocket;

import java.security.SecureRandom;
import java.util.Base64;

public class ProvisioningAddress extends WebsocketAddress {

  public static byte DEVICE_ID = 0;

  public static ProvisioningAddress create(String address) {
    return new ProvisioningAddress(address, DEVICE_ID);
  }

  private ProvisioningAddress(String address, byte deviceId) {
    super(address, deviceId);
  }

  public ProvisioningAddress(String serialized) throws InvalidWebsocketAddressException {
    super(serialized);
  }

  public String getAddress() {
    return getNumber();
  }

  public static ProvisioningAddress generate() {
    byte[] random = new byte[16];
    new SecureRandom().nextBytes(random);

    return create(Base64.getUrlEncoder().withoutPadding().encodeToString(random));
  }
}
