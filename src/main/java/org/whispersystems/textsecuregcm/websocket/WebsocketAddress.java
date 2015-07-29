package org.whispersystems.textsecuregcm.websocket;

import org.whispersystems.textsecuregcm.storage.PubSubAddress;

public class WebsocketAddress implements PubSubAddress {

  private final String number;
  private final long   deviceId;

  public WebsocketAddress(String number, long deviceId) {
    this.number    = number;
    this.deviceId  = deviceId;
  }

  public WebsocketAddress(String serialized) throws InvalidWebsocketAddressException {
    try {
      String[] parts = serialized.split(":", 2);

      if (parts.length != 2) {
        throw new InvalidWebsocketAddressException("Bad address: " + serialized);
      }

      this.number   = parts[0];
      this.deviceId = Long.parseLong(parts[1]);
    } catch (NumberFormatException e) {
      throw new InvalidWebsocketAddressException(e);
    }
  }

  public String getNumber() {
    return number;
  }

  public long getDeviceId() {
    return deviceId;
  }

  public String serialize() {
    return number + ":" + deviceId;
  }

  public String toString() {
    return serialize();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) return false;
    if (!(other instanceof WebsocketAddress)) return false;

    WebsocketAddress that = (WebsocketAddress)other;

    return
        this.number.equals(that.number) &&
        this.deviceId == that.deviceId;
  }

  @Override
  public int hashCode() {
    return number.hashCode() ^ (int)deviceId;
  }

}
