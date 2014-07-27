package org.whispersystems.textsecuregcm.websocket;

public class WebsocketAddress {

  private final String number;
  private final long   deviceId;

  public WebsocketAddress(String number, long deviceId) {
    this.number    = number;
    this.deviceId  = deviceId;
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
