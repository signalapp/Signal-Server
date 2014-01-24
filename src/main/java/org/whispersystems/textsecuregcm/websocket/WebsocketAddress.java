package org.whispersystems.textsecuregcm.websocket;

public class WebsocketAddress {

  private final long accountId;
  private final long deviceId;

  public WebsocketAddress(String serialized) throws InvalidWebsocketAddressException {
    try {
      String[] parts = serialized.split(":");

      if (parts == null || parts.length != 2) {
        throw new InvalidWebsocketAddressException(serialized);
      }

      this.accountId = Long.parseLong(parts[0]);
      this.deviceId  = Long.parseLong(parts[1]);
    } catch (NumberFormatException e) {
      throw new InvalidWebsocketAddressException(e);
    }
  }

  public WebsocketAddress(long accountId, long deviceId) {
    this.accountId = accountId;
    this.deviceId  = deviceId;
  }

  public long getAccountId() {
    return accountId;
  }

  public long getDeviceId() {
    return deviceId;
  }

  public String toString() {
    return accountId + ":" + deviceId;
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) return false;
    if (!(other instanceof WebsocketAddress)) return false;

    WebsocketAddress that = (WebsocketAddress)other;

    return
        this.accountId == that.accountId &&
        this.deviceId == that.deviceId;
  }

  @Override
  public int hashCode() {
    return (int)accountId ^ (int)deviceId;
  }

}
