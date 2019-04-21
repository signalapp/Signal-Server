package org.whispersystems.dispatch;

public interface DispatchChannel {
  public void onDispatchMessage(String channel, byte[] message);
  public void onDispatchSubscribed(String channel);
  public void onDispatchUnsubscribed(String channel);
}
