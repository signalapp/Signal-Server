package org.whispersystems.websocket;

/**
 * Class containing constants and shared logic for headers used in websocket upgrade requests.
 */
public class WebsocketHeaders {
  public final static String X_SIGNAL_RECEIVE_STORIES = "X-Signal-Receive-Stories";

  public static boolean parseReceiveStoriesHeader(String s) {
    return "true".equals(s);
  }
}
