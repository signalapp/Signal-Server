package org.whispersystems.websocket;

/**
 * Class containing constants and shared logic for handling stories.
 * <p>
 * In particular, it defines the way we interpret the X-Signal-Receive-Stories header
 * which is used by both WebSockets and by the REST API.
 */
public class Stories {
  public final static String X_SIGNAL_RECEIVE_STORIES = "X-Signal-Receive-Stories";

  public static boolean parseReceiveStoriesHeader(String s) {
    return "true".equals(s);
  }
}
