package org.whispersystems.websocket;

/**
 * Class containing constants and shared logic for headers used in websocket upgrade requests.
 */
public class WebsocketHeaders {
  public final static String X_SIGNAL_RECEIVE_STORIES = "X-Signal-Receive-Stories";

  public final static String X_SIGNAL_DISABLE_MESSAGES = "X-Signal-Disable-Messages";

  /// Parse a boolean header value
  ///
  /// @param s the value of an HTTP header
  /// @return true if the header is "true", otherwise false
  public static boolean parseBooleanHeader(String s) {
    return "true".equals(s);
  }
}
