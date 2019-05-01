/*
 * Copyright (C) 2014 Open WhisperSystems
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.whispersystems.websocket.session;

import org.whispersystems.websocket.WebSocketClient;

import java.util.LinkedList;
import java.util.List;

public class WebSocketSessionContext {

  private final List<WebSocketEventListener> closeListeners = new LinkedList<>();

  private final WebSocketClient webSocketClient;

  private Object authenticated;
  private boolean closed;

  public WebSocketSessionContext(WebSocketClient webSocketClient) {
    this.webSocketClient = webSocketClient;
  }

  public void setAuthenticated(Object authenticated) {
    this.authenticated = authenticated;
  }

  public <T> T getAuthenticated(Class<T> clazz) {
    if (authenticated != null && clazz.equals(authenticated.getClass())) {
      return clazz.cast(authenticated);
    }

    throw new IllegalArgumentException("No authenticated type for: " + clazz + ", we have: " + authenticated);
  }

  public Object getAuthenticated() {
    return authenticated;
  }

  public synchronized void addListener(WebSocketEventListener listener) {
    if (!closed) this.closeListeners.add(listener);
    else         listener.onWebSocketClose(this, 1000, "Closed");
  }

  public WebSocketClient getClient() {
    return webSocketClient;
  }

  public synchronized void notifyClosed(int statusCode, String reason) {
    for (WebSocketEventListener listener : closeListeners) {
      listener.onWebSocketClose(this, statusCode, reason);
    }

    closed = true;
  }

  public interface WebSocketEventListener {
    public void onWebSocketClose(WebSocketSessionContext context, int statusCode, String reason);
  }


}
