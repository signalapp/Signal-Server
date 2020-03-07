package org.whispersystems.websocket.session;

import java.security.Principal;

public class ContextPrincipal implements Principal {

  private final WebSocketSessionContext context;

  public ContextPrincipal(WebSocketSessionContext context) {
    this.context = context;
  }

  @Override
  public boolean equals(Object another) {
    return another instanceof ContextPrincipal &&
           context.equals(((ContextPrincipal) another).context);
  }

  @Override
  public String toString() {
    return super.toString();
  }

  @Override
  public int hashCode() {
    return context.hashCode();
  }

  @Override
  public String getName() {
    return "WebSocketSessionContext";
  }

  public WebSocketSessionContext getContext() {
    return context;
  }
}
