package org.whispersystems.websocket.session;

import org.glassfish.jersey.server.ContainerRequest;
import org.whispersystems.websocket.WebSocketSecurityContext;

import javax.ws.rs.core.SecurityContext;

public class WebSocketSessionContainerRequestValueFactory {
  private final ContainerRequest request;

  public WebSocketSessionContainerRequestValueFactory(ContainerRequest request) {
    this.request = request;
  }

  public WebSocketSessionContext provide() {
    SecurityContext securityContext = request.getSecurityContext();

    if (!(securityContext instanceof WebSocketSecurityContext)) {
      throw new IllegalStateException("Security context isn't for websocket!");
    }

    WebSocketSessionContext sessionContext = ((WebSocketSecurityContext)securityContext).getSessionContext();

    if (sessionContext == null) {
      throw new IllegalStateException("No session context found for websocket!");
    }

    return sessionContext;
  }

}
