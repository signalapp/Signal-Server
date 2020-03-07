package org.whispersystems.websocket;

import org.whispersystems.websocket.session.ContextPrincipal;
import org.whispersystems.websocket.session.WebSocketSessionContext;

import javax.ws.rs.core.SecurityContext;
import java.security.Principal;

public class WebSocketSecurityContext implements SecurityContext {

  private final ContextPrincipal principal;

  public WebSocketSecurityContext(ContextPrincipal principal) {
    this.principal = principal;
  }

  @Override
  public Principal getUserPrincipal() {
    return (Principal)principal.getContext().getAuthenticated();
  }

  @Override
  public boolean isUserInRole(String role) {
    return false;
  }

  @Override
  public boolean isSecure() {
    return principal != null;
  }

  @Override
  public String getAuthenticationScheme() {
    return null;
  }

  public WebSocketSessionContext getSessionContext() {
    return principal.getContext();
  }
}
