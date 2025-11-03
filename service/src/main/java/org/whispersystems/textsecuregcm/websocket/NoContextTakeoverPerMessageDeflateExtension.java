package org.whispersystems.textsecuregcm.websocket;

import org.eclipse.jetty.websocket.core.ExtensionConfig;
import org.eclipse.jetty.websocket.core.WebSocketComponents;
import org.eclipse.jetty.websocket.core.internal.PerMessageDeflateExtension;

/// A variant of the Jetty {@link PerMessageDeflateExtension} that always negotiates the [server_no_context_takeover
/// extension parameter](https://datatracker.ietf.org/doc/html/rfc7692#section-7.1.1.1)
public final class NoContextTakeoverPerMessageDeflateExtension extends PerMessageDeflateExtension {

  @Override
  public void init(ExtensionConfig config, WebSocketComponents components) {
    config.setParameter("server_no_context_takeover");
    super.init(config, components);
  }
}
