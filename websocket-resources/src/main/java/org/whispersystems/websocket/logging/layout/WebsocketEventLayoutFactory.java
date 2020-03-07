package org.whispersystems.websocket.logging.layout;

import org.whispersystems.websocket.logging.WebsocketEvent;

import java.util.TimeZone;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.pattern.PatternLayoutBase;
import io.dropwizard.logging.layout.LayoutFactory;

public class WebsocketEventLayoutFactory implements LayoutFactory<WebsocketEvent> {
  @Override
  public PatternLayoutBase<WebsocketEvent> build(LoggerContext context, TimeZone timeZone) {
    return new WebsocketEventLayout(context);
  }
}
