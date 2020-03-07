package org.whispersystems.websocket.logging;

import com.google.common.annotations.VisibleForTesting;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.ContainerResponse;

import ch.qos.logback.core.Appender;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.AppenderAttachableImpl;
import ch.qos.logback.core.spi.FilterAttachableImpl;
import ch.qos.logback.core.spi.FilterReply;

public class WebsocketRequestLog {

  private AppenderAttachableImpl<WebsocketEvent> aai = new AppenderAttachableImpl<>();
  private FilterAttachableImpl<WebsocketEvent>   fai = new FilterAttachableImpl<>();

  public WebsocketRequestLog() {
  }

  public void log(String remoteAddress, ContainerRequest jerseyRequest, ContainerResponse jettyResponse) {
    WebsocketEvent event = new WebsocketEvent(remoteAddress, jerseyRequest, jettyResponse);

    if (getFilterChainDecision(event) == FilterReply.DENY) {
      return;
    }

    aai.appendLoopOnAppenders(event);
  }


  public void addAppender(Appender<WebsocketEvent> newAppender) {
    aai.addAppender(newAppender);
  }

  public void addFilter(Filter<WebsocketEvent> newFilter) {
      fai.addFilter(newFilter);
    }

  public FilterReply getFilterChainDecision(WebsocketEvent event) {
    return fai.getFilterChainDecision(event);
  }
}
