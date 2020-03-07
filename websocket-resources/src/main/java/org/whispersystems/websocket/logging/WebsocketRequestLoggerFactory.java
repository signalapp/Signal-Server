package org.whispersystems.websocket.logging;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.LoggerFactory;
import org.whispersystems.websocket.logging.layout.WebsocketEventLayoutFactory;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Collections;
import java.util.List;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import io.dropwizard.logging.AppenderFactory;
import io.dropwizard.logging.ConsoleAppenderFactory;
import io.dropwizard.logging.async.AsyncAppenderFactory;
import io.dropwizard.logging.filter.LevelFilterFactory;
import io.dropwizard.logging.filter.NullLevelFilterFactory;
import io.dropwizard.logging.layout.LayoutFactory;

public class WebsocketRequestLoggerFactory {

  @VisibleForTesting
  @Valid
  @NotNull
  public List<AppenderFactory<WebsocketEvent>> appenders = Collections.singletonList(new ConsoleAppenderFactory<>());

  public WebsocketRequestLog build(String name) {
    final Logger logger = (Logger) LoggerFactory.getLogger("websocket.request");
    logger.setAdditive(false);

    final LoggerContext                        context              = logger.getLoggerContext();
    final WebsocketRequestLog                  requestLog           = new WebsocketRequestLog();
    final LevelFilterFactory<WebsocketEvent>   levelFilterFactory   = new NullLevelFilterFactory<>();
    final AsyncAppenderFactory<WebsocketEvent> asyncAppenderFactory = new AsyncWebsocketEventAppenderFactory();
    final LayoutFactory<WebsocketEvent>        layoutFactory        = new WebsocketEventLayoutFactory();

    for (AppenderFactory<WebsocketEvent> output : appenders) {
      requestLog.addAppender(output.build(context, name, layoutFactory, levelFilterFactory, asyncAppenderFactory));
    }

    return requestLog;
  }

}
