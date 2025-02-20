/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.websocket.logging;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.logging.common.AppenderFactory;
import io.dropwizard.logging.common.ConsoleAppenderFactory;
import io.dropwizard.logging.common.async.AsyncAppenderFactory;
import io.dropwizard.logging.common.filter.LevelFilterFactory;
import io.dropwizard.logging.common.filter.NullLevelFilterFactory;
import io.dropwizard.logging.common.layout.LayoutFactory;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.util.Collections;
import java.util.List;
import org.slf4j.LoggerFactory;
import org.whispersystems.websocket.logging.layout.WebsocketEventLayoutFactory;

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
