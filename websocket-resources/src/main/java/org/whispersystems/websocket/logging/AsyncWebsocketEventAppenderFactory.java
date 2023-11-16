/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.websocket.logging;

import ch.qos.logback.core.AsyncAppenderBase;
import io.dropwizard.logging.common.async.AsyncAppenderFactory;

public class AsyncWebsocketEventAppenderFactory implements AsyncAppenderFactory<WebsocketEvent> {
  @Override
  public AsyncAppenderBase<WebsocketEvent> build() {
    return new AsyncAppenderBase<WebsocketEvent>() {
      @Override
      protected void preprocess(WebsocketEvent event) {
        event.prepareForDeferredProcessing();
      }
    };
  }
}
