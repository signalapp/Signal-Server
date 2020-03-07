package org.whispersystems.websocket.logging.layout.converters;

import org.whispersystems.websocket.logging.WebsocketEvent;

import ch.qos.logback.core.Context;
import ch.qos.logback.core.pattern.DynamicConverter;
import ch.qos.logback.core.spi.ContextAware;
import ch.qos.logback.core.spi.ContextAwareBase;
import ch.qos.logback.core.status.Status;

public abstract class WebSocketEventConverter extends DynamicConverter<WebsocketEvent> implements  ContextAware {

    public final static char SPACE_CHAR = ' ';
    public final static char QUESTION_CHAR = '?';

    ContextAwareBase cab = new ContextAwareBase();

    @Override
    public void setContext(Context context) {
      cab.setContext(context);
    }

    @Override
    public Context getContext() {
      return cab.getContext();
    }

    @Override
    public void addStatus(Status status) {
      cab.addStatus(status);
    }

    @Override
    public void addInfo(String msg) {
      cab.addInfo(msg);
    }

    @Override
    public void addInfo(String msg, Throwable ex) {
      cab.addInfo(msg, ex);
    }

    @Override
    public void addWarn(String msg) {
      cab.addWarn(msg);
    }

    @Override
    public void addWarn(String msg, Throwable ex) {
      cab.addWarn(msg, ex);
    }

    @Override
    public void addError(String msg) {
      cab.addError(msg);
    }

    @Override
    public void addError(String msg, Throwable ex) {
      cab.addError(msg, ex);
    }

}
