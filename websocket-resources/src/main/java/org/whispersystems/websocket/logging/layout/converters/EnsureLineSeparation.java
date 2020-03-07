package org.whispersystems.websocket.logging.layout.converters;

import org.whispersystems.websocket.logging.WebsocketEvent;

import ch.qos.logback.core.Context;
import ch.qos.logback.core.pattern.Converter;
import ch.qos.logback.core.pattern.ConverterUtil;
import ch.qos.logback.core.pattern.PostCompileProcessor;

public class EnsureLineSeparation implements PostCompileProcessor<WebsocketEvent> {

  /**
   * Add a line separator converter so that access event appears on a separate
   * line.
   */
  @Override
  public void process(Context context, Converter<WebsocketEvent> head) {
    if (head == null)
      throw new IllegalArgumentException("Empty converter chain");

    // if head != null, then tail != null as well
    Converter<WebsocketEvent> tail             = ConverterUtil.findTail(head);
    Converter<WebsocketEvent> newLineConverter = new LineSeparatorConverter();

    if (!(tail instanceof LineSeparatorConverter)) {
      tail.setNext(newLineConverter);
    }
  }
}
