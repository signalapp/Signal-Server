/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.websocket.logging.layout;

import ch.qos.logback.core.Context;
import ch.qos.logback.core.pattern.DynamicConverter;
import ch.qos.logback.core.pattern.PatternLayoutBase;
import org.whispersystems.websocket.logging.WebsocketEvent;
import org.whispersystems.websocket.logging.layout.converters.ContentLengthConverter;
import org.whispersystems.websocket.logging.layout.converters.DateConverter;
import org.whispersystems.websocket.logging.layout.converters.EnsureLineSeparation;
import org.whispersystems.websocket.logging.layout.converters.NAConverter;
import org.whispersystems.websocket.logging.layout.converters.RemoteHostConverter;
import org.whispersystems.websocket.logging.layout.converters.RequestHeaderConverter;
import org.whispersystems.websocket.logging.layout.converters.RequestUrlConverter;
import org.whispersystems.websocket.logging.layout.converters.StatusCodeConverter;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class WebsocketEventLayout extends PatternLayoutBase<WebsocketEvent> {

  // Provides a mapping of conversion words to converter classes;
  // required for extending PatternLayoutBase.
  // See https://logback.qos.ch/manual/layouts.html#ClassicPatternLayout for more details.
  private static final Map<String, Supplier<DynamicConverter>> DEFAULT_CONVERTER_SUPPLIERS = Map.of(
      "h", RemoteHostConverter::new,
      "l", NAConverter::new,
      "u", NAConverter::new,
      "t", DateConverter::new,
      "r", RequestUrlConverter::new,
      "s", StatusCodeConverter::new,
      "b", ContentLengthConverter::new,
      "i", RequestHeaderConverter::new
  );

  // Provided for backwards compatibility
  private static final Map<String, String> DEFAULT_CONVERTERS = new HashMap<>() {{
    put("h", RemoteHostConverter.class.getName());
    put("l", NAConverter.class.getName());
    put("u", NAConverter.class.getName());
    put("t", DateConverter.class.getName());
    put("r", RequestUrlConverter.class.getName());
    put("s", StatusCodeConverter.class.getName());
    put("b", ContentLengthConverter.class.getName());
    put("i", RequestHeaderConverter.class.getName());
  }};

  public static final String CLF_PATTERN = "%h %l %u [%t] \"%r\" %s %b";
  public static final String CLF_PATTERN_NAME = "common";
  public static final String CLF_PATTERN_NAME_2 = "clf";
  public static final String COMBINED_PATTERN = "%h %l %u [%t] \"%r\" %s %b \"%i{Referer}\" \"%i{User-Agent}\"";
  public static final String COMBINED_PATTERN_NAME = "combined";
  public static final String HEADER_PREFIX = "#logback.access pattern: ";

  public WebsocketEventLayout(Context context) {
    setOutputPatternAsHeader(false);
    setPattern(COMBINED_PATTERN);
    setContext(context);

    this.postCompileProcessor = new EnsureLineSeparation();
  }

  @Override
  protected Map<String, Supplier<DynamicConverter>> getDefaultConverterSupplierMap() {
    return DEFAULT_CONVERTER_SUPPLIERS;
  }

  @Override
  public Map<String, String> getDefaultConverterMap() {
    return DEFAULT_CONVERTERS;
  }

  @Override
  public String doLayout(WebsocketEvent event) {
    if (!isStarted()) {
      return null;
    }

    return writeLoopOnConverters(event);
  }

  @Override
  public void start() {
    if (getPattern().equalsIgnoreCase(CLF_PATTERN_NAME) || getPattern().equalsIgnoreCase(CLF_PATTERN_NAME_2)) {
      setPattern(CLF_PATTERN);
    } else if (getPattern().equalsIgnoreCase(COMBINED_PATTERN_NAME)) {
      setPattern(COMBINED_PATTERN);
    }

    super.start();
  }

  @Override
  protected String getPresentationHeaderPrefix() {
    return HEADER_PREFIX;
  }

}
