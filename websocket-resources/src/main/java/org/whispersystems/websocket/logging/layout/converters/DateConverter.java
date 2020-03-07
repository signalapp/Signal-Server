package org.whispersystems.websocket.logging.layout.converters;

import org.whispersystems.websocket.logging.WebsocketEvent;

import java.util.List;
import java.util.TimeZone;

import ch.qos.logback.core.CoreConstants;
import ch.qos.logback.core.util.CachingDateFormatter;

public class DateConverter extends WebSocketEventConverter {

  private CachingDateFormatter cachingDateFormatter = null;

  @Override
  public void start() {

    String datePattern = getFirstOption();
    if (datePattern == null) {
      datePattern = CoreConstants.CLF_DATE_PATTERN;
    }

    if (datePattern.equals(CoreConstants.ISO8601_STR)) {
      datePattern = CoreConstants.ISO8601_PATTERN;
    }

    try {
      cachingDateFormatter = new CachingDateFormatter(datePattern);
      // maximumCacheValidity = CachedDateFormat.getMaximumCacheValidity(pattern);
    } catch (IllegalArgumentException e) {
      addWarn("Could not instantiate SimpleDateFormat with pattern " + datePattern, e);
      addWarn("Defaulting to  " + CoreConstants.CLF_DATE_PATTERN);
      cachingDateFormatter = new CachingDateFormatter(CoreConstants.CLF_DATE_PATTERN);
    }

    List optionList = getOptionList();

    // if the option list contains a TZ option, then set it.
    if (optionList != null && optionList.size() > 1) {
      TimeZone tz = TimeZone.getTimeZone((String) optionList.get(1));
      cachingDateFormatter.setTimeZone(tz);
    }
  }

  @Override
  public String convert(WebsocketEvent websocketEvent) {
    long timestamp = websocketEvent.getTimestamp();
    return cachingDateFormatter.format(timestamp);
  }

}
