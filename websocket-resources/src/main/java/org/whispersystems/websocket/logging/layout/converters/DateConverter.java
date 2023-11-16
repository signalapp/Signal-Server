/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.websocket.logging.layout.converters;

import ch.qos.logback.core.CoreConstants;
import ch.qos.logback.core.util.CachingDateFormatter;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import org.whispersystems.websocket.logging.WebsocketEvent;

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
      List<String> optionList = getOptionList();

      Optional<ZoneId> timeZone = Optional.empty();
      // if the option list contains a TZ option, then set it.
      if (optionList != null && optionList.size() > 1) {
        timeZone = Optional.of(ZoneId.of((String) optionList.get(1)));
      }

      cachingDateFormatter = new CachingDateFormatter(datePattern, timeZone.orElse(null));
      // maximumCacheValidity = CachedDateFormat.getMaximumCacheValidity(pattern);
    } catch (IllegalArgumentException e) {
      addWarn("Could not instantiate SimpleDateFormat with pattern " + datePattern, e);
      addWarn("Defaulting to  " + CoreConstants.CLF_DATE_PATTERN);
      cachingDateFormatter = new CachingDateFormatter(CoreConstants.CLF_DATE_PATTERN);
    }


  }

  @Override
  public String convert(WebsocketEvent websocketEvent) {
    long timestamp = websocketEvent.getTimestamp();
    return cachingDateFormatter.format(timestamp);
  }

}
