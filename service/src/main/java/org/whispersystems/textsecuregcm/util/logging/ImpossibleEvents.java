/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util.logging;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import net.logstash.logback.marker.Markers;
import org.slf4j.Logger;
import org.slf4j.Marker;

/// Log errors that represent a programming logic error
public final class ImpossibleEvents {

  private static final Marker IMPOSSIBLE = Markers.append("impossible", true);
  private static final Counter COUNTER = Metrics.counter(name(ImpossibleEvents.class, "count"));

  private ImpossibleEvents() {
  }

  /// Log that an event should never happen.
  public static void logImpossible(final Logger logger, final String message, final Object... args) {
    COUNTER.increment();
    logger.error(IMPOSSIBLE, message, args);
  }
}
