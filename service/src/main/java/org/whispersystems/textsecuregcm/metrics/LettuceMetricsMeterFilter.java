/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.config.MeterFilter;

public class LettuceMetricsMeterFilter implements MeterFilter {

  private static final String METRIC_NAME_PREFIX = "lettuce.command";
  private static final String REMOTE_TAG = "remote";

  // the `remote` tag is very high-cardinality, so we ignore it.
  // In the future, it would be nice to map a remote (address:port) to a logical cluster name
  private static final MeterFilter IGNORE_TAGS_FILTER = MeterFilter.ignoreTags(REMOTE_TAG);

  @Override
  public Meter.Id map(final Meter.Id id) {

    if (id.getName().startsWith(METRIC_NAME_PREFIX)) {
      return IGNORE_TAGS_FILTER.map(id);
    }

    return id;
  }
}
