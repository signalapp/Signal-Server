/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import static com.codahale.metrics.MetricRegistry.name;

import io.micrometer.core.instrument.Metrics;
import java.util.UUID;
import org.whispersystems.textsecuregcm.storage.ReportMessageManager;
import org.whispersystems.textsecuregcm.storage.ReportedMessageListener;
import org.whispersystems.textsecuregcm.util.Util;

public class ReportedMessageMetricsListener implements ReportedMessageListener {

  // ReportMessageManager name used deliberately to preserve continuity of metrics
  private static final String REPORT_COUNTER_NAME = name(ReportMessageManager.class, "reported");

  @Override
  public void handleMessageReported(final String sourceNumber, final UUID messageGuid, final UUID reporterUuid) {
    Metrics.counter(REPORT_COUNTER_NAME, "countryCode", Util.getCountryCode(sourceNumber)).increment();
  }
}
