/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import static com.codahale.metrics.MetricRegistry.name;

import io.micrometer.core.instrument.Metrics;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import net.logstash.logback.marker.Markers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.ReportMessageManager;
import org.whispersystems.textsecuregcm.storage.ReportedMessageListener;
import org.whispersystems.textsecuregcm.util.Util;

public class ReportedMessageMetricsListener implements ReportedMessageListener {

  private final AccountsManager accountsManager;

  // ReportMessageManager name used deliberately to preserve continuity of metrics
  private static final String REPORTED_COUNTER_NAME = name(ReportMessageManager.class, "reported");
  private static final String REPORTER_COUNTER_NAME = name(ReportMessageManager.class, "reporter");

  private static final String COUNTRY_CODE_TAG_NAME = "countryCode";

  private static final Logger logger = LoggerFactory.getLogger(ReportedMessageMetricsListener.class);

  public ReportedMessageMetricsListener(final AccountsManager accountsManager) {
    this.accountsManager = accountsManager;
  }

  @Override
  public void handleMessageReported(final String sourceNumber, final UUID messageGuid, final UUID reporterUuid,
      final Optional<byte[]> reportSpamToken) {

    final String sourceCountryCode = Util.getCountryCode(sourceNumber);

    Metrics.counter(REPORTED_COUNTER_NAME, COUNTRY_CODE_TAG_NAME, sourceCountryCode).increment();

    accountsManager.getByAccountIdentifier(reporterUuid).ifPresent(reporter -> {
      final String destinationCountryCode = Util.getCountryCode(reporter.getNumber());

      logger.info(Markers.appendEntries(Map.of(
              "sourceCountry", sourceCountryCode,
              "destinationCountry", destinationCountryCode)),
          "Message reported");

      Metrics.counter(REPORTER_COUNTER_NAME, COUNTRY_CODE_TAG_NAME, destinationCountryCode).increment();
    });
  }
}
