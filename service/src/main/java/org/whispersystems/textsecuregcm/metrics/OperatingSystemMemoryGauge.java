/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import com.codahale.metrics.Gauge;
import com.google.common.annotations.VisibleForTesting;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class OperatingSystemMemoryGauge implements Gauge<Long> {

    private final String metricName;

    private static final File MEMINFO_FILE = new File("/proc/meminfo");
    private static final Pattern MEMORY_METRIC_PATTERN = Pattern.compile("^([^:]+):\\s+([0-9]+).*$");

    public OperatingSystemMemoryGauge(final String metricName) {
        this.metricName = metricName;
    }

    @Override
    public Long getValue() {
        try (final BufferedReader bufferedReader = new BufferedReader(new FileReader(MEMINFO_FILE))) {
            return getValue(bufferedReader.lines());
        } catch (final IOException e) {
            return 0L;
        }
    }

    @VisibleForTesting
    long getValue(final Stream<String> lines) {
        return lines.map(MEMORY_METRIC_PATTERN::matcher)
                    .filter(Matcher::matches)
                    .filter(matcher -> this.metricName.equalsIgnoreCase(matcher.group(1)))
                    .map(matcher -> Long.parseLong(matcher.group(2), 10))
                    .findFirst()
                    .orElse(0L);
    }
}
