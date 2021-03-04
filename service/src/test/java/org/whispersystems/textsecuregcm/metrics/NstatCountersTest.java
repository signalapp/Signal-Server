/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.metrics.NstatCounters.NetworkStatistics;

import java.io.IOException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class NstatCountersTest {

  @Test
  @EnabledOnOs(OS.LINUX)
  void loadNetworkStatistics() throws IOException, InterruptedException {
    final NetworkStatistics networkStatistics = NstatCounters.loadNetworkStatistics();

    assertNotNull(networkStatistics.getKernelStatistics());
    assertFalse(networkStatistics.getKernelStatistics().isEmpty());
  }

  @ParameterizedTest
  @MethodSource("shouldIncludeMetricNameProvider")
  void shouldIncludeMetric(final String metricName, final boolean expectInclude) {
    assertEquals(expectInclude, NstatCounters.shouldIncludeMetric(metricName));
  }

  static Stream<Arguments> shouldIncludeMetricNameProvider() {
    return Stream.of(Arguments.of("IpInReceives", true),
        Arguments.of("TcpActiveOpens", true),
        Arguments.of("UdpInDatagrams", false),
        Arguments.of("Ip6InReceives", false),
        Arguments.of("Udp6InDatagrams", false),
        Arguments.of("TcpExtSyncookiesSent", true),
        Arguments.of("IpExtInNoRoutes", true));
  }
}
