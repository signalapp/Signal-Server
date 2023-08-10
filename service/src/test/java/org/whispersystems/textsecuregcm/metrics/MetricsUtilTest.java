/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import org.junit.jupiter.api.Test;


class MetricsUtilTest {

  @Test
  void name() {

    assertEquals("chat.MetricsUtilTest.metric", MetricsUtil.name(MetricsUtilTest.class, "metric"));
    assertEquals("chat.MetricsUtilTest.namespace.metric",
        MetricsUtil.name(MetricsUtilTest.class, "namespace", "metric"));
  }

  @Test
  void lettuceRejection() {
    MeterRegistry registry = new SimpleMeterRegistry();
    MetricsUtil.configureMeterFilters(registry.config());

    registry.counter("lettuce.command.completion.count").increment();
    registry.counter("lettuce.command.firstresponse.max").increment();
    registry.counter("lettuce.test").increment();
    assertThat(registry.getMeters()).isEmpty();

    // this lettuce statistic is allow-listed
    registry.counter("lettuce.command.completion.max", "command", "hello", "remote", "world").increment();
    final List<Meter> meters = registry.getMeters();
    assertThat(meters).hasSize(1);

    Meter meter = meters.get(0);
    assertThat(meter.getId().getName()).isEqualTo("chat.lettuce.command.completion.max");
    assertThat(meter.getId().getTag("command")).isNull();
    assertThat(meter.getId().getTag("remote")).isNotNull();
  }
}
