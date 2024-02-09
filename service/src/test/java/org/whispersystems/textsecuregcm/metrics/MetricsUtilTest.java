/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import org.assertj.core.api.AbstractStringAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicMetricsConfiguration;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;


class MetricsUtilTest {

  @Test
  void name() {

    assertEquals("chat.MetricsUtilTest.metric", MetricsUtil.name(MetricsUtilTest.class, "metric"));
    assertEquals("chat.MetricsUtilTest.namespace.metric",
        MetricsUtil.name(MetricsUtilTest.class, "namespace", "metric"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void lettuceTagRejection(final boolean enableLettuceRemoteTag) {
    DynamicConfiguration dynamicConfiguration = mock(DynamicConfiguration.class);
    DynamicMetricsConfiguration metricsConfiguration = new DynamicMetricsConfiguration(enableLettuceRemoteTag);
    when(dynamicConfiguration.getMetricsConfiguration()).thenReturn(metricsConfiguration);
    DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager =
        mock(DynamicConfigurationManager.class);
    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);

    MeterRegistry registry = new SimpleMeterRegistry();
    MetricsUtil.configureMeterFilters(registry.config(), dynamicConfigurationManager);

    registry.counter("lettuce.command.completion.max", "command", "hello", "remote", "world", "allowed", "!").increment();
    final List<Meter> meters = registry.getMeters();
    assertThat(meters).hasSize(1);

    Meter meter = meters.get(0);
    assertThat(meter.getId().getName()).isEqualTo("chat.lettuce.command.completion.max");
    assertThat(meter.getId().getTag("command")).isNull();
    AbstractStringAssert<?> remoteTag = assertThat(meter.getId().getTag("remote"));

    if (enableLettuceRemoteTag) {
      remoteTag.isNotNull();
    } else {
      remoteTag.isNull();
    }
    assertThat(meter.getId().getTag("allowed")).isNotNull();
  }
}
