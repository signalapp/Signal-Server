/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;

public class GarbageCollectionGauges {

  private GarbageCollectionGauges() {
  }

  public static void registerMetrics() {
    for (final GarbageCollectorMXBean garbageCollectorMXBean : ManagementFactory.getGarbageCollectorMXBeans()) {
      final List<Tag> tags = List.of(Tag.of("memoryManagerName", garbageCollectorMXBean.getName()));

      Metrics.gauge(name(GarbageCollectionGauges.class, "collectionCount"), tags, garbageCollectorMXBean,
          GarbageCollectorMXBean::getCollectionCount);
      Metrics.gauge(name(GarbageCollectionGauges.class, "collectionTime"), tags, garbageCollectorMXBean,
          GarbageCollectorMXBean::getCollectionTime);
    }
  }
}
