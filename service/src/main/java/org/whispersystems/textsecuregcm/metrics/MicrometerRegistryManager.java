/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import io.dropwizard.lifecycle.Managed;
import io.micrometer.core.instrument.MeterRegistry;

public class MicrometerRegistryManager implements Managed {

  private final MeterRegistry meterRegistry;

  public MicrometerRegistryManager(final MeterRegistry meterRegistry) {
    this.meterRegistry = meterRegistry;
  }

  @Override
  public void start() throws Exception {

  }

  @Override
  public void stop() throws Exception {
    // closing the registry publishes one final set of metrics
    meterRegistry.close();
  }
}
