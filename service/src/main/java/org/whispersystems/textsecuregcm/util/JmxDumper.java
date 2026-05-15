package org.whispersystems.textsecuregcm.util;

import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.management.MBeanServer;
import java.lang.management.ManagementFactory;
import java.util.concurrent.ScheduledExecutorService;

/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
public class JmxDumper implements Managed {
  private static final Logger log = LoggerFactory.getLogger(JmxDumper.class);


  private final ScheduledExecutorService executor;

  public JmxDumper(final ScheduledExecutorService executor) {
    this.executor = executor;
  }

  @Override
  public void start() throws Exception {
//    executor.schedule()
  }

  private void dump() {
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

  }
}
