/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.util;

import io.dropwizard.lifecycle.Managed;
import software.amazon.awssdk.crt.CRT;

/**
 * The AWS CRT client registers its own JVM shutdown handler which makes sure asynchronous operations in the CRT don't
 * call back into the JVM after the JVM shuts down. Unfortunately, this hook kills all outstanding operations using the
 * CRT, even though we typically orchestrate a graceful shutdown where we give outstanding requests time to complete.
 *
 * The CRT lets you take over the shutdown sequencing by incrementing/decrementing a ref count, so we introduce a
 * lifecycle object that will be shutdown after our graceful shutdown process.
 */
public class ManagedAwsCrt implements Managed {

  @Override
  public void start() {
    CRT.acquireShutdownRef();
  }

  @Override
  public void stop() {
    CRT.releaseShutdownRef();
  }
}
