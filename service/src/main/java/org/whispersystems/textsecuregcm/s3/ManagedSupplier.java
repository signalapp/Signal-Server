/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.s3;

import io.dropwizard.lifecycle.Managed;
import java.util.function.Supplier;

public interface ManagedSupplier<T> extends Supplier<T>, Managed {

  @Override
  default void start() throws Exception {
    // noop
  }

  @Override
  default void stop() throws Exception {
    // noop
  }
}
