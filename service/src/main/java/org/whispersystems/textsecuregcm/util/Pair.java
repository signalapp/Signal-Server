/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.util;

import java.util.Map;

public record Pair<T1, T2>(T1 first, T2 second) {
  public Pair(org.signal.libsignal.protocol.util.Pair<T1, T2> p) {
    this(p.first(), p.second());
  }

  public Pair(Map.Entry<T1, T2> e) {
    this(e.getKey(), e.getValue());
  }
}
