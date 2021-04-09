/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.util;

import static com.google.common.base.Objects.equal;

public class Pair<T1, T2> {
  private final T1 v1;
  private final T2 v2;

  public Pair(T1 v1, T2 v2) {
    this.v1 = v1;
    this.v2 = v2;
  }

  public T1 first(){
    return v1;
  }

  public T2 second(){
    return v2;
  }

  public boolean equals(Object o) {
    return o instanceof Pair &&
        equal(((Pair) o).first(), first()) &&
        equal(((Pair) o).second(), second());
  }

  public int hashCode() {
    return first().hashCode() ^ second().hashCode();
  }
}
