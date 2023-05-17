/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration.secrets;

public class Secret<T> {

  private final T value;


  public Secret(final T value) {
    this.value = value;
  }

  public T value() {
    return value;
  }

  @Override
  public String toString() {
    return "[REDACTED]";
  }
}
