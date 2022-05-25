/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import java.util.List;
import java.util.Optional;

public record AbusiveHostRule(String host, boolean blocked, List<String> regions) {

  public Optional<Integer> cidrPrefix() {
    String[] split = host.split("/");
    if (split.length != 2) {
      return Optional.empty();
    }
    try {
      return Optional.of(Integer.parseInt(split[1]));
    } catch (NumberFormatException e) {
      return Optional.empty();
    }
  }

}
