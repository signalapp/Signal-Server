/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.calls.routing;

import javax.validation.constraints.NotNull;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;

public record CallDnsRecords(
  @NotNull
  Map<String, List<InetAddress>> aByRegion,
  @NotNull
  Map<String, List<InetAddress>> aaaaByRegion
) {
  public String getSummary() {
    int numARecords = aByRegion.values().stream().mapToInt(List::size).sum();
    int numAAAARecords = aaaaByRegion.values().stream().mapToInt(List::size).sum();
    return String.format(
        "(A records, %s regions, %s records), (AAAA records, %s regions, %s records)",
        aByRegion.size(),
        numARecords,
        aaaaByRegion.size(),
        numAAAARecords
    );
  }

  public static CallDnsRecords empty() {
    return new CallDnsRecords(Map.of(), Map.of());
  }
}
