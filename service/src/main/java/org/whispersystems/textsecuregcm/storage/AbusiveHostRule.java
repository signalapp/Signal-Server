package org.whispersystems.textsecuregcm.storage;

import java.net.InetAddress;
import java.util.List;

public class AbusiveHostRule {

  private final String       host;
  private final boolean      blocked;
  private final List<String> regions;

  public AbusiveHostRule(String host, boolean blocked, List<String> regions) {
    this.host    = host;
    this.blocked = blocked;
    this.regions = regions;
  }

  public List<String> getRegions() {
    return regions;
  }

  public boolean isBlocked() {
    return blocked;
  }

  public String getHost() {
    return host;
  }
}
