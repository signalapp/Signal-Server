/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import java.util.List;

public record TurnToken(String username, String password, List<String> urls, List<String> urlsWithIps, String hostname) {
  public TurnToken(String username, String password, List<String> urls) {
    this(username, password, urls, null, null);
  }
}
