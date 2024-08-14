/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import org.whispersystems.textsecuregcm.util.SystemMapper;
import java.io.IOException;

public class AccountUtil {

  static Account cloneAccountAsNotStale(final Account account) {
    try {
      return SystemMapper.jsonMapper().readValue(
          SystemMapper.jsonMapper().writeValueAsBytes(account), Account.class);
    } catch (final IOException e) {
      // this should really, truly, never happen
      throw new IllegalArgumentException(e);
    }
  }
}
