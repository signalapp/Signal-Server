/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.backup;

import java.io.IOException;

public class SourceObjectNotFoundException extends IOException {
  public SourceObjectNotFoundException() {
    super();
  }
  public SourceObjectNotFoundException(String message) {
    super(message);
  }
}
