/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration.secrets;

import org.apache.commons.lang3.Validate;

public class SecretString extends Secret<String> {
  public SecretString(final String value) {
    super(Validate.notBlank(value, "SecretString value must not be blank"));
  }
}
