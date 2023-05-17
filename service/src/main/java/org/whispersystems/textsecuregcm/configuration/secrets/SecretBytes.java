/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration.secrets;

import org.apache.commons.lang3.Validate;

public class SecretBytes extends Secret<byte[]> {

  public SecretBytes(final byte[] value) {
    super(requireNotEmpty(value));
  }

  private static byte[] requireNotEmpty(final byte[] value) {
    Validate.isTrue(value.length > 0, "SecretBytes value must not be empty");
    return value;
  }
}
