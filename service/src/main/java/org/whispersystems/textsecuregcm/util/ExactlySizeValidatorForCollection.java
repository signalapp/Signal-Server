/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import java.util.Collection;

public class ExactlySizeValidatorForCollection extends ExactlySizeValidator<Collection<?>> {

  @Override
  protected int size(final Collection<?> value) {
    return value == null ? 0 : value.size();
  }
}
