/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration.secrets;

import com.google.common.collect.ImmutableList;
import jakarta.validation.constraints.NotEmpty;
import java.util.Collection;
import java.util.List;
import org.hibernate.validator.internal.constraintvalidators.bv.notempty.NotEmptyValidatorForCollection;

public class SecretBytesList extends Secret<List<byte[]>> {

  @SuppressWarnings("rawtypes")
  public static class ValidatorNotEmpty extends BaseSecretValidator<NotEmpty, Collection, SecretBytesList> {
    public ValidatorNotEmpty() {
      super(new NotEmptyValidatorForCollection());
    }
  }

  public SecretBytesList(final List<byte[]> value) {
    super(ImmutableList.copyOf(value));
  }
}
