/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc.validators;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import java.util.Set;

public class PresentFieldValidator extends BaseFieldValidator<Boolean> {

  public PresentFieldValidator() {
    super("present",
        Set.of(Descriptors.FieldDescriptor.Type.MESSAGE),
        MissingOptionalAction.FAIL,
        true);
  }

  @Override
  protected Boolean resolveExtensionValue(final Object extensionValue) throws FieldValidationException {
    return requireFlagExtension(extensionValue);
  }

  @Override
  protected void validateMessageValue(final Boolean extensionValue, final Message msg) throws FieldValidationException {
    if (msg == null) {
      throw new FieldValidationException("message expected to be present");
    }
  }
}
