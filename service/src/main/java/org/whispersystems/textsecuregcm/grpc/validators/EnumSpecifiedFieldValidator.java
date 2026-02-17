/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc.validators;

import com.google.protobuf.Descriptors;
import java.util.Set;

public class EnumSpecifiedFieldValidator extends BaseFieldValidator<Boolean> {

  public EnumSpecifiedFieldValidator() {
    super("specified", Set.of(Descriptors.FieldDescriptor.Type.ENUM), MissingOptionalAction.FAIL, false);
  }

  @Override
  protected Boolean resolveExtensionValue(final Object extensionValue) throws FieldValidationException {
    return requireFlagExtension(extensionValue);
  }

  @Override
  protected void validateEnumValue(
      final Boolean extensionValue,
      final Descriptors.EnumValueDescriptor enumValueDescriptor) throws FieldValidationException {
    if (enumValueDescriptor.getIndex() <= 0) {
      throw new FieldValidationException("enum field must be specified");
    }
  }
}
