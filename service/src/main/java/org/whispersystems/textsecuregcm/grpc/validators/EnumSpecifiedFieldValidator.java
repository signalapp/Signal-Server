/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc.validators;

import static org.whispersystems.textsecuregcm.grpc.validators.ValidatorUtils.invalidArgument;

import com.google.protobuf.Descriptors;
import io.grpc.StatusException;
import java.util.Set;

public class EnumSpecifiedFieldValidator extends BaseFieldValidator<Boolean> {

  public EnumSpecifiedFieldValidator() {
    super("specified", Set.of(Descriptors.FieldDescriptor.Type.ENUM), MissingOptionalAction.FAIL, false);
  }

  @Override
  protected Boolean resolveExtensionValue(final Object extensionValue) throws StatusException {
    return requireFlagExtension(extensionValue);
  }

  @Override
  protected void validateEnumValue(
      final Boolean extensionValue,
      final Descriptors.EnumValueDescriptor enumValueDescriptor) throws StatusException {
    if (enumValueDescriptor.getIndex() <= 0) {
      throw invalidArgument("enum field must be specified");
    }
  }
}
