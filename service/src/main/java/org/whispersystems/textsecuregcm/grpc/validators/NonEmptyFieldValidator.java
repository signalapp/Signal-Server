/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc.validators;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;

public class NonEmptyFieldValidator extends BaseFieldValidator<Boolean> {

  public NonEmptyFieldValidator() {
    super("nonEmpty", Set.of(
        Descriptors.FieldDescriptor.Type.STRING,
        Descriptors.FieldDescriptor.Type.BYTES
    ), MissingOptionalAction.FAIL, true);
  }

  @Override
  protected Boolean resolveExtensionValue(final Object extensionValue) throws FieldValidationException {
    return requireFlagExtension(extensionValue);
  }

  @Override
  protected void validateBytesValue(
      final Boolean extensionValue,
      final ByteString fieldValue) throws FieldValidationException {
    if (!fieldValue.isEmpty()) {
      return;
    }
    throw new FieldValidationException("byte array expected to be non-empty");
  }

  @Override
  protected void validateStringValue(
      final Boolean extensionValue,
      final String fieldValue) throws FieldValidationException {
    if (StringUtils.isNotEmpty(fieldValue)) {
      return;
    }
    throw new FieldValidationException("string expected to be non-empty");
  }

  @Override
  protected void validateRepeatedField(
      final Boolean extensionValue,
      final Descriptors.FieldDescriptor fd,
      final Message msg) throws FieldValidationException {
    if (msg.getRepeatedFieldCount(fd) > 0) {
      return;
    }
    throw new FieldValidationException("repeated field is expected to be non-empty");
  }
}
