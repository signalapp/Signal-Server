/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc.validators;

import static org.whispersystems.textsecuregcm.grpc.validators.ValidatorUtils.invalidArgument;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.grpc.StatusException;
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
  protected Boolean resolveExtensionValue(final Object extensionValue) throws StatusException {
    return requireFlagExtension(extensionValue);
  }

  @Override
  protected void validateBytesValue(
      final Boolean extensionValue,
      final ByteString fieldValue) throws StatusException {
    if (!fieldValue.isEmpty()) {
      return;
    }
    throw invalidArgument("byte array expected to be non-empty");
  }

  @Override
  protected void validateStringValue(
      final Boolean extensionValue,
      final String fieldValue) throws StatusException {
    if (StringUtils.isNotEmpty(fieldValue)) {
      return;
    }
    throw invalidArgument("string expected to be non-empty");
  }

  @Override
  protected void validateRepeatedField(
      final Boolean extensionValue,
      final Descriptors.FieldDescriptor fd,
      final Message msg) throws StatusException {
    if (msg.getRepeatedFieldCount(fd) > 0) {
      return;
    }
    throw invalidArgument("repeated field is expected to be non-empty");
  }
}
