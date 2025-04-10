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
import org.signal.chat.require.SizeConstraint;

public class SizeFieldValidator extends BaseFieldValidator<Range> {

  public SizeFieldValidator() {
    super("size", Set.of(
        Descriptors.FieldDescriptor.Type.STRING,
        Descriptors.FieldDescriptor.Type.BYTES
    ), MissingOptionalAction.VALIDATE_DEFAULT_VALUE, true);
  }

  @Override
  protected Range resolveExtensionValue(final Object extensionValue) throws StatusException {
    final SizeConstraint sizeConstraint = (SizeConstraint) extensionValue;
    final int min = sizeConstraint.hasMin() ? sizeConstraint.getMin() : 0;
    final int max = sizeConstraint.hasMax() ? sizeConstraint.getMax() : Integer.MAX_VALUE;
    return new Range(min, max);
  }

  @Override
  protected void validateBytesValue(final Range range, final ByteString fieldValue) throws StatusException {
    if (fieldValue.size() < range.min() || fieldValue.size() > range.max()) {
      throw invalidArgument("field value is [%d] but expected to be within the [%d, %d] range".formatted(
          fieldValue.size(), range.min(), range.max()));
    }
  }

  @Override
  protected void validateStringValue(final Range range, final String fieldValue) throws StatusException {
    if (fieldValue.length() < range.min() || fieldValue.length() > range.max()) {
      throw invalidArgument("field value is [%d] but expected to be within the [%d, %d] range".formatted(
          fieldValue.length(), range.min(), range.max()));
    }
  }

  @Override
  protected void validateRepeatedField(final Range range, final Descriptors.FieldDescriptor fd, final Message msg) throws StatusException {
    final int size = msg.getRepeatedFieldCount(fd);
    if (size < range.min() || size > range.max()) {
      throw invalidArgument("field value is [%d] but expected to be within the [%d, %d] range".formatted(
          size, range.min(), range.max()));
    }
  }
}
