/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc.validators;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import java.util.List;
import java.util.Set;

public class ExactlySizeFieldValidator extends BaseFieldValidator<Set<Integer>> {

  public ExactlySizeFieldValidator() {
    super("exactlySize", Set.of(
        Descriptors.FieldDescriptor.Type.STRING,
        Descriptors.FieldDescriptor.Type.BYTES
    ), MissingOptionalAction.VALIDATE_DEFAULT_VALUE, true);
  }

  @Override
  protected Set<Integer> resolveExtensionValue(final Object extensionValue) {
    //noinspection unchecked
    return Set.copyOf((List<Integer>) extensionValue);
  }

  @Override
  protected void validateBytesValue(
      final Set<Integer> permittedSizes,
      final ByteString fieldValue) throws FieldValidationException {
    if (permittedSizes.contains(fieldValue.size())) {
      return;
    }
    throw new FieldValidationException("byte array length is [%d] but expected to be one of %s".formatted(fieldValue.size(), permittedSizes));
  }

  @Override
  protected void validateStringValue(
      final Set<Integer> permittedSizes,
      final String fieldValue) throws FieldValidationException {
    if (permittedSizes.contains(fieldValue.length())) {
      return;
    }
    throw new FieldValidationException("string length is [%d] but expected to be one of %s".formatted(fieldValue.length(), permittedSizes));
  }

  @Override
  protected void validateRepeatedField(
      final Set<Integer> permittedSizes,
      final Descriptors.FieldDescriptor fd,
      final Message msg) throws FieldValidationException {
    final int size = msg.getRepeatedFieldCount(fd);
    if (permittedSizes.contains(size)) {
      return;
    }
    throw new FieldValidationException("list size is [%d] but expected to be one of %s".formatted(size, permittedSizes));
  }
}
