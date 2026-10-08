/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc.validators;

import com.google.protobuf.Descriptors;
import java.util.Set;
import org.signal.chat.require.AllowedValuesConstraint;

public class AllowedValuesFieldValidator extends FieldValidator<AllowedValuesConstraint> {

  public AllowedValuesFieldValidator() {
    super("allowedValues", Set.of(
        Descriptors.FieldDescriptor.Type.INT32,
        Descriptors.FieldDescriptor.Type.INT64,
        Descriptors.FieldDescriptor.Type.UINT32,
        Descriptors.FieldDescriptor.Type.UINT64,
        Descriptors.FieldDescriptor.Type.FIXED32,
        Descriptors.FieldDescriptor.Type.FIXED64,
        Descriptors.FieldDescriptor.Type.SFIXED32,
        Descriptors.FieldDescriptor.Type.SFIXED64,
        Descriptors.FieldDescriptor.Type.SINT32,
        Descriptors.FieldDescriptor.Type.SINT64,
        Descriptors.FieldDescriptor.Type.STRING,
        Descriptors.FieldDescriptor.Type.ENUM
    ), MissingOptionalAction.SUCCEED, false);
  }

  @Override
  protected AllowedValuesConstraint resolveExtensionValue(final Object extensionValue) {
    return (AllowedValuesConstraint) extensionValue;
  }

  @Override
  protected void validateIntegerNumber(
      final AllowedValuesConstraint constraint,
      final long fieldValue,
      final Descriptors.FieldDescriptor.Type type) throws FieldValidationException {
    if (constraint.getIntsCount() == 0) {
      throw new IllegalArgumentException("allowedValues on integer field must specify ints");
    }
    if (constraint.getIntsList().contains(fieldValue)) {
      return;
    }
    throw new FieldValidationException("field value is [%d] but expected to be one of %s"
        .formatted(fieldValue, constraint.getIntsList()));
  }

  @Override
  protected void validateStringValue(
      final AllowedValuesConstraint constraint,
      final String fieldValue) throws FieldValidationException {
    if (constraint.getStringsCount() == 0) {
      throw new IllegalArgumentException("allowedValues on string field must specify strings");
    }
    if (constraint.getStringsList().contains(fieldValue)) {
      return;
    }
    throw new FieldValidationException("field value is [%s] but expected to be one of %s"
        .formatted(fieldValue, constraint.getStringsList()));
  }

  @Override
  protected void validateEnumValue(
      final AllowedValuesConstraint constraint,
      final Descriptors.EnumValueDescriptor enumValueDescriptor) throws FieldValidationException {
    if (constraint.getEnumsCount() == 0) {
      throw new IllegalArgumentException("allowedValues on enum field must specify enums");
    }
    if (constraint.getEnumsList().contains(enumValueDescriptor.getName())) {
      return;
    }
    throw new FieldValidationException("field value is [%s] but expected to be one of %s"
        .formatted(enumValueDescriptor.getName(), constraint.getEnumsList()));
  }
}
