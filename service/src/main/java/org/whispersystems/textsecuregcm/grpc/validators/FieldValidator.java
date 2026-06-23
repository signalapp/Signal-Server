/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc.validators;

import static java.util.Objects.requireNonNull;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import java.util.List;
import java.util.Set;

public abstract class FieldValidator<T> {

  private final String extensionName;

  private final Set<Descriptors.FieldDescriptor.Type> supportedTypes;

  private final MissingOptionalAction missingOptionalAction;

  private final boolean applicableToRepeated;

  public enum MissingOptionalAction {
    FAIL,
    SUCCEED,
    VALIDATE_DEFAULT_VALUE
  }


  protected FieldValidator(
      final String extensionName,
      final Set<Descriptors.FieldDescriptor.Type> supportedTypes,
      final MissingOptionalAction missingOptionalAction,
      final boolean applicableToRepeated) {
    this.extensionName = requireNonNull(extensionName);
    this.supportedTypes = requireNonNull(supportedTypes);
    this.missingOptionalAction = missingOptionalAction;
    this.applicableToRepeated = applicableToRepeated;
  }

  public MissingOptionalAction getMissingOptionalAction() {
    return missingOptionalAction;
  }

  public String getExtensionName() {
    return extensionName;
  }


  /// Validate a field
  ///
  /// @param extensionValue The value of the option this field was annotated with
  /// @param fd The descriptor of the field to validate
  /// @param fieldValue The value of the field to validate
  /// @throws FieldValidationException if a field constraint was violated
  public final void validate(
      final Object extensionValue,
      final Descriptors.FieldDescriptor fd,
      final Object fieldValue) throws FieldValidationException {
    final T extensionValueTyped = resolveExtensionValue(extensionValue);

    // for the `repeated` fields, checking if it's supported by the extension
    if (fd.isRepeated() && fieldValue instanceof List list) {
      if (!applicableToRepeated) {
        throw new IllegalArgumentException("can't apply extension %s to `repeated` field %s"
            .formatted(extensionName, fd.getFullName()));
      }
      validateRepeatedField(extensionValueTyped, fd, list);
      return;
    }
    final Descriptors.FieldDescriptor.Type type = fd.getType();
    if (!supportedTypes.contains(type)) {
      throw new IllegalArgumentException("can't apply extension %s to field %s of type %s".formatted(
          extensionName, fd.getFullName(), type));
    }
    switch (type) {
      case INT64, UINT64, INT32, FIXED64, FIXED32, UINT32, SFIXED32, SFIXED64, SINT32, SINT64 ->
          validateIntegerNumber(extensionValueTyped, ((Number) fieldValue).longValue(), type);
      case STRING -> validateStringValue(extensionValueTyped, (String) fieldValue);
      case BYTES -> validateBytesValue(extensionValueTyped, (ByteString) fieldValue);
      case ENUM -> validateEnumValue(extensionValueTyped, (Descriptors.EnumValueDescriptor) fieldValue);
      case MESSAGE -> validateMessageValue(extensionValueTyped, (Message) fieldValue);
      case FLOAT, DOUBLE, BOOL, GROUP -> {
        // at this moment, there are no validations specific to these types of fields
      }
    }

  }

  protected abstract T resolveExtensionValue(final Object extensionValue) throws FieldValidationException;

  protected void validateRepeatedField(
      final T extensionValue,
      final Descriptors.FieldDescriptor fd,
      final List<?> repeated) throws FieldValidationException {
    throw new UnsupportedOperationException("`validateRepeatedField` method needs to be implemented");
  }

  protected void validateIntegerNumber(
      final T extensionValue,
      final long fieldValue, final Descriptors.FieldDescriptor.Type type) throws FieldValidationException {
    throw new UnsupportedOperationException("`validateIntegerNumber` method needs to be implemented");
  }

  protected void validateStringValue(
      final T extensionValue,
      final String fieldValue) throws FieldValidationException {
    throw new UnsupportedOperationException("`validateStringValue` method needs to be implemented");
  }

  protected void validateBytesValue(
      final T extensionValue,
      final ByteString fieldValue) throws FieldValidationException {
    throw new UnsupportedOperationException("`validateBytesValue` method needs to be implemented");
  }

  protected void validateEnumValue(
      final T extensionValue,
      final Descriptors.EnumValueDescriptor enumValueDescriptor) throws FieldValidationException {
    throw new UnsupportedOperationException("`validateEnumValue` method needs to be implemented");
  }

  protected void validateMessageValue(
      final T extensionValue,
      final Message message) throws FieldValidationException {
    throw new UnsupportedOperationException("`validateMessageValue` method needs to be implemented");
  }

  protected static boolean requireFlagExtension(final Object extensionValue) throws FieldValidationException {
    if (extensionValue instanceof Boolean flagIsOn && flagIsOn) {
      return true;
    }
    throw new UnsupportedOperationException("only value `true` is allowed");
  }
}
