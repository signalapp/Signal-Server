/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc.validators;

import static java.util.Objects.requireNonNull;
import static org.whispersystems.textsecuregcm.grpc.validators.ValidatorUtils.internalError;
import static org.whispersystems.textsecuregcm.grpc.validators.ValidatorUtils.invalidArgument;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.grpc.Status;
import io.grpc.StatusException;
import java.util.Set;

public abstract class BaseFieldValidator<T> implements FieldValidator {

  private final String extensionName;

  private final Set<Descriptors.FieldDescriptor.Type> supportedTypes;

  private final MissingOptionalAction missingOptionalAction;

  private final boolean applicableToRepeated;

  protected enum MissingOptionalAction {
    FAIL,
    SUCCEED,
    VALIDATE_DEFAULT_VALUE
  }


  protected BaseFieldValidator(
      final String extensionName,
      final Set<Descriptors.FieldDescriptor.Type> supportedTypes,
      final MissingOptionalAction missingOptionalAction,
      final boolean applicableToRepeated) {
    this.extensionName = requireNonNull(extensionName);
    this.supportedTypes = requireNonNull(supportedTypes);
    this.missingOptionalAction = missingOptionalAction;
    this.applicableToRepeated = applicableToRepeated;
  }

  @Override
  public void validate(
      final Object extensionValue,
      final Descriptors.FieldDescriptor fd,
      final Message msg) throws StatusException {
    try {
      final T extensionValueTyped = resolveExtensionValue(extensionValue);

      // for the fields with an `optional` modifier, checking if the field was set
      // and if not, checking if extension allows missing optional field
      if (fd.hasPresence() && !msg.hasField(fd)) {
        switch (missingOptionalAction) {
          case FAIL -> {
            throw invalidArgument("extension requires a value to be set");
          }
          case SUCCEED -> {
            return;
          }
          case VALIDATE_DEFAULT_VALUE -> {
            // just continuing
          }
        }
      }

      // for the `repeated` fields, checking if it's supported by the extension
      if (fd.isRepeated()) {
        if (applicableToRepeated) {
          validateRepeatedField(extensionValueTyped, fd, msg);
          return;
        }
        throw internalError("can't apply extension to a `repeated` field");
      }

      // checking field type against the set of supported types
      final Descriptors.FieldDescriptor.Type type = fd.getType();
      if (!supportedTypes.contains(type)) {
        throw internalError("can't apply extension to a field of type [%s]".formatted(type));
      }
      switch (type) {
        case INT64, UINT64, INT32, FIXED64, FIXED32, UINT32, SFIXED32, SFIXED64, SINT32, SINT64 ->
            validateIntegerNumber(extensionValueTyped, ((Number) msg.getField(fd)).longValue(), type);
        case STRING ->
            validateStringValue(extensionValueTyped, (String) msg.getField(fd));
        case BYTES ->
            validateBytesValue(extensionValueTyped, (ByteString) msg.getField(fd));
        case ENUM ->
            validateEnumValue(extensionValueTyped, (Descriptors.EnumValueDescriptor) msg.getField(fd));
        case MESSAGE -> {
          validateMessageValue(extensionValueTyped, (Message) msg.getField(fd));
        }
        case FLOAT, DOUBLE, BOOL, GROUP -> {
          // at this moment, there are no validations specific to these types of fields
        }
      }
    } catch (StatusException e) {
      throw new StatusException(e.getStatus().withDescription(
          "field [%s], extension [%s]: %s".formatted(fd.getName(), extensionName, e.getStatus().getDescription())
      ), e.getTrailers());
    } catch (RuntimeException e) {
      throw Status.INTERNAL
          .withDescription("field [%s], extension [%s]: %s".formatted(fd.getName(), extensionName, e.getMessage()))
          .withCause(e)
          .asException();
    }
  }

  protected abstract T resolveExtensionValue(final Object extensionValue) throws StatusException;

  protected void validateRepeatedField(
      final T extensionValue,
      final Descriptors.FieldDescriptor fd,
      final Message msg) throws StatusException {
    throw internalError("`validateRepeatedField` method needs to be implemented");
  }

  protected void validateIntegerNumber(
      final T extensionValue,
      final long fieldValue, final Descriptors.FieldDescriptor.Type type) throws StatusException {
    throw internalError("`validateIntegerNumber` method needs to be implemented");
  }

  protected void validateStringValue(
      final T extensionValue,
      final String fieldValue) throws StatusException {
    throw internalError("`validateStringValue` method needs to be implemented");
  }

  protected void validateBytesValue(
      final T extensionValue,
      final ByteString fieldValue) throws StatusException {
    throw internalError("`validateBytesValue` method needs to be implemented");
  }

  protected void validateEnumValue(
      final T extensionValue,
      final Descriptors.EnumValueDescriptor enumValueDescriptor) throws StatusException {
    throw internalError("`validateEnumValue` method needs to be implemented");
  }

  protected void validateMessageValue(
      final T extensionValue,
      final Message message) throws StatusException {
    throw internalError("`validateMessageValue` method needs to be implemented");
  }

  protected static boolean requireFlagExtension(final Object extensionValue) throws StatusException {
    if (extensionValue instanceof Boolean flagIsOn && flagIsOn) {
      return true;
    }
    throw internalError("only value `true` is allowed");
  }
}
