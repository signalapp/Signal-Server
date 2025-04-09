/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc.validators;

import static org.whispersystems.textsecuregcm.grpc.validators.ValidatorUtils.invalidArgument;

import com.google.protobuf.Descriptors;
import io.grpc.StatusException;
import java.util.Set;
import org.signal.chat.require.ValueRangeConstraint;

public class RangeFieldValidator extends BaseFieldValidator<Range> {

  private static final Set<Descriptors.FieldDescriptor.Type> UNSIGNED_TYPES = Set.of(
      Descriptors.FieldDescriptor.Type.FIXED32,
      Descriptors.FieldDescriptor.Type.UINT32,
      Descriptors.FieldDescriptor.Type.FIXED64,
      Descriptors.FieldDescriptor.Type.UINT64
  );

  public RangeFieldValidator() {
    super("range", Set.of(
        Descriptors.FieldDescriptor.Type.INT64,
        Descriptors.FieldDescriptor.Type.UINT64,
        Descriptors.FieldDescriptor.Type.INT32,
        Descriptors.FieldDescriptor.Type.FIXED64,
        Descriptors.FieldDescriptor.Type.FIXED32,
        Descriptors.FieldDescriptor.Type.UINT32,
        Descriptors.FieldDescriptor.Type.SFIXED32,
        Descriptors.FieldDescriptor.Type.SFIXED64,
        Descriptors.FieldDescriptor.Type.SINT32,
        Descriptors.FieldDescriptor.Type.SINT64
    ), MissingOptionalAction.SUCCEED, false);
  }

  @Override
  protected Range resolveExtensionValue(final Object extensionValue) throws StatusException {
    final ValueRangeConstraint rangeConstraint = (ValueRangeConstraint) extensionValue;
    final long min = rangeConstraint.hasMin() ? rangeConstraint.getMin() : Long.MIN_VALUE;
    final long max = rangeConstraint.hasMax() ? rangeConstraint.getMax() : Long.MAX_VALUE;
    return new Range(min, max);
  }

  @Override
  protected void validateIntegerNumber(
      final Range range,
      final long fieldValue,
      final Descriptors.FieldDescriptor.Type type) throws StatusException {
    if (fieldValue < 0 && UNSIGNED_TYPES.contains(type)) {
      throw invalidArgument("field value is expected to be within the [%d, %d] range".formatted(
          range.min(), range.max()));
    }
    if (fieldValue < range.min() || fieldValue > range.max()) {
      throw invalidArgument("field value is [%d] but expected to be within the [%d, %d] range".formatted(
          fieldValue, range.min(), range.max()));
    }
  }
}
