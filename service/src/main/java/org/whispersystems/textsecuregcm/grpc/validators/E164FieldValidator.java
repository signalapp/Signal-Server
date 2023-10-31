/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc.validators;

import static org.whispersystems.textsecuregcm.grpc.validators.ValidatorUtils.invalidArgument;

import com.google.protobuf.Descriptors;
import io.grpc.StatusException;
import java.util.Set;
import org.whispersystems.textsecuregcm.util.ImpossiblePhoneNumberException;
import org.whispersystems.textsecuregcm.util.NonNormalizedPhoneNumberException;
import org.whispersystems.textsecuregcm.util.Util;

public class E164FieldValidator extends BaseFieldValidator<Boolean> {

  public E164FieldValidator() {
    super("e164", Set.of(Descriptors.FieldDescriptor.Type.STRING), MissingOptionalAction.SUCCEED, false);
  }

  @Override
  protected Boolean resolveExtensionValue(final Object extensionValue) throws StatusException {
    return requireFlagExtension(extensionValue);
  }

  @Override
  protected void validateStringValue(
      final Boolean extensionValue,
      final String fieldValue) throws StatusException {
    try {
      Util.requireNormalizedNumber(fieldValue);
    } catch (final ImpossiblePhoneNumberException | NonNormalizedPhoneNumberException e) {
      throw invalidArgument("value is not in E164 format");
    }
  }
}
