/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc.validators;

import com.google.protobuf.Descriptors;
import org.whispersystems.textsecuregcm.util.ImpossiblePhoneNumberException;
import org.whispersystems.textsecuregcm.util.NonNormalizedPhoneNumberException;
import org.whispersystems.textsecuregcm.util.Util;

import java.util.Base64;
import java.util.Objects;
import java.util.Set;

/// Validate that a string field is a valid base64 url string (padded or unpadded)
public class Base64UrlFieldValidator extends BaseFieldValidator<Boolean> {

  public Base64UrlFieldValidator() {
    super("base64url", Set.of(Descriptors.FieldDescriptor.Type.STRING), MissingOptionalAction.SUCCEED, false);
  }

  @Override
  protected Boolean resolveExtensionValue(final Object extensionValue) throws FieldValidationException {
    return requireFlagExtension(extensionValue);
  }

  @Override
  protected void validateStringValue(
      final Boolean extensionValue,
      final String fieldValue) throws FieldValidationException {
    try {
      Base64.getUrlDecoder().decode(fieldValue);
    } catch (IllegalArgumentException e) {
      throw new FieldValidationException("value is not valid base64 url");
    }
  }
}
