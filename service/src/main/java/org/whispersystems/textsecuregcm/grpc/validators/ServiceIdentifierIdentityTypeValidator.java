/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc.validators;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import java.util.Objects;
import java.util.Set;
import org.signal.chat.common.IdentityType;
import org.signal.chat.common.ServiceIdentifier;

public class ServiceIdentifierIdentityTypeValidator extends FieldValidator<IdentityType> {

  public ServiceIdentifierIdentityTypeValidator() {
    super("identityType", Set.of(Descriptors.FieldDescriptor.Type.MESSAGE), MissingOptionalAction.SUCCEED, false);
  }

  @Override
  protected IdentityType resolveExtensionValue(final Object extensionValue) throws FieldValidationException {

    if (extensionValue instanceof Descriptors.EnumValueDescriptor d) {
      if (d.getType().equals(org.signal.chat.require.IdentityType.getDescriptor())) {
        return switch (Objects.requireNonNull(org.signal.chat.require.IdentityType.forNumber(d.getNumber()))) {
          case IDENTITY_TYPE_ACI -> IdentityType.IDENTITY_TYPE_ACI;
          case IDENTITY_TYPE_PNI -> IdentityType.IDENTITY_TYPE_PNI;
          default -> throw new IllegalArgumentException("unsupported value: " + d.getName());
        };
      }
    }

    throw new IllegalArgumentException("value must be org.signal.chat.require.IdentityType");
  }

  @Override
  protected void validateMessageValue(final IdentityType extensionValue, final Message msg) throws FieldValidationException {
    if (msg == null) {
      return;
    }

    if (msg instanceof ServiceIdentifier i) {
      if (i.getIdentityType() == extensionValue) {
        return;
      }
      throw new FieldValidationException("identity type must be " + extensionValue.getValueDescriptor().getName());
    }

    throw new IllegalArgumentException("field must be " + ServiceIdentifier.class.getName());
  }
}
