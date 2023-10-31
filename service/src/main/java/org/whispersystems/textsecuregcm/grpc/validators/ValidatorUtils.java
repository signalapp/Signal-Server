/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc.validators;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.protobuf.ProtoServiceDescriptorSupplier;
import java.util.Map;
import java.util.Optional;
import org.signal.chat.require.Auth;

public final class ValidatorUtils {

  public static final String REQUIRE_AUTH_EXTENSION_NAME = "org.signal.chat.require.auth";

  private ValidatorUtils() {
    // noop
  }

  public static StatusException invalidArgument(final String description) {
    return Status.INVALID_ARGUMENT.withDescription(description).asException();
  }

  public static StatusException internalError(final String description) {
    return Status.INTERNAL.withDescription(description).asException();
  }

  public static StatusException internalError(final Exception cause) {
    return Status.INTERNAL.withCause(cause).asException();
  }

  public static Optional<Auth> serviceAuthExtensionValue(final ServerServiceDefinition serviceDefinition) {
    return serviceExtensionValueByName(serviceDefinition, REQUIRE_AUTH_EXTENSION_NAME)
        .map(val -> Auth.valueOf((Descriptors.EnumValueDescriptor) val));
  }

  private static Optional<Object> serviceExtensionValueByName(
      final ServerServiceDefinition serviceDefinition,
      final String fullExtensionName) {
    final Object schemaDescriptor = serviceDefinition.getServiceDescriptor().getSchemaDescriptor();
    if (schemaDescriptor instanceof ProtoServiceDescriptorSupplier protoServiceDescriptorSupplier) {
      final DescriptorProtos.ServiceOptions options = protoServiceDescriptorSupplier.getServiceDescriptor().getOptions();
      return options.getAllFields().entrySet()
          .stream()
          .filter(e -> e.getKey().getFullName().equals(fullExtensionName))
          .map(Map.Entry::getValue)
          .findFirst();
    }
    return Optional.empty();
  }
}
