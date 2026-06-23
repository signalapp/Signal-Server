/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.Map;
import org.signal.chat.require.ElementConstraint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.grpc.validators.Base64UrlFieldValidator;
import org.whispersystems.textsecuregcm.grpc.validators.FieldValidator;
import org.whispersystems.textsecuregcm.grpc.validators.E164FieldValidator;
import org.whispersystems.textsecuregcm.grpc.validators.EnumSpecifiedFieldValidator;
import org.whispersystems.textsecuregcm.grpc.validators.ExactlySizeFieldValidator;
import org.whispersystems.textsecuregcm.grpc.validators.FieldValidationException;
import org.whispersystems.textsecuregcm.grpc.validators.NonEmptyFieldValidator;
import org.whispersystems.textsecuregcm.grpc.validators.PresentFieldValidator;
import org.whispersystems.textsecuregcm.grpc.validators.RangeFieldValidator;
import org.whispersystems.textsecuregcm.grpc.validators.ServiceIdentifierIdentityTypeValidator;
import org.whispersystems.textsecuregcm.grpc.validators.SizeFieldValidator;

public class ValidatingInterceptor implements ServerInterceptor {

  private static final Logger log = LoggerFactory.getLogger(ValidatingInterceptor.class);
  private static final String REQUIRE_PATH = "org.signal.chat.require.";
  private static final String EACH_PATH = "org.signal.chat.require.each";

  // The keys in this map correspond to the names of our custom FieldOptions as well as the names in the fields of
  // `ElementConstraint` which itself is the value of the `each` FieldOption. For this reason it's a good idea to make
  // the names match when they refer to the same validator.
  private final static Map<String, FieldValidator<?>> VALIDATORS = Map.of(
      "nonEmpty", new NonEmptyFieldValidator(),
      "specified", new EnumSpecifiedFieldValidator(),
      "e164", new E164FieldValidator(),
      "exactlySize", new ExactlySizeFieldValidator(),
      "range", new RangeFieldValidator(),
      "size", new SizeFieldValidator(),
      "base64url", new Base64UrlFieldValidator(),
      "identityType", new ServiceIdentifierIdentityTypeValidator(),
      "present", new PresentFieldValidator());

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      final ServerCall<ReqT, RespT> call,
      final Metadata headers,
      final ServerCallHandler<ReqT, RespT> next) {
    return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(next.startCall(call, headers)) {

      // The way `UnaryServerCallHandler` (which is what we're wrapping here) is implemented
      // is when `onMessage()` is called, the processing of the message doesn't immediately start
      // and instead is delayed until `onHalfClose()` (which is the point when client says
      // that no more messages will be sent). Then, in `onHalfClose()` it either tries to process
      // the message if it's there, or reports an error if the message is not there.
      // This means that the logic is not designed for the case of the call being closed by the interceptor.
      // The only workaround is to not delegate calls to it in the case when we're closing the call
      // because of the validation error.
      private boolean forwardCalls = true;

      @Override
      public void onMessage(final ReqT message) {
        try {
          validateMessage(message);
          super.onMessage(message);
        } catch (RuntimeException runtimeException) {
          final StatusRuntimeException grpcException = runtimeException instanceof StatusRuntimeException sre
              ? sre
              : GrpcExceptions.unavailable("failure applying request validation");
          if (grpcException.getStatus().getCode() != Status.Code.INVALID_ARGUMENT) {
            log.error("Failure applying request validation to message {}",
                call.getMethodDescriptor().getFullMethodName(), runtimeException);
          }
          call.close(grpcException.getStatus(), grpcException.getTrailers());
          forwardCalls = false;
        }
      }

      @Override
      public void onHalfClose() {
        if (forwardCalls) {
          super.onHalfClose();
        }
      }
    };
  }

  private void validateMessage(final Object message) {
    if (!(message instanceof Message msg)) {
      return;
    }
    for (final Descriptors.FieldDescriptor fd : msg.getDescriptorForType().getFields()) {
      for (final Map.Entry<Descriptors.FieldDescriptor, Object> entry : fd.getOptions().getAllFields().entrySet()) {
        final Descriptors.FieldDescriptor extensionFieldDescriptor = entry.getKey();

        if (!extensionFieldDescriptor.getFullName().startsWith(REQUIRE_PATH) || (fd.getRealContainingOneof() != null && !msg.hasField(fd))) {
          // Either this is a non-require extension, or this is oneof but this field isn't set. In the latter case
          // we assume if you have a validator that requires presence, you don't actually want presence enforcement on
          // a oneof. In either case we should just skip this validator.
        } else if (extensionFieldDescriptor.getFullName().equals(EACH_PATH)) {
          if (!(entry.getValue() instanceof ElementConstraint elementConstraint)) {
            throw new IllegalStateException("'each' value must be an ElementConstraint");
          }
          validateRepeatedElementConstraints(elementConstraint, msg, fd);
        } else {
          validateField(getValidatorOrThrow(extensionFieldDescriptor), entry.getValue(), msg, fd);
        }
      }

      // Recursively validate the field's value(s) if it is a message or a repeated field
      // gRPC's proto deserialization limits nesting to 100 so this has bounded stack usage
      if (fd.isRepeated() && msg.getField(fd) instanceof List list) {
        // Checking for repeated fields also handles maps, because maps are syntax sugar for repeated MapEntries
        // which themselves are Messages that will be recursively descended.
        for (final Object o : list) {
          validateMessage(o);
        }
      } else if (fd.hasPresence() && msg.hasField(fd)) {
        // If the field has presence information and is present, recursively validate it. Not all fields have
        // presence, but we only validate Message type fields anyway, which always have explicit presence.
        validateMessage(msg.getField(fd));
      }
    }
  }

  private void validateField(final FieldValidator<?> validator, final Object extensionValue, final Message msg, final Descriptors.FieldDescriptor fd) {
    // for the fields with an `optional` modifier, checking if the field was set
    // and if not, checking if extension allows missing optional field
    if (fd.hasPresence() && !msg.hasField(fd)) {
      switch (validator.getMissingOptionalAction()) {
        case FAIL -> throw fieldViolation(fd, validator.getExtensionName(), "extension requires a value to be set");
        case SUCCEED -> {
          return;
        }
        case VALIDATE_DEFAULT_VALUE -> {}
      }
    }

    try {
      validator.validate(extensionValue, fd, msg.getField(fd));
    } catch (FieldValidationException e) {
      throw fieldViolation(fd, validator.getExtensionName(), e.getMessage());
    }
  }

  private void validateRepeatedElementConstraints(
      final ElementConstraint elementConstraint,
      final Message message,
      final Descriptors.FieldDescriptor fd) {
    if (!fd.isRepeated() || fd.isMapField()) {
      throw new IllegalStateException("each may only be applied to repeated fields");
    }

    for (final Map.Entry<Descriptors.FieldDescriptor, Object> entry : elementConstraint.getAllFields().entrySet()) {
      final FieldValidator<?> elementValidator = getValidatorOrThrow(entry.getKey());
      final int count = message.getRepeatedFieldCount(fd);
      for (int i = 0; i < count; i++) {
        final Object element = message.getRepeatedField(fd, i);
        try {
          elementValidator.validate(entry.getValue(), fd, element);
        } catch (final FieldValidationException e) {
          throw fieldViolation(fd, entry.getKey().getFullName(), "element [%d]: %s".formatted(i, e.getMessage()));
        }
      }
    }
  }

  private StatusRuntimeException fieldViolation(final Descriptors.FieldDescriptor fd, final String extensionName, final String message) {
    return GrpcExceptions.fieldViolation(fd.getName(), "extension %s: %s".formatted(extensionName, message));
  }

  private static FieldValidator<?> getValidatorOrThrow(final Descriptors.FieldDescriptor extensionFd) {
    final FieldValidator<?> validator = VALIDATORS.get(extensionFd.getName());
    if (validator == null) {
      throw new IllegalStateException("unrecognized extension " + extensionFd.getFullName());
    }
    return validator;
  }

}
