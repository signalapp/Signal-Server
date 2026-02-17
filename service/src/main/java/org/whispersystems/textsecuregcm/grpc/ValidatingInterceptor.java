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
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.grpc.validators.E164FieldValidator;
import org.whispersystems.textsecuregcm.grpc.validators.EnumSpecifiedFieldValidator;
import org.whispersystems.textsecuregcm.grpc.validators.ExactlySizeFieldValidator;
import org.whispersystems.textsecuregcm.grpc.validators.FieldValidationException;
import org.whispersystems.textsecuregcm.grpc.validators.FieldValidator;
import org.whispersystems.textsecuregcm.grpc.validators.NonEmptyFieldValidator;
import org.whispersystems.textsecuregcm.grpc.validators.PresentFieldValidator;
import org.whispersystems.textsecuregcm.grpc.validators.RangeFieldValidator;
import org.whispersystems.textsecuregcm.grpc.validators.SizeFieldValidator;

public class ValidatingInterceptor implements ServerInterceptor {

  private static final Logger log = LoggerFactory.getLogger(ValidatingInterceptor.class);
  private final Map<String, FieldValidator> fieldValidators = Map.of(
      "org.signal.chat.require.nonEmpty", new NonEmptyFieldValidator(),
      "org.signal.chat.require.present", new PresentFieldValidator(),
      "org.signal.chat.require.specified", new EnumSpecifiedFieldValidator(),
      "org.signal.chat.require.e164", new E164FieldValidator(),
      "org.signal.chat.require.exactlySize", new ExactlySizeFieldValidator(),
      "org.signal.chat.require.range", new RangeFieldValidator(),
      "org.signal.chat.require.size", new SizeFieldValidator()
  );

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
        } catch (final StatusRuntimeException e) {
          call.close(e.getStatus(), e.getTrailers());
          forwardCalls = false;
        } catch (RuntimeException runtimeException) {
          final StatusRuntimeException grpcException = switch (runtimeException) {
            case StatusRuntimeException e -> e;
            default -> {
              log.error("Failure applying request validation to message {}", call.getMethodDescriptor().getFullMethodName(), runtimeException);
              yield GrpcExceptions.unavailable("failure applying request validation");
            }
          };
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
    if (message instanceof Message msg) {
      for (final Descriptors.FieldDescriptor fd : msg.getDescriptorForType().getFields()) {
        for (final Map.Entry<Descriptors.FieldDescriptor, Object> entry : fd.getOptions().getAllFields().entrySet()) {
          final Descriptors.FieldDescriptor extensionFieldDescriptor = entry.getKey();
          final String extensionName = extensionFieldDescriptor.getFullName();

          // first validate the field
          final FieldValidator validator = fieldValidators.get(extensionName);
          // not all extensions are validators, so `validator` value here could legitimately be `null`
          if (validator != null) {
            try {
              validator.validate(entry.getValue(), fd, msg);
            } catch (FieldValidationException e) {
              throw GrpcExceptions.fieldViolation(fd.getName(),
                  "extension %s: %s".formatted(extensionName, e.getMessage()));
            }
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
  }
}
