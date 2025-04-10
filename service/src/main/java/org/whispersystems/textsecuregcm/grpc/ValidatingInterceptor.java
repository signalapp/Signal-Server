/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import static org.whispersystems.textsecuregcm.grpc.validators.ValidatorUtils.internalError;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.StatusException;
import java.util.Map;
import org.whispersystems.textsecuregcm.grpc.validators.E164FieldValidator;
import org.whispersystems.textsecuregcm.grpc.validators.EnumSpecifiedFieldValidator;
import org.whispersystems.textsecuregcm.grpc.validators.ExactlySizeFieldValidator;
import org.whispersystems.textsecuregcm.grpc.validators.FieldValidator;
import org.whispersystems.textsecuregcm.grpc.validators.NonEmptyFieldValidator;
import org.whispersystems.textsecuregcm.grpc.validators.PresentFieldValidator;
import org.whispersystems.textsecuregcm.grpc.validators.RangeFieldValidator;
import org.whispersystems.textsecuregcm.grpc.validators.SizeFieldValidator;

public class ValidatingInterceptor implements ServerInterceptor {

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
        } catch (final StatusException e) {
          call.close(e.getStatus(), new Metadata());
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

  private void validateMessage(final Object message) throws StatusException {
    if (message instanceof Message msg) {
      try {
        for (final Descriptors.FieldDescriptor fd: msg.getDescriptorForType().getFields()) {
          for (final Map.Entry<Descriptors.FieldDescriptor, Object> entry: fd.getOptions().getAllFields().entrySet()) {
            final Descriptors.FieldDescriptor extensionFieldDescriptor = entry.getKey();
            final String extensionName = extensionFieldDescriptor.getFullName();
            final FieldValidator validator = fieldValidators.get(extensionName);
            // not all extensions are validators, so `validator` value here could legitimately be `null`
            if (validator != null) {
              validator.validate(entry.getValue(), fd, msg);
            }
          }
        }
      } catch (final StatusException e) {
        throw e;
      } catch (final Exception e) {
        throw internalError(e);
      }
    }
  }
}
