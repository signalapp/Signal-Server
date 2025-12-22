/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import com.google.protobuf.Any;
import com.google.rpc.BadRequest;
import com.google.rpc.ErrorInfo;
import com.google.rpc.RetryInfo;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import java.time.Duration;
import javax.annotation.Nullable;

public class GrpcExceptions {

  public static final String DOMAIN = "grpc.chat.signal.org";

  private static final Any ERROR_INFO_CONSTRAINT_VIOLATED = Any.pack(ErrorInfo.newBuilder()
      .setDomain(DOMAIN)
      .setReason("CONSTRAINT_VIOLATED")
      .build());

  private static final Any ERROR_INFO_RESOURCE_EXHAUSTED = Any.pack(ErrorInfo.newBuilder()
      .setDomain(DOMAIN)
      .setReason("RESOURCE_EXHAUSTED")
      .build());

  private static final Any ERROR_INFO_INVALID_CREDENTIALS = Any.pack(ErrorInfo.newBuilder()
      .setDomain(DOMAIN)
      .setReason("INVALID_CREDENTIALS")
      .build());

  private static final Any ERROR_INFO_BAD_AUTHENTICATION = Any.pack(ErrorInfo.newBuilder()
      .setDomain(DOMAIN)
      .setReason("BAD_AUTHENTICATION")
      .build());

  private static final com.google.rpc.Status UPGRADE_REQUIRED = com.google.rpc.Status.newBuilder()
      .setCode(Status.Code.INVALID_ARGUMENT.value())
      .setMessage("Upgrade required")
      .addDetails(Any.pack(ErrorInfo.newBuilder()
          .setDomain(DOMAIN)
          .setReason("UPGRADE_REQUIRED")
          .build()))
      .build();


  private GrpcExceptions() {
  }

  /// The client version provided in the User-Agent is no longer supported. The client must upgrade to use the service.
  ///
  /// @return A [StatusRuntimeException] encoding the error
  public static StatusRuntimeException upgradeRequired() {
    return StatusProto.toStatusRuntimeException(UPGRADE_REQUIRED);
  }

  /// The RPC argument violated a constraint that was annotated or documented in the service definition. It is always
  /// possible to check this constraint without communicating with the chat server. This always represents a client bug
  /// or out of date client. Additional information about the violating field will be included in the metadata.
  ///
  /// @param fieldName The name of the field that violated a service constraint
  /// @param message   Additional context about the constraint violation
  /// @return A [StatusRuntimeException] encoding the error
  public static StatusRuntimeException fieldViolation(final String fieldName, @Nullable final String message) {
    return StatusProto.toStatusRuntimeException(com.google.rpc.Status.newBuilder()
        .setCode(Status.Code.INVALID_ARGUMENT.value())
        .setMessage(messageOrDefault(message, Status.Code.INVALID_ARGUMENT))
        .addDetails(ERROR_INFO_CONSTRAINT_VIOLATED)
        .addDetails(Any.pack(BadRequest.newBuilder()
            .addFieldViolations(BadRequest.FieldViolation.newBuilder()
                .setField(fieldName)
                .setDescription(messageOrDefault(message, Status.Code.INVALID_ARGUMENT)))
            .build()))
        .build());
  }

  /// The RPC argument violated a constraint that was annotated or documented in the service definition. It is always
  /// possible to check this constraint without communicating with the chat server. This always represents a client bug
  /// or out of date client.
  ///
  /// @param message   Additional context about the constraint violation
  /// @return A [StatusRuntimeException] encoding the error
  public static StatusRuntimeException constraintViolation(@Nullable final String message) {
    return StatusProto.toStatusRuntimeException(com.google.rpc.Status.newBuilder()
        .setCode(Status.Code.INVALID_ARGUMENT.value())
        .setMessage(messageOrDefault(message, Status.Code.INVALID_ARGUMENT))
        .addDetails(ERROR_INFO_CONSTRAINT_VIOLATED)
        .build());
  }

  ///  The request has incorrectly set authentication credentials for the RPC. This represents a client bug where the
  /// authorization header is not correct for the RPC. For example,
  ///
  ///  - The RPC was for an anonymous service, but included an Authentication header in the RPC metadata
  ///  - The RPC should only be made by the primary device, but the request had linked device credentials
  ///
  /// @param message indicating why the credentials were set incorrectly
  /// @return A [StatusRuntimeException] encoding the error
  public static StatusRuntimeException badAuthentication(@Nullable final String message) {
    return StatusProto.toStatusRuntimeException(com.google.rpc.Status.newBuilder()
        .setCode(Status.Code.INVALID_ARGUMENT.value())
        .setMessage(messageOrDefault(message, Status.Code.INVALID_ARGUMENT))
        .addDetails(ERROR_INFO_BAD_AUTHENTICATION)
        .build());
  }

  /// The account credentials provided in the authorization header are no longer valid.
  ///
  /// @param message indicating why the credentials were invalid
  /// @return A [StatusRuntimeException] encoding the error
  public static StatusRuntimeException invalidCredentials(@Nullable final String message) {
    return StatusProto.toStatusRuntimeException(com.google.rpc.Status.newBuilder()
        .setCode(Status.Code.UNAUTHENTICATED.value())
        .setMessage(messageOrDefault(message, Status.Code.UNAUTHENTICATED))
        .addDetails(ERROR_INFO_INVALID_CREDENTIALS)
        .build());
  }

  /// A server-side resource was exhausted. The details field may include a RetryInfo message that includes the amount
  /// of time in seconds the client should wait before retrying the request.
  ///
  /// If a RetryInfo is present, the client must wait the indicated time before retrying the request. If absent, the
  /// client should retry with an exponential backoff.
  ///
  /// @param retryDuration If present, the duration the client should wait before retrying the request
  /// @return A [StatusRuntimeException] encoding the error
  public static StatusRuntimeException rateLimitExceeded(@Nullable final Duration retryDuration) {
    final com.google.rpc.Status.Builder builder = com.google.rpc.Status.newBuilder()
        .setCode(Status.Code.RESOURCE_EXHAUSTED.value())
        .addDetails(ERROR_INFO_RESOURCE_EXHAUSTED);

    if (retryDuration != null) {
      builder.addDetails(Any.pack(RetryInfo.newBuilder()
          .setRetryDelay(com.google.protobuf.Duration.newBuilder()
              .setSeconds(retryDuration.getSeconds())
              .setNanos(retryDuration.getNano()))
          .build()));
    }
    return StatusProto.toStatusRuntimeException(builder.build());
  }

  /// There was an internal error processing the RPC. The client should retry the request with exponential backoff.
  ///
  /// @return A [StatusRuntimeException] encoding the error
  public static StatusRuntimeException unavailable(@Nullable final String message) {
    return StatusProto.toStatusRuntimeException(com.google.rpc.Status.newBuilder()
        .setCode(Status.Code.UNAVAILABLE.value())
        .setMessage(messageOrDefault(message, Status.Code.UNAVAILABLE))
        .addDetails(Any.pack(ErrorInfo.newBuilder()
            .setDomain(DOMAIN)
            .setReason("UNAVAILABLE")
            .build()))
        .build());
  }

  private static String messageOrDefault(@Nullable final String message, Status.Code code) {
    return message == null ? code.toString() : message;
  }
}
