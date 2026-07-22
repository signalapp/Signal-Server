/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.verifyNoInteractions;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.protobuf.Message;
import com.google.rpc.ErrorInfo;
import com.google.rpc.RetryInfo;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.time.Duration;
import io.grpc.protobuf.StatusProto;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.function.ThrowingSupplier;

public final class GrpcTestUtils {

  private GrpcTestUtils() {
    // noop
  }

  @CanIgnoreReturnValue
  public static StatusRuntimeException assertStatusException(final Status expected, final ThrowingSupplier<?> serviceCall) {
    final StatusRuntimeException exception = Assertions.assertThrows(StatusRuntimeException.class, serviceCall::get);
    assertEquals(expected.getCode(), exception.getStatus().getCode());

    return exception;
  }

  public static void assertStatusException(final Status expected, final String expectedReason, final Throwable throwable) {
    final StatusRuntimeException exception = Assertions.assertInstanceOf(StatusRuntimeException.class, throwable);
    assertEquals(expected.getCode(), exception.getStatus().getCode());
    assertEquals(expectedReason, extractErrorInfo(exception).getReason());
  }

  public static void assertStatusException(final Status expected, final String expectedReason, final ThrowingSupplier<?> serviceCall) {
    final StatusRuntimeException exception = Assertions.assertThrows(StatusRuntimeException.class, serviceCall::get);
    assertEquals(expected.getCode(), exception.getStatus().getCode());
    assertEquals(expectedReason, extractErrorInfo(exception).getReason());
  }

  @CanIgnoreReturnValue
  public static StatusRuntimeException assertStatusInvalidArgument(final ThrowingSupplier<?> serviceCall) {
    return assertStatusException(Status.INVALID_ARGUMENT, serviceCall);
  }

  public static void assertRateLimitExceeded(
      final Duration expectedRetryAfter,
      final Executable serviceCall,
      final Object... mocksToCheckForNoInteraction) {
    final StatusRuntimeException exception = Assertions.assertThrows(StatusRuntimeException.class, serviceCall);
    assertEquals(Status.RESOURCE_EXHAUSTED.getCode(), exception.getStatus().getCode());
    assertNotNull(exception.getTrailers());

    final ErrorInfo errorInfo = extractErrorInfo(exception);
    final RetryInfo retryInfo = extractDetail(RetryInfo.class, exception);
    final Duration actual = Duration.ofSeconds(retryInfo.getRetryDelay().getSeconds(), retryInfo.getRetryDelay().getNanos());
    assertEquals(GrpcExceptions.DOMAIN, errorInfo.getDomain());
    assertEquals("RESOURCE_EXHAUSTED", errorInfo.getReason());
    assertEquals(expectedRetryAfter, actual);

    for (final Object mock: mocksToCheckForNoInteraction) {
      verifyNoInteractions(mock);
    }
  }

  public static <T extends Message> T assertStreamClosed(final Class<T> streamClosedMessageClass, final Throwable exception) {
    final StatusRuntimeException statusRuntimeException = assertInstanceOf(StatusRuntimeException.class, exception);
    final ErrorInfo errorInfo = extractErrorInfo(statusRuntimeException);
    assertEquals("STREAM_CLOSED", errorInfo.getReason());
    return extractDetail(streamClosedMessageClass, statusRuntimeException);
  }

  public static ErrorInfo extractErrorInfo(final StatusRuntimeException exception) {
    return extractDetail(ErrorInfo.class, exception);
  }

  public static <T extends Message> T extractDetail(final Class<T> detailCls, final StatusRuntimeException exception) {
    final com.google.rpc.Status status = StatusProto.fromThrowable(exception);

    return assertDoesNotThrow(() -> status.getDetailsList().stream()
        .filter(any -> any.is(detailCls)).findFirst()
        .orElseThrow(() -> new AssertionError("No error info found"))
        .unpack(detailCls));
  }
}
