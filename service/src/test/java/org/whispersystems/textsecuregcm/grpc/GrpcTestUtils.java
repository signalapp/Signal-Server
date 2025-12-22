/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.verifyNoInteractions;

import com.google.protobuf.Any;
import com.google.protobuf.Message;
import com.google.rpc.ErrorInfo;
import com.google.rpc.RetryInfo;
import io.grpc.BindableService;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import io.grpc.protobuf.StatusProto;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.function.Executable;
import org.whispersystems.textsecuregcm.auth.grpc.MockAuthenticationInterceptor;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;

public final class GrpcTestUtils {

  private GrpcTestUtils() {
    // noop
  }

  public static void setupAuthenticatedExtension(
      final GrpcServerExtension extension,
      final MockAuthenticationInterceptor mockAuthenticationInterceptor,
      final MockRequestAttributesInterceptor mockRequestAttributesInterceptor,
      final UUID authenticatedAci,
      final byte authenticatedDeviceId,
      final BindableService service) {
    mockAuthenticationInterceptor.setAuthenticatedDevice(authenticatedAci, authenticatedDeviceId);
    extension.getServiceRegistry()
        .addService(ServerInterceptors.intercept(service, new ValidatingInterceptor(), mockRequestAttributesInterceptor, mockAuthenticationInterceptor, new ErrorMappingInterceptor()));
  }

  public static void setupUnauthenticatedExtension(
      final GrpcServerExtension extension,
      final MockRequestAttributesInterceptor mockRequestAttributesInterceptor,
      final BindableService service) {
    extension.getServiceRegistry()
        .addService(ServerInterceptors.intercept(service, new ValidatingInterceptor(), mockRequestAttributesInterceptor, new ErrorMappingInterceptor()));
  }

  public static void assertStatusException(final Status expected, final Executable serviceCall) {
    final StatusRuntimeException exception = Assertions.assertThrows(StatusRuntimeException.class, serviceCall);
    assertEquals(expected.getCode(), exception.getStatus().getCode());
  }

  public static void assertStatusException(final Status expected, final String expectedReason, final Executable serviceCall) {
    final StatusRuntimeException exception = Assertions.assertThrows(StatusRuntimeException.class, serviceCall);
    assertEquals(expected.getCode(), exception.getStatus().getCode());
    assertEquals(expectedReason, extractErrorInfo(exception).getReason());
  }

  public static void assertStatusInvalidArgument(final Executable serviceCall) {
    assertStatusException(Status.INVALID_ARGUMENT, serviceCall);
  }

  public static void assertStatusUnauthenticated(final Executable serviceCall) {
    assertStatusException(Status.UNAUTHENTICATED, serviceCall);
  }

  public static void assertStatusPermissionDenied(final Executable serviceCall) {
    assertStatusException(Status.PERMISSION_DENIED, serviceCall);
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
    assertEquals(errorInfo.getDomain(), GrpcExceptions.DOMAIN);
    assertEquals(errorInfo.getReason(), "RESOURCE_EXHAUSTED");
    assertEquals(expectedRetryAfter, actual);

    for (final Object mock: mocksToCheckForNoInteraction) {
      verifyNoInteractions(mock);
    }
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
