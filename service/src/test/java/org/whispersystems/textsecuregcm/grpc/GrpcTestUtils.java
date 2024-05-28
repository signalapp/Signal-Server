/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.verifyNoInteractions;

import io.grpc.BindableService;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.time.Duration;
import java.util.UUID;
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
    assertEquals(Status.RESOURCE_EXHAUSTED, exception.getStatus());
    assertNotNull(exception.getTrailers());
    assertEquals(expectedRetryAfter, exception.getTrailers().get(RateLimitExceededException.RETRY_AFTER_DURATION_KEY));
    for (final Object mock: mocksToCheckForNoInteraction) {
      verifyNoInteractions(mock);
    }
  }
}
