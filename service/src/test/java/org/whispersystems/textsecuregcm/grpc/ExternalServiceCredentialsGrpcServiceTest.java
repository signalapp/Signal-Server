/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.whispersystems.textsecuregcm.grpc.GrpcTestUtils.assertRateLimitExceeded;
import static org.whispersystems.textsecuregcm.grpc.GrpcTestUtils.assertStatusException;
import static org.whispersystems.textsecuregcm.grpc.GrpcTestUtils.assertStatusUnauthenticated;

import io.grpc.Status;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.signal.chat.credentials.ExternalServiceCredentialsGrpc;
import org.signal.chat.credentials.ExternalServiceType;
import org.signal.chat.credentials.GetExternalServiceCredentialsRequest;
import org.signal.chat.credentials.GetExternalServiceCredentialsResponse;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.util.MockUtils;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import reactor.core.publisher.Mono;

public class ExternalServiceCredentialsGrpcServiceTest
    extends SimpleBaseGrpcTest<ExternalServiceCredentialsGrpcService, ExternalServiceCredentialsGrpc.ExternalServiceCredentialsBlockingStub> {

  private static final ExternalServiceCredentialsGenerator DIRECTORY_CREDENTIALS_GENERATOR = Mockito.spy(ExternalServiceCredentialsGenerator
      .builder(TestRandomUtil.nextBytes(32))
      .withUserDerivationKey(TestRandomUtil.nextBytes(32))
      .prependUsername(false)
      .truncateSignature(false)
      .build());

  private static final ExternalServiceCredentialsGenerator PAYMENTS_CREDENTIALS_GENERATOR = Mockito.spy(ExternalServiceCredentialsGenerator
      .builder(TestRandomUtil.nextBytes(32))
      .prependUsername(true)
      .build());

  @Mock
  private RateLimiters rateLimiters;


  @Override
  protected ExternalServiceCredentialsGrpcService createServiceBeforeEachTest() {
    return new ExternalServiceCredentialsGrpcService(Map.of(
        ExternalServiceType.EXTERNAL_SERVICE_TYPE_DIRECTORY, DIRECTORY_CREDENTIALS_GENERATOR,
        ExternalServiceType.EXTERNAL_SERVICE_TYPE_PAYMENTS, PAYMENTS_CREDENTIALS_GENERATOR
    ), rateLimiters);
  }

  static Stream<Arguments> testSuccess() {
    return Stream.of(
        Arguments.of(ExternalServiceType.EXTERNAL_SERVICE_TYPE_DIRECTORY, DIRECTORY_CREDENTIALS_GENERATOR),
        Arguments.of(ExternalServiceType.EXTERNAL_SERVICE_TYPE_PAYMENTS, PAYMENTS_CREDENTIALS_GENERATOR)
    );
  }

  @ParameterizedTest
  @MethodSource
  public void testSuccess(
      final ExternalServiceType externalServiceType,
      final ExternalServiceCredentialsGenerator credentialsGenerator) throws Exception {
    final RateLimiter limiter = mock(RateLimiter.class);
    doReturn(limiter).when(rateLimiters).forDescriptor(eq(RateLimiters.For.EXTERNAL_SERVICE_CREDENTIALS));
    doReturn(Mono.fromFuture(CompletableFuture.completedFuture(null))).when(limiter).validateReactive(eq(AUTHENTICATED_ACI));
    final GetExternalServiceCredentialsResponse artResponse = authenticatedServiceStub().getExternalServiceCredentials(
        GetExternalServiceCredentialsRequest.newBuilder()
            .setExternalService(externalServiceType)
            .build());
    final Optional<Long> artValidation = credentialsGenerator.validateAndGetTimestamp(
        new ExternalServiceCredentials(artResponse.getUsername(), artResponse.getPassword()));
    assertTrue(artValidation.isPresent());
  }

  @ParameterizedTest
  @ValueSource(ints = { -1, 0, 1000 })
  public void testUnrecognizedService(final int externalServiceTypeValue) throws Exception {
    assertStatusException(Status.INVALID_ARGUMENT, () -> authenticatedServiceStub().getExternalServiceCredentials(
        GetExternalServiceCredentialsRequest.newBuilder()
            .setExternalServiceValue(externalServiceTypeValue)
            .build()));
  }

  @Test
  public void testInvalidRequest() throws Exception {
    assertStatusException(Status.INVALID_ARGUMENT, () -> authenticatedServiceStub().getExternalServiceCredentials(
        GetExternalServiceCredentialsRequest.newBuilder()
            .build()));
  }

  @Test
  public void testRateLimitExceeded() throws Exception {
    final Duration retryAfter = MockUtils.updateRateLimiterResponseToFail(
        rateLimiters, RateLimiters.For.EXTERNAL_SERVICE_CREDENTIALS, AUTHENTICATED_ACI, Duration.ofSeconds(100));
    Mockito.reset(DIRECTORY_CREDENTIALS_GENERATOR);
    assertRateLimitExceeded(
        retryAfter,
        () -> authenticatedServiceStub().getExternalServiceCredentials(
            GetExternalServiceCredentialsRequest.newBuilder()
                .setExternalService(ExternalServiceType.EXTERNAL_SERVICE_TYPE_DIRECTORY)
                .build()),
        DIRECTORY_CREDENTIALS_GENERATOR
    );
  }

  @Test
  public void testUnauthenticatedCall() throws Exception {
    assertStatusUnauthenticated(() -> unauthenticatedServiceStub().getExternalServiceCredentials(
        GetExternalServiceCredentialsRequest.newBuilder()
            .setExternalService(ExternalServiceType.EXTERNAL_SERVICE_TYPE_DIRECTORY)
            .build()));
  }

  /**
   * `ExternalServiceDefinitions` enum is supposed to have entries for all values in `ExternalServiceType`,
   * except for the `EXTERNAL_SERVICE_TYPE_UNSPECIFIED` and `UNRECOGNIZED`.
   * This test makes sure that is the case.
   */
  @ParameterizedTest
  @EnumSource(mode = EnumSource.Mode.EXCLUDE, names = { "UNRECOGNIZED", "EXTERNAL_SERVICE_TYPE_UNSPECIFIED" })
  public void testHaveExternalServiceDefinitionForServiceTypes(final ExternalServiceType externalServiceType) throws Exception {
    assertTrue(
        Arrays.stream(ExternalServiceDefinitions.values()).anyMatch(v -> v.externalService() == externalServiceType),
        "`ExternalServiceDefinitions` enum entry is missing for the `%s` value of `ExternalServiceType`".formatted(externalServiceType)
    );
  }
}
