/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.signal.chat.require.Auth;
import org.signal.chat.rpc.Color;
import org.signal.chat.rpc.ReactorAnonymousServiceGrpc;
import org.signal.chat.rpc.ReactorAuthServiceGrpc;
import org.signal.chat.rpc.ReactorValidationTestServiceGrpc;
import org.signal.chat.rpc.ValidationTestServiceGrpc;
import org.signal.chat.rpc.ValidationsRequest;
import org.signal.chat.rpc.ValidationsResponse;
import org.whispersystems.textsecuregcm.grpc.validators.ValidatorUtils;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import reactor.core.publisher.Mono;

public class ValidatingInterceptorTest {

  @RegisterExtension
  static final GrpcServerExtension GRPC_SERVER_EXTENSION = new GrpcServerExtension();

  private static final class ValidationTestGrpcServiceImpl extends
      ReactorValidationTestServiceGrpc.ValidationTestServiceImplBase {

    @Override
    public Mono<ValidationsResponse> validationsEndpoint(final ValidationsRequest request) {
      return Mono.just(ValidationsResponse.newBuilder().build());
    }
  }

  private static final class AuthGrpcServiceImpl extends ReactorAuthServiceGrpc.AuthServiceImplBase {

    @Override
    public Mono<Empty> authenticatedMethod(final Empty request) {
      return Mono.just(Empty.getDefaultInstance());
    }
  }

  private static final class AnonymousGrpcServiceImpl extends ReactorAnonymousServiceGrpc.AnonymousServiceImplBase {

    @Override
    public Mono<Empty> anonymousMethod(final Empty request) {
      return Mono.just(Empty.getDefaultInstance());
    }
  }

  private ValidationTestServiceGrpc.ValidationTestServiceBlockingStub stub;


  @BeforeEach
  void setUp() {
    final ValidationTestGrpcServiceImpl validationTestGrpcService = new ValidationTestGrpcServiceImpl();
    final AuthGrpcServiceImpl authGrpcService = new AuthGrpcServiceImpl();
    final AnonymousGrpcServiceImpl anonymousGrpcService = new AnonymousGrpcServiceImpl();

    GRPC_SERVER_EXTENSION.getServiceRegistry()
        .addService(ServerInterceptors.intercept(validationTestGrpcService, new ValidatingInterceptor()));
    GRPC_SERVER_EXTENSION.getServiceRegistry()
        .addService(authGrpcService);
    GRPC_SERVER_EXTENSION.getServiceRegistry()
        .addService(anonymousGrpcService);

    stub = ValidationTestServiceGrpc.newBlockingStub(GRPC_SERVER_EXTENSION.getChannel());
  }

  @ParameterizedTest
  @ValueSource(strings = {"15551234567", "", "123", "+1 555 1234567", "asdf"})
  public void testE164ValidationFailure(final String invalidNumber) throws Exception {
    assertStatusException(Status.INVALID_ARGUMENT, () -> stub.validationsEndpoint(
        builderWithValidDefaults()
            .setNumber(invalidNumber)
            .build()
    ));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 3, 4, 6, 1000})
  public void testExactlySizeValidationFailure(final int size) throws Exception {
    final String stringValue = RandomStringUtils.secure().nextAlphanumeric(size);
    assertStatusException(Status.INVALID_ARGUMENT, () -> stub.validationsEndpoint(
        builderWithValidDefaults()
            .setFixedSizeString(stringValue)
            .build()
    ));

    final ByteString byteValue = ByteString.copyFrom(TestRandomUtil.nextBytes(size));
    assertStatusException(Status.INVALID_ARGUMENT, () -> stub.validationsEndpoint(
        builderWithValidDefaults()
            .setFixedSizeBytes(byteValue)
            .build()
    ));

    final List<String> listValue = IntStream.range(0, size)
        .mapToObj(i -> RandomStringUtils.secure().nextAlphabetic(10))
        .toList();
    assertStatusException(Status.INVALID_ARGUMENT, () -> stub.validationsEndpoint(
        builderWithValidDefaults()
            .clearFixedSizeList()
            .addAllFixedSizeList(listValue)
            .build()
    ));
  }

  @Test
  public void testExactlySizeMultiplePermittedValues() throws Exception {
    assertStatusException(Status.INVALID_ARGUMENT, () -> stub.validationsEndpoint(
        builderWithValidDefaults()
            .setExactlySizeVariants("abc")
            .build()
    ));
    assertStatusException(Status.INVALID_ARGUMENT, () -> stub.validationsEndpoint(
        builderWithValidDefaults()
            .setExactlySizeVariants("")
            .build()
    ));
    assertStatusException(Status.INVALID_ARGUMENT, () -> stub.validationsEndpoint(
        builderWithValidDefaults()
            .clearExactlySizeVariants()
            .build()
    ));
    stub.validationsEndpoint(
        builderWithValidDefaults()
            .setExactlySizeVariants("ab")
            .build()
    );
    stub.validationsEndpoint(
        builderWithValidDefaults()
            .setExactlySizeVariants("abcd")
            .build()
    );
    stub.validationsEndpoint(
        builderWithValidDefaults()
            .build()
    );
  }

  public static Stream<Arguments> testRangeSizeValidationFailure() {
    return Stream.of(
        Arguments.of(0, Status.INVALID_ARGUMENT),
        Arguments.of(1, Status.INVALID_ARGUMENT),
        Arguments.of(2, Status.INVALID_ARGUMENT),
        Arguments.of(3, Status.OK),
        Arguments.of(4, Status.OK),
        Arguments.of(5, Status.OK),
        Arguments.of(6, Status.OK),
        Arguments.of(7, Status.OK),
        Arguments.of(8, Status.OK),
        Arguments.of(9, Status.INVALID_ARGUMENT),
        Arguments.of(1000, Status.INVALID_ARGUMENT)
    );
  }

  @ParameterizedTest
  @MethodSource
  public void testRangeSizeValidationFailure(final int size, final Status expectedStatus) throws Exception {
    final String stringValue = RandomStringUtils.secure().nextAlphanumeric(size);
    assertEquals(expectedStatus.getCode(), requestStatus(() -> stub.validationsEndpoint(
        builderWithValidDefaults()
            .setRangeSizeString(stringValue)
            .build()
    )).getCode());

    final ByteString byteValue = ByteString.copyFrom(TestRandomUtil.nextBytes(size));
    assertEquals(expectedStatus.getCode(), requestStatus(() -> stub.validationsEndpoint(
        builderWithValidDefaults()
            .setRangeSizeBytes(byteValue)
            .build()
    )).getCode());

    final List<String> listValue = IntStream.range(0, size)
        .mapToObj(i -> RandomStringUtils.secure().nextAlphabetic(10))
        .toList();
    assertEquals(expectedStatus.getCode(), requestStatus(() -> stub.validationsEndpoint(
        builderWithValidDefaults()
            .clearRangeSizeList()
            .addAllRangeSizeList(listValue)
            .build()
    )).getCode());
  }

  @Test
  public void testNotOptionalWithMaxLimit() throws Exception {
    stub.validationsEndpoint(
        builderWithValidDefaults()
            .clearWithMaxBytes()
            .build()
    );
    stub.validationsEndpoint(
        builderWithValidDefaults()
            .clearWithMaxString()
            .build()
    );
  }

  @Test
  public void testNotOptionalWithMinLimit() throws Exception {
    assertStatusException(Status.INVALID_ARGUMENT, () -> stub.validationsEndpoint(
        builderWithValidDefaults()
            .clearWithMinBytes()
            .build()
    ));
    assertStatusException(Status.INVALID_ARGUMENT, () -> stub.validationsEndpoint(
        builderWithValidDefaults()
            .clearWithMinString()
            .build()
    ));
  }

  @Test
  public void testServiceExtensionValueExtraction() throws Exception {
    final Map<String, Optional<Auth>> authValues = GRPC_SERVER_EXTENSION.getServiceRegistry().getServices()
        .stream()
        .map(sd -> Pair.of(
            sd.getServiceDescriptor().getName(),
            ValidatorUtils.serviceAuthExtensionValue(sd)
        ))
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    assertEquals(Map.of(
        "org.signal.chat.rpc.ValidationTestService", Optional.empty(),
        "org.signal.chat.rpc.AuthService", Optional.of(Auth.AUTH_ONLY_AUTHENTICATED),
        "org.signal.chat.rpc.AnonymousService", Optional.of(Auth.AUTH_ONLY_ANONYMOUS)
    ), authValues);
  }

  @Test
  public void testNonEmpty() throws Exception {
    assertStatusException(Status.INVALID_ARGUMENT, () -> stub.validationsEndpoint(
        builderWithValidDefaults()
            .clearNonEmptyList()
            .build()
    ));
    // check not setting a value
    assertStatusException(Status.INVALID_ARGUMENT, () -> stub.validationsEndpoint(
        builderWithValidDefaults()
            .clearNonEmptyBytes()
            .build()
    ));
    assertStatusException(Status.INVALID_ARGUMENT, () -> stub.validationsEndpoint(
        builderWithValidDefaults()
            .clearNonEmptyBytesOptional()
            .build()
    ));
    assertStatusException(Status.INVALID_ARGUMENT, () -> stub.validationsEndpoint(
        builderWithValidDefaults()
            .clearNonEmptyString()
            .build()
    ));
    assertStatusException(Status.INVALID_ARGUMENT, () -> stub.validationsEndpoint(
        builderWithValidDefaults()
            .clearNonEmptyStringOptional()
            .build()
    ));
    // now check explicitly setting an empty value
    assertStatusException(Status.INVALID_ARGUMENT, () -> stub.validationsEndpoint(
        builderWithValidDefaults()
            .setNonEmptyBytes(ByteString.EMPTY)
            .build()
    ));
    assertStatusException(Status.INVALID_ARGUMENT, () -> stub.validationsEndpoint(
        builderWithValidDefaults()
            .setNonEmptyBytesOptional(ByteString.EMPTY)
            .build()
    ));
    assertStatusException(Status.INVALID_ARGUMENT, () -> stub.validationsEndpoint(
        builderWithValidDefaults()
            .setNonEmptyString("")
            .build()
    ));
    assertStatusException(Status.INVALID_ARGUMENT, () -> stub.validationsEndpoint(
        builderWithValidDefaults()
            .setNonEmptyStringOptional("")
            .build()
    ));
  }

  @Test
  public void testEnumSpecified() throws Exception {
    assertStatusException(Status.INVALID_ARGUMENT, () -> stub.validationsEndpoint(
        builderWithValidDefaults()
            .clearColor()
            .build()
    ));
    assertStatusException(Status.INVALID_ARGUMENT, () -> stub.validationsEndpoint(
        builderWithValidDefaults()
            .setColor(Color.COLOR_UNSPECIFIED)
            .build()
    ));
    assertStatusException(Status.INVALID_ARGUMENT, () -> stub.validationsEndpoint(
        builderWithValidDefaults()
            .clearColorOptional()
            .build()
    ));
    assertStatusException(Status.INVALID_ARGUMENT, () -> stub.validationsEndpoint(
        builderWithValidDefaults()
            .setColorOptional(Color.COLOR_UNSPECIFIED)
            .build()
    ));
  }

  @Test
  public void testRange() throws Exception {
    assertStatusException(Status.INVALID_ARGUMENT, () -> stub.validationsEndpoint(
        builderWithValidDefaults()
            .setI32(1000)
            .build()
    ));
    assertStatusException(Status.INVALID_ARGUMENT, () -> stub.validationsEndpoint(
        builderWithValidDefaults()
            .setUi32(-1)
            .build()
    ));
    assertStatusException(Status.INVALID_ARGUMENT, () -> stub.validationsEndpoint(
        builderWithValidDefaults()
            .clearI32Range()
            .build()
    ));
    assertStatusException(Status.INVALID_ARGUMENT, () -> stub.validationsEndpoint(
        builderWithValidDefaults()
            .setI32OptRange(5)
            .build()
    ));
    assertStatusException(Status.INVALID_ARGUMENT, () -> stub.validationsEndpoint(
        builderWithValidDefaults()
            .setI32OptRange(1000)
            .build()
    ));
  }

  @Test
  public void testPresent() throws Exception {
    assertStatusException(Status.INVALID_ARGUMENT, () -> stub.validationsEndpoint(
        builderWithValidDefaults()
            .clearPresentMessage()
            .build()
    ));

    assertStatusException(Status.INVALID_ARGUMENT, () -> stub.validationsEndpoint(
        builderWithValidDefaults()
            .clearOptionalPresentMessage()
            .build()
    ));
  }

  @Test
  public void testAllFieldsValidationSuccess() throws Exception {
    stub.validationsEndpoint(builderWithValidDefaults().build());
  }

  @Nonnull
  private static ValidationsRequest.Builder builderWithValidDefaults() {
    return ValidationsRequest.newBuilder()
        .setNumber("+15551234567")
        .setFixedSizeString("12345")
        .setFixedSizeBytes(ByteString.copyFrom(new byte[5]))
        .setWithMinBytes(ByteString.copyFrom(new byte[5]))
        .setWithMaxBytes(ByteString.copyFrom(new byte[5]))
        .setWithMinString("12345")
        .setWithMaxString("12345")
        .setExactlySizeVariants("ab")
        .setRangeSizeString("abc")
        .setNonEmptyString("abc")
        .setNonEmptyStringOptional("abc")
        .setPresentMessage(ValidationsRequest.RequirePresentMessage.getDefaultInstance())
        .setOptionalPresentMessage(ValidationsRequest.RequirePresentMessage.getDefaultInstance())
        .setColor(Color.COLOR_GREEN)
        .setColorOptional(Color.COLOR_GREEN)
        .setNonEmptyBytes(ByteString.copyFrom(new byte[5]))
        .setNonEmptyBytesOptional(ByteString.copyFrom(new byte[5]))
        .addAllNonEmptyList(List.of("a", "b", "c", "d", "e"))
        .setRangeSizeBytes(ByteString.copyFrom(new byte[3]))
        .addAllFixedSizeList(List.of("a", "b", "c", "d", "e"))
        .addAllRangeSizeList(List.of("a", "b", "c", "d", "e"))
        .setI32Range(15);
  }

  private static void assertStatusException(final Status expected, final Executable serviceCall) {
    final StatusRuntimeException exception = Assertions.assertThrows(StatusRuntimeException.class, serviceCall);
    assertEquals(expected.getCode(), exception.getStatus().getCode());
  }

  private static Status requestStatus(final Runnable runnable) {
    try {
      runnable.run();
      return Status.OK;
    } catch (final StatusRuntimeException e) {
      return e.getStatus();
    }
  }
}
