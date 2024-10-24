/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.whispersystems.textsecuregcm.controllers.KeyTransparencyController.ACI_PREFIX;
import static org.whispersystems.textsecuregcm.controllers.KeyTransparencyController.E164_PREFIX;
import static org.whispersystems.textsecuregcm.controllers.KeyTransparencyController.USERNAME_PREFIX;
import static org.whispersystems.textsecuregcm.controllers.KeyTransparencyController.getFullSearchKeyByteString;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.net.HttpHeaders;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.protobuf.ByteString;
import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ecc.Curve;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.entities.KeyTransparencyDistinguishedKeyResponse;
import org.whispersystems.textsecuregcm.entities.KeyTransparencyMonitorRequest;
import org.whispersystems.textsecuregcm.entities.KeyTransparencyMonitorResponse;
import org.whispersystems.textsecuregcm.entities.KeyTransparencySearchRequest;
import org.whispersystems.textsecuregcm.entities.KeyTransparencySearchResponse;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.keytransparency.KeyTransparencyServiceClient;
import org.whispersystems.textsecuregcm.limits.RateLimitByIpFilter;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.MockUtils;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import org.whispersystems.textsecuregcm.util.TestRemoteAddressFilterProvider;

@ExtendWith(DropwizardExtensionsSupport.class)
public class KeyTransparencyControllerTest {

  private static final String NUMBER = PhoneNumberUtil.getInstance().format(
      PhoneNumberUtil.getInstance().getExampleNumber("US"),
      PhoneNumberUtil.PhoneNumberFormat.E164);
  private static final AciServiceIdentifier ACI = new AciServiceIdentifier(UUID.randomUUID());
  private static final byte[] USERNAME_HASH = TestRandomUtil.nextBytes(20);
  private static final TestRemoteAddressFilterProvider TEST_REMOTE_ADDRESS_FILTER_PROVIDER
      = new TestRemoteAddressFilterProvider("127.0.0.1");
  private static final IdentityKey ACI_IDENTITY_KEY =  new IdentityKey(Curve.generateKeyPair().getPublicKey());
  private static final byte[] UNIDENTIFIED_ACCESS_KEY = new byte[16];
  private final KeyTransparencyServiceClient keyTransparencyServiceClient = mock(KeyTransparencyServiceClient.class);
  private static final RateLimiters rateLimiters = mock(RateLimiters.class);
  private static final RateLimiter searchRatelimiter = mock(RateLimiter.class);
  private static final RateLimiter monitorRatelimiter = mock(RateLimiter.class);
  private static final RateLimiter distinguishedRatelimiter = mock(RateLimiter.class);

  private final ResourceExtension resources = ResourceExtension.builder()
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(new AuthValueFactoryProvider.Binder<>(AuthenticatedDevice.class))
      .addProvider(TEST_REMOTE_ADDRESS_FILTER_PROVIDER)
      .addProvider(new RateLimitByIpFilter(rateLimiters))
      .setMapper(SystemMapper.jsonMapper())
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(new KeyTransparencyController(keyTransparencyServiceClient))
      .build();

  @BeforeEach
  void setup() {
    when(rateLimiters.forDescriptor(RateLimiters.For.KEY_TRANSPARENCY_DISTINGUISHED_PER_IP)).thenReturn(
        distinguishedRatelimiter);
    when(rateLimiters.forDescriptor(RateLimiters.For.KEY_TRANSPARENCY_SEARCH_PER_IP)).thenReturn(searchRatelimiter);
    when(rateLimiters.forDescriptor(RateLimiters.For.KEY_TRANSPARENCY_MONITOR_PER_IP)).thenReturn(monitorRatelimiter);
  }

  @AfterEach
  void teardown() {
    reset(rateLimiters,
        searchRatelimiter,
        monitorRatelimiter);
  }

  @Test
  void getFullSearchKey() {
    final byte[] charBytes = new byte[]{ACI_PREFIX};
    final byte[] aci = ACI.toCompactByteArray();

    final byte[] expectedFullSearchKey = new byte[aci.length + 1];
    System.arraycopy(charBytes, 0, expectedFullSearchKey, 0, charBytes.length);
    System.arraycopy(aci, 0, expectedFullSearchKey, charBytes.length, aci.length);

    assertArrayEquals(expectedFullSearchKey, getFullSearchKeyByteString(KeyTransparencyController.ACI_PREFIX, aci).toByteArray());
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  @ParameterizedTest
  @MethodSource
  void searchSuccess(final Optional<String> e164, final Optional<byte[]> usernameHash, final int expectedNumClientCalls,
      final Set<ByteString> expectedSearchKeys,
      final Set<ByteString> expectedValues,
      final List<Optional<ByteString>> expectedUnidentifiedAccessKey) {
    when(keyTransparencyServiceClient.search(any(), any(), any(), any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(TestRandomUtil.nextBytes(16)));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/key-transparency/search")
        .request();

    final Optional<byte[]> unidentifiedAccessKey = e164.isPresent() ? Optional.of(UNIDENTIFIED_ACCESS_KEY) : Optional.empty();
    final String searchJson = createSearchRequestJson(ACI, e164, usernameHash, ACI_IDENTITY_KEY,
        unidentifiedAccessKey, Optional.of(3L), Optional.of(4L));

    try (Response response = request.post(Entity.json(searchJson))) {
      assertEquals(200, response.getStatus());

      final KeyTransparencySearchResponse keyTransparencySearchResponse = response.readEntity(
          KeyTransparencySearchResponse.class);
      assertNotNull(keyTransparencySearchResponse.aciSearchResponse());

      usernameHash.ifPresentOrElse(
          ignored -> assertTrue(keyTransparencySearchResponse.usernameHashSearchResponse().isPresent()),
          () -> assertTrue(keyTransparencySearchResponse.usernameHashSearchResponse().isEmpty()));

      e164.ifPresentOrElse(ignored -> assertTrue(keyTransparencySearchResponse.e164SearchResponse().isPresent()),
          () -> assertTrue(keyTransparencySearchResponse.e164SearchResponse().isEmpty()));

      ArgumentCaptor<ByteString> valueArguments = ArgumentCaptor.forClass(ByteString.class);
      ArgumentCaptor<ByteString> searchKeyArguments = ArgumentCaptor.forClass(ByteString.class);
      ArgumentCaptor<Optional<ByteString>> unidentifiedAccessKeyArgument = ArgumentCaptor.forClass(Optional.class);

      verify(keyTransparencyServiceClient, times(expectedNumClientCalls)).search(searchKeyArguments.capture(), valueArguments.capture(), unidentifiedAccessKeyArgument.capture(), eq(Optional.of(3L)), eq(Optional.of(4L)),
          eq(KeyTransparencyController.KEY_TRANSPARENCY_RPC_TIMEOUT));

      assertEquals(expectedSearchKeys, new HashSet<>(searchKeyArguments.getAllValues()));
      assertEquals(expectedValues, new HashSet<>(valueArguments.getAllValues()));
      assertEquals(expectedUnidentifiedAccessKey, unidentifiedAccessKeyArgument.getAllValues());
    }
  }

  private static Stream<Arguments> searchSuccess() {
    final byte[] aciBytes = ACI.toCompactByteArray();
    final ByteString aciValueByteString = ByteString.copyFrom(aciBytes);

    final byte[] aciIdentityKeyBytes = ACI_IDENTITY_KEY.serialize();
    final ByteString aciIdentityKeyValueByteString = ByteString.copyFrom(aciIdentityKeyBytes);


    return Stream.of(
        // Only looking up ACI; ACI identity key should be the only value provided; no UAK
        Arguments.of(Optional.empty(), Optional.empty(), 1,
            Set.of(getFullSearchKeyByteString(ACI_PREFIX, aciBytes)),
            Set.of(aciIdentityKeyValueByteString),
            List.of(Optional.empty())),
        // Looking up ACI and username hash; ACI identity key and ACI should be the values provided; no UAK
        Arguments.of(Optional.empty(), Optional.of(USERNAME_HASH), 2,
            Set.of(getFullSearchKeyByteString(ACI_PREFIX, aciBytes),
                getFullSearchKeyByteString(USERNAME_PREFIX, USERNAME_HASH)),
            Set.of(aciIdentityKeyValueByteString, aciValueByteString),
            List.of(Optional.empty(), Optional.empty())),
        // Looking up ACI and phone number; ACI identity key and ACI should be the values provided; must provide UAK
        Arguments.of(Optional.of(NUMBER), Optional.empty(), 2,
            Set.of(getFullSearchKeyByteString(ACI_PREFIX, aciBytes),
                getFullSearchKeyByteString(E164_PREFIX, NUMBER.getBytes(StandardCharsets.UTF_8))),
            Set.of(aciValueByteString, aciIdentityKeyValueByteString),
            List.of(Optional.empty(), Optional.of(ByteString.copyFrom(UNIDENTIFIED_ACCESS_KEY))))
    );
  }

  @Test
  void searchAuthenticated() {
    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/key-transparency/search")
        .request()
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD));
    try (Response response = request.post(Entity.json(createSearchRequestJson(ACI, Optional.empty(), Optional.empty(),
        ACI_IDENTITY_KEY, Optional.empty(), Optional.empty(), Optional.empty())))) {
      assertEquals(400, response.getStatus());
    }
    verifyNoInteractions(keyTransparencyServiceClient);
  }

  @ParameterizedTest
  @MethodSource
  void searchGrpcErrors(final Status grpcStatus, final int httpStatus) {
    when(keyTransparencyServiceClient.search(any(), any(), any(), any(), any(), any()))
        .thenReturn(CompletableFuture.failedFuture(new CompletionException(new StatusRuntimeException(grpcStatus))));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/key-transparency/search")
        .request();
    try (Response response = request.post(Entity.json(createSearchRequestJson(ACI, Optional.empty(), Optional.empty(),
        ACI_IDENTITY_KEY, Optional.empty(),Optional.empty(), Optional.empty())))) {
      assertEquals(httpStatus, response.getStatus());
      verify(keyTransparencyServiceClient, times(1)).search(any(), any(), any(), any(), any(), any());
    }
  }

  private static Stream<Arguments> searchGrpcErrors() {
    return Stream.of(
        Arguments.of(Status.NOT_FOUND, 404),
        Arguments.of(Status.PERMISSION_DENIED, 403),
        Arguments.of(Status.INVALID_ARGUMENT, 422),
        Arguments.of(Status.UNKNOWN, 500)
    );
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  @ParameterizedTest
  @MethodSource
  void searchInvalidRequest(final AciServiceIdentifier aci,
      final IdentityKey aciIdentityKey,
      final Optional<String> e164,
      final Optional<byte[]> unidentifiedAccessKey,
      final Optional<Long> lastTreeHeadSize,
      final Optional<Long> distinguishedTreeHeadSize) {
    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/key-transparency/search")
        .request();
    try (Response response = request.post(Entity.json(
        createSearchRequestJson(aci,  e164, Optional.empty(),
            aciIdentityKey, unidentifiedAccessKey, lastTreeHeadSize, distinguishedTreeHeadSize)))) {
      assertEquals(422, response.getStatus());
      verifyNoInteractions(keyTransparencyServiceClient);
    }
  }

  private static Stream<Arguments> searchInvalidRequest() {
    return Stream.of(
        // ACI can't be null
        Arguments.of(null, ACI_IDENTITY_KEY, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()),
        // ACI identity key can't be null
        Arguments.of(ACI, null, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()),
        // lastNonDistinguishedTreeHeadSize must be positive
        Arguments.of(ACI, ACI_IDENTITY_KEY, Optional.empty(), Optional.empty(), Optional.of(0L), Optional.empty()),
        // lastDistinguishedTreeHeadSize must be positive
        Arguments.of(ACI, ACI_IDENTITY_KEY, Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(0L)),
        // E164 can't be provided without an unidentified access key
        Arguments.of(ACI, ACI_IDENTITY_KEY, Optional.of(NUMBER), Optional.empty(), Optional.empty(), Optional.empty()),
        // ...and an unidentified access key can't be provided without an E164
        Arguments.of(ACI, ACI_IDENTITY_KEY, Optional.empty(), Optional.of(UNIDENTIFIED_ACCESS_KEY), Optional.empty(), Optional.empty())
        );
  }

  @Test
  void searchRateLimited() {
    MockUtils.updateRateLimiterResponseToFail(
        rateLimiters, RateLimiters.For.KEY_TRANSPARENCY_SEARCH_PER_IP, "127.0.0.1", Duration.ofMinutes(10), true);
    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/key-transparency/search")
        .request();
    try (Response response = request.post(Entity.json(createSearchRequestJson(ACI, Optional.empty(), Optional.empty(),
        ACI_IDENTITY_KEY, Optional.empty(),Optional.empty(), Optional.empty())))) {
      assertEquals(429, response.getStatus());
      verifyNoInteractions(keyTransparencyServiceClient);
    }
  }

  @Test
  void monitorSuccess() {
    when(keyTransparencyServiceClient.monitor(any(), any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(TestRandomUtil.nextBytes(16)));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/key-transparency/monitor")
        .request();

    try (Response response = request.post(Entity.json(
        createMonitorRequestJson(
            ACI, List.of(3L),
            Optional.empty(), Optional.empty(),
            Optional.empty(), Optional.empty(),
            Optional.of(3L), Optional.of(4L))))) {
      assertEquals(200, response.getStatus());

      final KeyTransparencyMonitorResponse keyTransparencyMonitorResponse = response.readEntity(
          KeyTransparencyMonitorResponse.class);
      assertNotNull(keyTransparencyMonitorResponse.monitorResponse());

      verify(keyTransparencyServiceClient, times(1)).monitor(
          any(), eq(Optional.of(3L)), eq(Optional.of(4L)), eq(KeyTransparencyController.KEY_TRANSPARENCY_RPC_TIMEOUT));
    }
  }

  @Test
  void monitorAuthenticated() {
    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/key-transparency/monitor")
        .request()
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD));
    try (Response response = request.post(
        Entity.json(createMonitorRequestJson(ACI, List.of(3L), Optional.empty(), Optional.empty(),
            Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty())))) {
      assertEquals(400, response.getStatus());
      verifyNoInteractions(keyTransparencyServiceClient);
    }
  }

  @ParameterizedTest
  @MethodSource
  void monitorGrpcErrors(final Status grpcStatus, final int httpStatus) {
    when(keyTransparencyServiceClient.monitor(any(), any(), any(), any()))
        .thenReturn(CompletableFuture.failedFuture(new CompletionException(new StatusRuntimeException(grpcStatus))));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/key-transparency/monitor")
        .request();
    try (Response response = request.post(
        Entity.json(createMonitorRequestJson(ACI, List.of(3L), Optional.empty(), Optional.empty(),
            Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty())))) {
      assertEquals(httpStatus, response.getStatus());
      verify(keyTransparencyServiceClient, times(1)).monitor(any(), any(), any(), any());
    }
  }

  private static Stream<Arguments> monitorGrpcErrors() {
    return Stream.of(
        Arguments.of(Status.NOT_FOUND, 404),
        Arguments.of(Status.PERMISSION_DENIED, 403),
        Arguments.of(Status.INVALID_ARGUMENT, 422),
        Arguments.of(Status.UNKNOWN, 500)
    );
  }

  @ParameterizedTest
  @MethodSource
  void monitorInvalidRequest(final String requestJson) {
    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/key-transparency/monitor")
        .request();
    try (Response response = request.post(Entity.json(requestJson))) {
      assertEquals(422, response.getStatus());
      verifyNoInteractions(keyTransparencyServiceClient);
    }
  }

  private static Stream<Arguments> monitorInvalidRequest() {
    return Stream.of(
        // aci and aciPositions can't be empty
        Arguments.of(createMonitorRequestJson(null, null, Optional.empty(), Optional.empty(), Optional.empty(),
            Optional.empty(), Optional.empty(), Optional.empty())),
        // aciPositions list can't be empty
        Arguments.of(createMonitorRequestJson(ACI, Collections.emptyList(), Optional.empty(), Optional.empty(), Optional.empty(),
            Optional.empty(), Optional.empty(), Optional.empty())),
        // usernameHash cannot be empty if usernameHashPositions isn't
        Arguments.of(createMonitorRequestJson(ACI, List.of(4L), Optional.empty(), Optional.of(List.of(5L)),
            Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty())),
        // usernameHashPosition cannot be empty if usernameHash isn't
        Arguments.of(createMonitorRequestJson(ACI, List.of(4L), Optional.of(USERNAME_HASH),
            Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty())),
        // usernameHashPositions list cannot be empty
        Arguments.of(createMonitorRequestJson(ACI, List.of(4L), Optional.of(USERNAME_HASH),
            Optional.of(Collections.emptyList()), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty())),
        // e164 cannot be empty if e164Positions isn't
        Arguments.of(
            createMonitorRequestJson(ACI, List.of(4L), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.of(List.of(5L)), Optional.empty(), Optional.empty())),
        // e164Positions cannot be empty if e164 isn't
        Arguments.of(createMonitorRequestJson(ACI, List.of(4L), Optional.empty(),
            Optional.empty(), Optional.of(NUMBER), Optional.empty(), Optional.empty(), Optional.empty())),
        // e164Positions list cannot empty
        Arguments.of(createMonitorRequestJson(ACI, List.of(4L), Optional.empty(),
            Optional.empty(), Optional.of(NUMBER), Optional.of(Collections.emptyList()), Optional.empty(), Optional.empty())),
        // lastNonDistinguishedTreeHeadSize must be positive
        Arguments.of(createMonitorRequestJson(ACI, List.of(4L), Optional.empty(),
            Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(0L), Optional.empty())),
        // lastDistinguishedTreeHeadSize must be positive
        Arguments.of(createMonitorRequestJson(ACI, List.of(4L), Optional.empty(),
            Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(-1L)))
    );
  }

  @Test
  void monitorRateLimited() {
    MockUtils.updateRateLimiterResponseToFail(
        rateLimiters, RateLimiters.For.KEY_TRANSPARENCY_MONITOR_PER_IP, "127.0.0.1", Duration.ofMinutes(10), true);
    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/key-transparency/monitor")
        .request();
    try (Response response = request.post(
        Entity.json(createMonitorRequestJson(ACI, List.of(3L), Optional.empty(), Optional.empty(),
            Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty())))) {
      assertEquals(429, response.getStatus());
      verifyNoInteractions(keyTransparencyServiceClient);
    }
  }

  @ParameterizedTest
  @CsvSource(", 1")
  void distinguishedSuccess(@Nullable Long lastTreeHeadSize) {
    when(keyTransparencyServiceClient.getDistinguishedKey(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(TestRandomUtil.nextBytes(16)));

    WebTarget webTarget = resources.getJerseyTest()
        .target("/v1/key-transparency/distinguished");

    if (lastTreeHeadSize != null) {
      webTarget = webTarget.queryParam("lastTreeHeadSize", lastTreeHeadSize);
    }

    try (Response response = webTarget.request().get()) {
      assertEquals(200, response.getStatus());

      final KeyTransparencyDistinguishedKeyResponse distinguishedKeyResponse = response.readEntity(
          KeyTransparencyDistinguishedKeyResponse.class);
      assertNotNull(distinguishedKeyResponse.distinguishedKeyResponse());

      verify(keyTransparencyServiceClient, times(1))
          .getDistinguishedKey(eq(Optional.ofNullable(lastTreeHeadSize)),
              eq(KeyTransparencyController.KEY_TRANSPARENCY_RPC_TIMEOUT));
    }
  }

  @Test
  void distinguishedAuthenticated() {
    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/key-transparency/distinguished")
        .request()
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD));
    try (Response response = request.get()) {
      assertEquals(400, response.getStatus());
    }
    verifyNoInteractions(keyTransparencyServiceClient);
  }

  @ParameterizedTest
  @MethodSource
  void distinguishedGrpcErrors(final Status grpcStatus, final int httpStatus) {
    when(keyTransparencyServiceClient.getDistinguishedKey(any(), any()))
        .thenReturn(CompletableFuture.failedFuture(new CompletionException(new StatusRuntimeException(grpcStatus))));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/key-transparency/distinguished")
        .request();
    try (Response response = request.get()) {
      assertEquals(httpStatus, response.getStatus());
      verify(keyTransparencyServiceClient).getDistinguishedKey(any(), any());
    }
  }

  private static Stream<Arguments> distinguishedGrpcErrors() {
    return Stream.of(
        Arguments.of(Status.NOT_FOUND, 404),
        Arguments.of(Status.PERMISSION_DENIED, 403),
        Arguments.of(Status.INVALID_ARGUMENT, 422),
        Arguments.of(Status.UNKNOWN, 500)
    );
  }

  @Test
  void distinguishedInvalidRequest() {
    when(keyTransparencyServiceClient.getDistinguishedKey(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(TestRandomUtil.nextBytes(16)));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/key-transparency/distinguished")
        .queryParam("lastTreeHeadSize", -1)
        .request();

    try (Response response = request.get()) {
      assertEquals(400, response.getStatus());

      verifyNoInteractions(keyTransparencyServiceClient);
    }
  }

  @Test
  void distinguishedRateLimited() {
    MockUtils.updateRateLimiterResponseToFail(
        rateLimiters, RateLimiters.For.KEY_TRANSPARENCY_DISTINGUISHED_PER_IP, "127.0.0.1", Duration.ofMinutes(10),
        true);
    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/key-transparency/distinguished")
        .request();
    try (Response response = request.get()) {
      assertEquals(429, response.getStatus());
      verifyNoInteractions(keyTransparencyServiceClient);
    }
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private static String createMonitorRequestJson(
      final AciServiceIdentifier aci,
      final List<Long> aciPositions,
      final Optional<byte[]> usernameHash,
      final Optional<List<Long>> usernameHashPositions,
      final Optional<String> e164,
      final Optional<List<Long>> e164Positions,
      final Optional<Long> lastTreeHeadSize,
      final Optional<Long> distinguishedTreeHeadSize) {
    final KeyTransparencyMonitorRequest request = new KeyTransparencyMonitorRequest(aci, aciPositions,
        e164, e164Positions, usernameHash, usernameHashPositions, lastTreeHeadSize, distinguishedTreeHeadSize);
    try {
      return SystemMapper.jsonMapper().writeValueAsString(request);
    } catch (final JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private static String createSearchRequestJson(
      final AciServiceIdentifier aci,
      final Optional<String> e164,
      final Optional<byte[]> usernameHash,
      final IdentityKey aciIdentityKey,
      final Optional<byte[]> unidentifiedAccessKey,
      final Optional<Long> lastTreeHeadSize,
      final Optional<Long> distinguishedTreeHeadSize) {
    final KeyTransparencySearchRequest request = new KeyTransparencySearchRequest(aci, e164, usernameHash, aciIdentityKey, unidentifiedAccessKey, lastTreeHeadSize, distinguishedTreeHeadSize);
    try {
      return SystemMapper.jsonMapper().writeValueAsString(request);
    } catch (final JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }
  }
}
