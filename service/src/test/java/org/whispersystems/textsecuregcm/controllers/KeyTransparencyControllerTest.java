/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.net.HttpHeaders;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
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

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.Response;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(DropwizardExtensionsSupport.class)
public class KeyTransparencyControllerTest {

  private static final String NUMBER = PhoneNumberUtil.getInstance().format(
      PhoneNumberUtil.getInstance().getExampleNumber("US"),
      PhoneNumberUtil.PhoneNumberFormat.E164);
  private static final AciServiceIdentifier ACI = new AciServiceIdentifier(UUID.randomUUID());
  private static final TestRemoteAddressFilterProvider TEST_REMOTE_ADDRESS_FILTER_PROVIDER
      = new TestRemoteAddressFilterProvider("127.0.0.1");
  private final KeyTransparencyServiceClient keyTransparencyServiceClient = mock(KeyTransparencyServiceClient.class);
  private static final RateLimiters rateLimiters = mock(RateLimiters.class);
  private static final RateLimiter searchRatelimiter = mock(RateLimiter.class);
  private static final RateLimiter monitorRatelimiter = mock(RateLimiter.class);

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
    when(rateLimiters.forDescriptor(eq(RateLimiters.For.KEY_TRANSPARENCY_SEARCH_PER_IP))).thenReturn(searchRatelimiter);
    when(rateLimiters.forDescriptor(eq(RateLimiters.For.KEY_TRANSPARENCY_MONITOR_PER_IP))).thenReturn(
        monitorRatelimiter);
  }

  @AfterEach
  void teardown() {
    reset(rateLimiters,
        searchRatelimiter,
        monitorRatelimiter);
  }

  @Test
  void getFullSearchKey() {
    final byte[] charBytes = new byte[]{KeyTransparencyController.ACI_PREFIX};
    final byte[] aci = ACI.toCompactByteArray();

    final byte[] expectedFullSearchKey = new byte[aci.length + 1];
    System.arraycopy(charBytes, 0, expectedFullSearchKey, 0, charBytes.length);
    System.arraycopy(aci, 0, expectedFullSearchKey, charBytes.length, aci.length);

    assertArrayEquals(expectedFullSearchKey,
        KeyTransparencyController.getFullSearchKeyByteString(KeyTransparencyController.ACI_PREFIX, aci).toByteArray());
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  @ParameterizedTest
  @MethodSource
  void searchSuccess(final Optional<String> e164, final Optional<byte[]> usernameHash, final int expectedNumClientCalls) {
    when(keyTransparencyServiceClient.search(any(), any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(TestRandomUtil.nextBytes(16)));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/key-transparency/search")
        .request();

    final String searchJson = createSearchRequestJson(ACI, e164, usernameHash, Optional.of(3L), Optional.of(4L));
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

      verify(keyTransparencyServiceClient, times(expectedNumClientCalls)).search(any(), eq(Optional.of(3L)), eq(Optional.of(4L)),
          eq(KeyTransparencyController.KEY_TRANSPARENCY_RPC_TIMEOUT));
    }
  }

  private static Stream<Arguments> searchSuccess() {
    return Stream.of(
        Arguments.of(Optional.empty(), Optional.empty(), 1),
        Arguments.of(Optional.empty(), Optional.of(TestRandomUtil.nextBytes(20)), 2),
        Arguments.of(Optional.of(NUMBER), Optional.empty(), 2)
    );
  }

  @Test
  void searchAuthenticated() {
    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/key-transparency/search")
        .request()
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD));
    try (Response response = request.post(Entity.json(createSearchRequestJson(ACI, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty())))) {
      assertEquals(400, response.getStatus());
    }
    verify(keyTransparencyServiceClient, never()).search(any(), any(), any(), any());
  }

  @ParameterizedTest
  @MethodSource
  void searchGrpcErrors(final Status grpcStatus, final int httpStatus) {
    when(keyTransparencyServiceClient.search(any(), any(), any(), any()))
        .thenReturn(CompletableFuture.failedFuture(new CompletionException(new StatusRuntimeException(grpcStatus))));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/key-transparency/search")
        .request();
    try (Response response = request.post(Entity.json(createSearchRequestJson(ACI, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty())))) {
      assertEquals(httpStatus, response.getStatus());
      verify(keyTransparencyServiceClient, times(1)).search(any(), any(), any(), any());
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
      final Optional<Long> lastTreeHeadSize,
      final Optional<Long> distinguishedTreeHeadSize) {
    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/key-transparency/search")
        .request();
    try (Response response = request.post(Entity.json(
        createSearchRequestJson(aci, Optional.empty(), Optional.empty(), lastTreeHeadSize, distinguishedTreeHeadSize)))) {
      assertEquals(422, response.getStatus());
      verify(keyTransparencyServiceClient, never()).search(any(), any(), any(), any());
    }
  }

  private static Stream<Arguments> searchInvalidRequest() {
    return Stream.of(
        // ACI can't be null
        Arguments.of(null, Optional.empty(), Optional.empty()),
        // lastNonDistinguishedTreeHeadSize must be positive
        Arguments.of(ACI, Optional.of(0L), Optional.empty()),
        // lastDistinguishedTreeHeadSize must be positive
        Arguments.of(ACI, Optional.empty(), Optional.of(0L))
    );
  }

  @Test
  void searchRatelimited() {
    MockUtils.updateRateLimiterResponseToFail(
        rateLimiters, RateLimiters.For.KEY_TRANSPARENCY_SEARCH_PER_IP, "127.0.0.1", Duration.ofMinutes(10), true);
    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/key-transparency/search")
        .request();
    try (Response response = request.post(Entity.json(createSearchRequestJson(ACI, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty())))) {
      assertEquals(429, response.getStatus());
      verify(keyTransparencyServiceClient, never()).search(any(), any(), any(), any());
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
      verify(keyTransparencyServiceClient, never()).monitor(any(), any(), any(), any());
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
      verify(keyTransparencyServiceClient, never()).monitor(any(), any(), any(), any());
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
        Arguments.of(createMonitorRequestJson(ACI, List.of(4L), Optional.of(TestRandomUtil.nextBytes(20)),
            Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty())),
        // usernameHashPositions list cannot be empty
        Arguments.of(createMonitorRequestJson(ACI, List.of(4L), Optional.of(TestRandomUtil.nextBytes(20)),
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
  void monitorRatelimited() {
    MockUtils.updateRateLimiterResponseToFail(
        rateLimiters, RateLimiters.For.KEY_TRANSPARENCY_MONITOR_PER_IP, "127.0.0.1", Duration.ofMinutes(10), true);
    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/key-transparency/monitor")
        .request();
    try (Response response = request.post(
        Entity.json(createMonitorRequestJson(ACI, List.of(3L), Optional.empty(), Optional.empty(),
            Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty())))) {
      assertEquals(429, response.getStatus());
      verify(keyTransparencyServiceClient, never()).monitor(any(), any(), any(), any());
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
      final Optional<Long> lastTreeHeadSize,
      final Optional<Long> distinguishedTreeHeadSize) {
    final KeyTransparencySearchRequest request = new KeyTransparencySearchRequest(aci, e164, usernameHash, lastTreeHeadSize, distinguishedTreeHeadSize);
    try {
      return SystemMapper.jsonMapper().writeValueAsString(request);
    } catch (final JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }
  }
}
