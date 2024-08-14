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
import katie.FullTreeHead;
import katie.MonitorProof;
import katie.MonitorResponse;
import katie.SearchResponse;
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
import java.util.ArrayList;
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
import static org.mockito.Mockito.reset;
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
  void searchSuccess(final Optional<String> e164, final Optional<byte[]> usernameHash) {
    final SearchResponse searchResponse = SearchResponse.newBuilder().build();
    when(keyTransparencyServiceClient.search(any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(searchResponse));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/key-transparency/search")
        .request();
    try (Response response = request.post(Entity.json(createSearchRequestJson(ACI, e164, usernameHash)))) {
      assertEquals(200, response.getStatus());

      final KeyTransparencySearchResponse keyTransparencySearchResponse = response.readEntity(
          KeyTransparencySearchResponse.class);
      assertNotNull(keyTransparencySearchResponse.aciSearchResponse());

      usernameHash.ifPresentOrElse(
          ignored -> assertTrue(keyTransparencySearchResponse.usernameHashSearchResponse().isPresent()),
          () -> assertTrue(keyTransparencySearchResponse.usernameHashSearchResponse().isEmpty()));

      e164.ifPresentOrElse(ignored -> assertTrue(keyTransparencySearchResponse.e164SearchResponse().isPresent()),
          () -> assertTrue(keyTransparencySearchResponse.e164SearchResponse().isEmpty()));
    }
  }

  private static Stream<Arguments> searchSuccess() {
    return Stream.of(
        Arguments.of(Optional.empty(), Optional.empty()),
        Arguments.of(Optional.empty(), Optional.of(TestRandomUtil.nextBytes(20))),
        Arguments.of(Optional.of(NUMBER), Optional.empty())
    );
  }

  @Test
  void searchAuthenticated() {
    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/key-transparency/search")
        .request()
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD));
    try (Response response = request.post(Entity.json(createSearchRequestJson(ACI, Optional.empty(), Optional.empty())))) {
      assertEquals(400, response.getStatus());
    }
  }

  @ParameterizedTest
  @MethodSource
  void searchGrpcErrors(final Status grpcStatus, final int httpStatus) {
    when(keyTransparencyServiceClient.search(any(), any(), any()))
        .thenReturn(CompletableFuture.failedFuture(new CompletionException(new StatusRuntimeException(grpcStatus))));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/key-transparency/search")
        .request();
    try (Response response = request.post(Entity.json(createSearchRequestJson(ACI, Optional.empty(), Optional.empty())))) {
      assertEquals(httpStatus, response.getStatus());
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

  @Test
  void searchInvalidRequest() {
    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/key-transparency/search")
        .request();
    try (Response response = request.post(Entity.json(
        // ACI can't be null
        createSearchRequestJson(null, Optional.empty(), Optional.empty())))) {
      assertEquals(422, response.getStatus());
    }
  }

  @Test
  void searchRatelimited() {
    MockUtils.updateRateLimiterResponseToFail(
        rateLimiters, RateLimiters.For.KEY_TRANSPARENCY_SEARCH_PER_IP, "127.0.0.1", Duration.ofMinutes(10), true);
    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/key-transparency/search")
        .request();
    try (Response response = request.post(Entity.json(createSearchRequestJson(ACI, Optional.empty(), Optional.empty())))) {
      assertEquals(429, response.getStatus());
    }
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  @ParameterizedTest
  @MethodSource
  void monitorSuccess(
      final Optional<String> e164,
      final Optional<List<Long>> e164Positions,
      final Optional<byte[]> usernameHash,
      final Optional<List<Long>> usernameHashPositions) {
    final List<MonitorProof> monitorProofs = new ArrayList<>(List.of(MonitorProof.newBuilder().build()));
    e164.ifPresent(ignored -> monitorProofs.add(MonitorProof.newBuilder().build()));
    usernameHash.ifPresent(ignored -> monitorProofs.add(MonitorProof.newBuilder().build()));

    final MonitorResponse monitorResponse = MonitorResponse.newBuilder()
        .setTreeHead(FullTreeHead.newBuilder().build())
        .addAllContactProofs(monitorProofs)
        .build();

    when(keyTransparencyServiceClient.monitor(any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(monitorResponse));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/key-transparency/monitor")
        .request();

    try (Response response = request.post(Entity.json(
        createMonitorRequestJson(
            ACI, List.of(3L),
            usernameHash, usernameHashPositions,
            e164, e164Positions)))) {
      assertEquals(200, response.getStatus());

      final KeyTransparencyMonitorResponse keyTransparencyMonitorResponse = response.readEntity(
          KeyTransparencyMonitorResponse.class);
      assertNotNull(keyTransparencyMonitorResponse.aciMonitorProof());

      usernameHash.ifPresentOrElse(
          ignored -> assertTrue(keyTransparencyMonitorResponse.usernameHashMonitorProof().isPresent()),
          () -> assertTrue(keyTransparencyMonitorResponse.usernameHashMonitorProof().isEmpty()));

      e164.ifPresentOrElse(ignored -> assertTrue(keyTransparencyMonitorResponse.e164MonitorProof().isPresent()),
          () -> assertTrue(keyTransparencyMonitorResponse.e164MonitorProof().isEmpty()));
    }
  }

  private static Stream<Arguments> monitorSuccess() {
    return Stream.of(
        Arguments.of(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()),
        Arguments.of(Optional.empty(), Optional.empty(), Optional.of(TestRandomUtil.nextBytes(20)), Optional.of(List.of(3L))),
        Arguments.of(Optional.of(NUMBER), Optional.of(List.of(3L)), Optional.empty(), Optional.empty())
    );
  }

  @Test
  void monitorAuthenticated() {
    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/key-transparency/monitor")
        .request()
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD));
    try (Response response = request.post(
        Entity.json(createMonitorRequestJson(ACI, List.of(3L), Optional.empty(), Optional.empty(),
            Optional.empty(), Optional.empty())))) {
      assertEquals(400, response.getStatus());
    }
  }

  @ParameterizedTest
  @MethodSource
  void monitorGrpcErrors(final Status grpcStatus, final int httpStatus) {
    when(keyTransparencyServiceClient.monitor(any(), any(), any()))
        .thenReturn(CompletableFuture.failedFuture(new CompletionException(new StatusRuntimeException(grpcStatus))));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/key-transparency/monitor")
        .request();
    try (Response response = request.post(
        Entity.json(createMonitorRequestJson(ACI, List.of(3L), Optional.empty(), Optional.empty(),
            Optional.empty(), Optional.empty())))) {
      assertEquals(httpStatus, response.getStatus());
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
    }
  }

  private static Stream<Arguments> monitorInvalidRequest() {
    return Stream.of(
        // aci and aciPositions can't be empty
        Arguments.of(createMonitorRequestJson(null, null, Optional.empty(), Optional.empty(), Optional.empty(),
            Optional.empty())),
        // aciPositions list can't be empty
        Arguments.of(createMonitorRequestJson(ACI, Collections.emptyList(), Optional.empty(), Optional.empty(), Optional.empty(),
            Optional.empty())),
        // usernameHash cannot be empty if usernameHashPositions isn't
        Arguments.of(createMonitorRequestJson(ACI, List.of(4L), Optional.empty(), Optional.of(List.of(5L)),
            Optional.empty(), Optional.empty())),
        // usernameHashPosition cannot be empty if usernameHash isn't
        Arguments.of(createMonitorRequestJson(ACI, List.of(4L), Optional.of(TestRandomUtil.nextBytes(20)),
            Optional.empty(), Optional.empty(), Optional.empty())),
        // usernameHashPositions list cannot be empty
        Arguments.of(createMonitorRequestJson(ACI, List.of(4L), Optional.of(TestRandomUtil.nextBytes(20)),
            Optional.of(Collections.emptyList()), Optional.empty(), Optional.empty())),
        // e164 cannot be empty if e164Positions isn't
        Arguments.of(
            createMonitorRequestJson(ACI, List.of(4L), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.of(List.of(5L)))),
        // e164Positions cannot be empty if e164 isn't
        Arguments.of(createMonitorRequestJson(ACI, List.of(4L), Optional.empty(),
            Optional.empty(), Optional.of(NUMBER), Optional.empty())),
        // e164Positions list cannot empty
        Arguments.of(createMonitorRequestJson(ACI, List.of(4L), Optional.empty(),
            Optional.empty(), Optional.of(NUMBER), Optional.of(Collections.emptyList())))
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
            Optional.empty(), Optional.empty())))) {
      assertEquals(429, response.getStatus());
    }
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  /**
   * Create an invalid monitor request by supplying an invalid combination of inputs. For example, providing
   * a username hash but no corresponding list of positions.
   */
  private static String createMonitorRequestJson(
      final AciServiceIdentifier aci,
      final List<Long> aciPositions,
      final Optional<byte[]> usernameHash,
      final Optional<List<Long>> usernameHashPositions,
      final Optional<String> e164,
      final Optional<List<Long>> e164Positions) {
    final KeyTransparencyMonitorRequest request = new KeyTransparencyMonitorRequest(aci, aciPositions,
        e164, e164Positions, usernameHash, usernameHashPositions, Optional.empty());
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
      final Optional<byte[]> usernameHash) {
    final KeyTransparencySearchRequest request = new KeyTransparencySearchRequest(aci, e164, usernameHash, null);
    try {
      return SystemMapper.jsonMapper().writeValueAsString(request);
    } catch (final JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }
  }
}
