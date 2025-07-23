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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.net.HttpHeaders;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.Response;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import javax.annotation.Nullable;
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
import org.signal.keytransparency.client.CondensedTreeSearchResponse;
import org.signal.keytransparency.client.DistinguishedResponse;
import org.signal.keytransparency.client.E164SearchRequest;
import org.signal.keytransparency.client.FullTreeHead;
import org.signal.keytransparency.client.MonitorResponse;
import org.signal.keytransparency.client.SearchProof;
import org.signal.keytransparency.client.SearchResponse;
import org.signal.keytransparency.client.UpdateValue;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
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

  public static final String NUMBER = PhoneNumberUtil.getInstance().format(
      PhoneNumberUtil.getInstance().getExampleNumber("US"),
      PhoneNumberUtil.PhoneNumberFormat.E164);
  public static final AciServiceIdentifier ACI = new AciServiceIdentifier(UUID.randomUUID());
  public static final byte[] USERNAME_HASH = TestRandomUtil.nextBytes(20);
  private static final TestRemoteAddressFilterProvider TEST_REMOTE_ADDRESS_FILTER_PROVIDER
      = new TestRemoteAddressFilterProvider("127.0.0.1");
  public static final IdentityKey ACI_IDENTITY_KEY =  new IdentityKey(ECKeyPair.generate().getPublicKey());
  private static final byte[] COMMITMENT_INDEX = new byte[32];
  public static final byte[] UNIDENTIFIED_ACCESS_KEY = new byte[16];
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

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  @ParameterizedTest
  @MethodSource
  void searchSuccess(final Optional<String> e164, final Optional<byte[]> usernameHash) {
    final CondensedTreeSearchResponse aciSearchResponse = CondensedTreeSearchResponse.newBuilder()
        .setOpening(ByteString.copyFrom(TestRandomUtil.nextBytes(16)))
        .setSearch(SearchProof.getDefaultInstance())
        .setValue(UpdateValue.newBuilder()
            .setValue(ByteString.copyFrom(TestRandomUtil.nextBytes(16)))
            .build())
        .build();

    final SearchResponse.Builder searchResponseBuilder = SearchResponse.newBuilder()
        .setTreeHead(FullTreeHead.getDefaultInstance())
        .setAci(aciSearchResponse);

    e164.ifPresent(ignored -> searchResponseBuilder.setE164(CondensedTreeSearchResponse.getDefaultInstance()));
    usernameHash.ifPresent(ignored -> searchResponseBuilder.setUsernameHash(CondensedTreeSearchResponse.getDefaultInstance()));

    when(keyTransparencyServiceClient.search(any(), any(), any(), any(), any(), anyLong()))
        .thenReturn(searchResponseBuilder.build());

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/key-transparency/search")
        .request();

    final Optional<byte[]> unidentifiedAccessKey = e164.isPresent() ? Optional.of(UNIDENTIFIED_ACCESS_KEY) : Optional.empty();
    final String searchJson = createRequestJson(
        new KeyTransparencySearchRequest(ACI, e164, usernameHash, ACI_IDENTITY_KEY,
            unidentifiedAccessKey, Optional.of(3L), 4L));

    try (Response response = request.post(Entity.json(searchJson))) {
      assertEquals(200, response.getStatus());

      final KeyTransparencySearchResponse keyTransparencySearchResponse = response.readEntity(
          KeyTransparencySearchResponse.class);
      assertNotNull(keyTransparencySearchResponse.serializedResponse());
      assertEquals(aciSearchResponse, SearchResponse.parseFrom(keyTransparencySearchResponse.serializedResponse()).getAci());

      ArgumentCaptor<ByteString> aciArgument = ArgumentCaptor.forClass(ByteString.class);
      ArgumentCaptor<ByteString> aciIdentityKeyArgument = ArgumentCaptor.forClass(ByteString.class);
      ArgumentCaptor<Optional<ByteString>> usernameHashArgument = ArgumentCaptor.forClass(Optional.class);
      ArgumentCaptor<Optional<E164SearchRequest>> e164Argument = ArgumentCaptor.forClass(Optional.class);

      verify(keyTransparencyServiceClient).search(aciArgument.capture(), aciIdentityKeyArgument.capture(),
          usernameHashArgument.capture(), e164Argument.capture(), eq(Optional.of(3L)), eq(4L));

      assertArrayEquals(ACI.toCompactByteArray(), aciArgument.getValue().toByteArray());
      assertArrayEquals(ACI_IDENTITY_KEY.serialize(), aciIdentityKeyArgument.getValue().toByteArray());

      if (usernameHash.isPresent()) {
        assertArrayEquals(USERNAME_HASH, usernameHashArgument.getValue().orElseThrow().toByteArray());
      } else {
        assertTrue(usernameHashArgument.getValue().isEmpty());
      }

      if (e164.isPresent()) {
        final E164SearchRequest expected = E164SearchRequest.newBuilder()
            .setE164(e164.get())
            .setUnidentifiedAccessKey(ByteString.copyFrom(unidentifiedAccessKey.get()))
            .build();
        assertEquals(expected, e164Argument.getValue().orElseThrow());
      } else {
        assertTrue(e164Argument.getValue().isEmpty());
      }
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  private static Stream<Arguments> searchSuccess() {
    return Stream.of(
        Arguments.of(Optional.of(NUMBER), Optional.empty()),
        Arguments.of(Optional.empty(), Optional.of(USERNAME_HASH)),
        Arguments.of(Optional.of(NUMBER), Optional.of(USERNAME_HASH))
    );
  }

  @Test
  void searchAuthenticated() {
    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/key-transparency/search")
        .request()
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD));
    try (Response response = request.post(
        Entity.json(createRequestJson(new KeyTransparencySearchRequest(ACI, Optional.empty(), Optional.empty(),
            ACI_IDENTITY_KEY, Optional.empty(), Optional.empty(), 4L))))) {
      assertEquals(400, response.getStatus());
    }
    verifyNoInteractions(keyTransparencyServiceClient);
  }

  @ParameterizedTest
  @MethodSource
  void searchGrpcErrors(final Status grpcStatus, final int httpStatus) {
    when(keyTransparencyServiceClient.search(any(), any(), any(), any(), any(), anyLong()))
        .thenThrow(new StatusRuntimeException(grpcStatus));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/key-transparency/search")
        .request();
    try (Response response = request.post(
        Entity.json(createRequestJson(new KeyTransparencySearchRequest(ACI, Optional.empty(), Optional.empty(),
            ACI_IDENTITY_KEY, Optional.empty(), Optional.empty(), 4L))))) {
      assertEquals(httpStatus, response.getStatus());
      verify(keyTransparencyServiceClient, times(1)).search(any(), any(), any(), any(), any(), anyLong());
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
      final long distinguishedTreeHeadSize) {
    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/key-transparency/search")
        .request();
    try (Response response = request.post(Entity.json(
        createRequestJson(new KeyTransparencySearchRequest(aci, e164, Optional.empty(),
            aciIdentityKey, unidentifiedAccessKey, lastTreeHeadSize, distinguishedTreeHeadSize))))) {
      assertEquals(422, response.getStatus());
      verifyNoInteractions(keyTransparencyServiceClient);
    }
  }

  private static Stream<Arguments> searchInvalidRequest() {
    return Stream.of(
        // ACI can't be null
        Arguments.of(null, ACI_IDENTITY_KEY, Optional.empty(), Optional.empty(), Optional.empty(), 4L),
        // ACI identity key can't be null
        Arguments.of(ACI, null, Optional.empty(), Optional.empty(), Optional.empty(), 4L),
        // lastNonDistinguishedTreeHeadSize must be positive
        Arguments.of(ACI, ACI_IDENTITY_KEY, Optional.empty(), Optional.empty(), Optional.of(0L), 4L),
        // lastDistinguishedTreeHeadSize must be positive
        Arguments.of(ACI, ACI_IDENTITY_KEY, Optional.empty(), Optional.empty(), Optional.empty(), 0L),
        // E164 can't be provided without an unidentified access key
        Arguments.of(ACI, ACI_IDENTITY_KEY, Optional.of(NUMBER), Optional.empty(), Optional.empty(), 4L),
        // ...and an unidentified access key can't be provided without an E164
        Arguments.of(ACI, ACI_IDENTITY_KEY, Optional.empty(), Optional.of(UNIDENTIFIED_ACCESS_KEY), Optional.empty(), 4L)
        );
  }

  @Test
  void searchRateLimited() {
    MockUtils.updateRateLimiterResponseToFail(
        rateLimiters, RateLimiters.For.KEY_TRANSPARENCY_SEARCH_PER_IP, "127.0.0.1", Duration.ofMinutes(10));
    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/key-transparency/search")
        .request();
    try (Response response = request.post(
        Entity.json(createRequestJson(new KeyTransparencySearchRequest(ACI, Optional.empty(), Optional.empty(),
            ACI_IDENTITY_KEY, Optional.empty(), Optional.empty(), 4L))))) {
      assertEquals(429, response.getStatus());
      verifyNoInteractions(keyTransparencyServiceClient);
    }
  }

  @Test
  void monitorSuccess() {
    when(keyTransparencyServiceClient.monitor(any(), any(), any(), anyLong(), anyLong()))
        .thenReturn(MonitorResponse.getDefaultInstance());

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/key-transparency/monitor")
        .request();

    try (Response response = request.post(Entity.json(
        createRequestJson(
            new KeyTransparencyMonitorRequest(
                new KeyTransparencyMonitorRequest.AciMonitor(ACI,3, COMMITMENT_INDEX),
                Optional.empty(), Optional.empty(), 3L, 4L))))) {
      assertEquals(200, response.getStatus());

      final KeyTransparencyMonitorResponse keyTransparencyMonitorResponse = response.readEntity(
          KeyTransparencyMonitorResponse.class);
      assertNotNull(keyTransparencyMonitorResponse.serializedResponse());

      verify(keyTransparencyServiceClient, times(1)).monitor(
          any(), any(), any(), eq(3L), eq(4L));
    }
  }

  @Test
  void monitorAuthenticated() {
    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/key-transparency/monitor")
        .request()
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD));
    try (Response response = request.post(
        Entity.json(createRequestJson(
            new KeyTransparencyMonitorRequest(
                new KeyTransparencyMonitorRequest.AciMonitor(ACI, 3, COMMITMENT_INDEX),
                Optional.empty(), Optional.empty(), 3L, 4L))))) {
      assertEquals(400, response.getStatus());
      verifyNoInteractions(keyTransparencyServiceClient);
    }
  }

  @ParameterizedTest
  @MethodSource
  void monitorGrpcErrors(final Status grpcStatus, final int httpStatus) {
    when(keyTransparencyServiceClient.monitor(any(), any(), any(), anyLong(), anyLong()))
        .thenThrow(new StatusRuntimeException(grpcStatus));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/key-transparency/monitor")
        .request();
    try (Response response = request.post(
        Entity.json(createRequestJson(
            new KeyTransparencyMonitorRequest(
                new KeyTransparencyMonitorRequest.AciMonitor(ACI, 3, COMMITMENT_INDEX),
                Optional.empty(), Optional.empty(), 3L, 4L))))) {
      assertEquals(httpStatus, response.getStatus());
      verify(keyTransparencyServiceClient, times(1)).monitor(any(), any(), any(), anyLong(), anyLong());
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
        // aci monitor cannot be null
        Arguments.of(createRequestJson(
            new KeyTransparencyMonitorRequest(null, Optional.empty(), Optional.empty(), 3L, 4L))),
        // aci monitor fields can't be null
        Arguments.of(createRequestJson(
            new KeyTransparencyMonitorRequest(new KeyTransparencyMonitorRequest.AciMonitor(null, 4, null),
                Optional.empty(), Optional.empty(), 3L, 4L))),
        Arguments.of(createRequestJson(
            new KeyTransparencyMonitorRequest(
                new KeyTransparencyMonitorRequest.AciMonitor(null, 4, COMMITMENT_INDEX),
                Optional.empty(), Optional.empty(), 3L, 4L))),
        Arguments.of(createRequestJson(
            new KeyTransparencyMonitorRequest(new KeyTransparencyMonitorRequest.AciMonitor(ACI, 4, null),
                Optional.empty(), Optional.empty(), 3L, 4L))),
        // aciPosition must be positive
        Arguments.of(createRequestJson(new KeyTransparencyMonitorRequest(
            new KeyTransparencyMonitorRequest.AciMonitor(ACI, 0, COMMITMENT_INDEX),
            Optional.empty(), Optional.empty(), 3L, 4L))),
        // aci commitment index must be the correct size
        Arguments.of(createRequestJson(new KeyTransparencyMonitorRequest(
            new KeyTransparencyMonitorRequest.AciMonitor(ACI, 4, new byte[0]),
            Optional.empty(), Optional.empty(), 3L, 4L))),
        Arguments.of(createRequestJson(new KeyTransparencyMonitorRequest(
            new KeyTransparencyMonitorRequest.AciMonitor(ACI, 0, new byte[33]),
            Optional.empty(), Optional.empty(), 3L, 4L))),
        // username monitor fields cannot be null
        Arguments.of(createRequestJson(
            new KeyTransparencyMonitorRequest(
                new KeyTransparencyMonitorRequest.AciMonitor(ACI, 4, COMMITMENT_INDEX), Optional.empty(),
                Optional.of(new KeyTransparencyMonitorRequest.UsernameHashMonitor(null, 5, null)),
                3L, 4L))),
        Arguments.of(createRequestJson(
            new KeyTransparencyMonitorRequest(
                new KeyTransparencyMonitorRequest.AciMonitor(ACI, 4, COMMITMENT_INDEX), Optional.empty(),
                Optional.of(new KeyTransparencyMonitorRequest.UsernameHashMonitor(null, 5, COMMITMENT_INDEX)),
                3L, 4L))),
        Arguments.of(createRequestJson(
            new KeyTransparencyMonitorRequest(
                new KeyTransparencyMonitorRequest.AciMonitor(ACI, 4, COMMITMENT_INDEX), Optional.empty(),
                Optional.of(new KeyTransparencyMonitorRequest.UsernameHashMonitor(USERNAME_HASH, 5, null)),
                3L, 4L))),
        // usernameHashPosition must be positive
        Arguments.of(createRequestJson(
            new KeyTransparencyMonitorRequest(
                new KeyTransparencyMonitorRequest.AciMonitor(ACI, 4, COMMITMENT_INDEX),
                Optional.empty(),
                Optional.of(new KeyTransparencyMonitorRequest.UsernameHashMonitor(USERNAME_HASH,
                    0, COMMITMENT_INDEX)), 3L, 4L))),
        // username commitment index must be the correct size
        Arguments.of(createRequestJson(
            new KeyTransparencyMonitorRequest(
                new KeyTransparencyMonitorRequest.AciMonitor(ACI, 4, new byte[0]),
                Optional.empty(),
                Optional.of(new KeyTransparencyMonitorRequest.UsernameHashMonitor(USERNAME_HASH,
                    5, new byte[0])), 3L, 4L))),
        Arguments.of(createRequestJson(
            new KeyTransparencyMonitorRequest(new KeyTransparencyMonitorRequest.AciMonitor(ACI, 4, null),
                Optional.empty(),
                Optional.of(new KeyTransparencyMonitorRequest.UsernameHashMonitor(USERNAME_HASH,
                    5, new byte[33])), 3L, 4L))),
        // e164 fields cannot be null
        Arguments.of(
            createRequestJson(new KeyTransparencyMonitorRequest(
                new KeyTransparencyMonitorRequest.AciMonitor(ACI, 4, COMMITMENT_INDEX),
                Optional.of(new KeyTransparencyMonitorRequest.E164Monitor(null, 5, null)),
                Optional.empty(), 3L, 4L))),
        Arguments.of(
            createRequestJson(new KeyTransparencyMonitorRequest(
                new KeyTransparencyMonitorRequest.AciMonitor(ACI, 4, COMMITMENT_INDEX),
                Optional.of(new KeyTransparencyMonitorRequest.E164Monitor(null, 5, COMMITMENT_INDEX)),
                Optional.empty(), 3L, 4L))),
        Arguments.of(
            createRequestJson(new KeyTransparencyMonitorRequest(
                new KeyTransparencyMonitorRequest.AciMonitor(ACI, 4, COMMITMENT_INDEX),
                Optional.of(new KeyTransparencyMonitorRequest.E164Monitor(NUMBER, 5, null)),
                Optional.empty(), 3L, 4L))),
        // e164Position must be positive
        Arguments.of(createRequestJson(new KeyTransparencyMonitorRequest(
            new KeyTransparencyMonitorRequest.AciMonitor(ACI, 4, COMMITMENT_INDEX),
            Optional.of(
                new KeyTransparencyMonitorRequest.E164Monitor(NUMBER, 0, COMMITMENT_INDEX)),
            Optional.empty(), 3L, 4L))),
        // e164 commitment index must be the correct size
        Arguments.of(createRequestJson(new KeyTransparencyMonitorRequest(
            new KeyTransparencyMonitorRequest.AciMonitor(ACI, 4, COMMITMENT_INDEX),
            Optional.of(
                new KeyTransparencyMonitorRequest.E164Monitor(NUMBER, 5, new byte[0])),
            Optional.empty(), 3L, 4L))),
        Arguments.of(createRequestJson(new KeyTransparencyMonitorRequest(
            new KeyTransparencyMonitorRequest.AciMonitor(ACI, 4, COMMITMENT_INDEX),
            Optional.of(
                new KeyTransparencyMonitorRequest.E164Monitor(NUMBER, 5, new byte[33])),
            Optional.empty(), 3L, 4L))),
        // lastNonDistinguishedTreeHeadSize must be positive
        Arguments.of(createRequestJson(new KeyTransparencyMonitorRequest(
            new KeyTransparencyMonitorRequest.AciMonitor(ACI, 4, COMMITMENT_INDEX), Optional.empty(),
            Optional.empty(), 0L, 4L))),
        // lastDistinguishedTreeHeadSize must be positive
        Arguments.of(createRequestJson(new KeyTransparencyMonitorRequest(
            new KeyTransparencyMonitorRequest.AciMonitor(ACI, 4, COMMITMENT_INDEX), Optional.empty(),
            Optional.empty(), 3L, 0L)))
    );
  }

  @Test
  void monitorRateLimited() {
    MockUtils.updateRateLimiterResponseToFail(
        rateLimiters, RateLimiters.For.KEY_TRANSPARENCY_MONITOR_PER_IP, "127.0.0.1", Duration.ofMinutes(10));
    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/key-transparency/monitor")
        .request();
    try (Response response = request.post(
        Entity.json(createRequestJson(
            new KeyTransparencyMonitorRequest(new KeyTransparencyMonitorRequest.AciMonitor(ACI, 3, null),
                Optional.empty(), Optional.empty(),
                3L, 4L))))) {
      assertEquals(429, response.getStatus());
      verifyNoInteractions(keyTransparencyServiceClient);
    }
  }

  @ParameterizedTest
  @CsvSource(", 1")
  void distinguishedSuccess(@Nullable Long lastTreeHeadSize) {
    when(keyTransparencyServiceClient.getDistinguishedKey(any()))
        .thenReturn(DistinguishedResponse.getDefaultInstance());

    WebTarget webTarget = resources.getJerseyTest()
        .target("/v1/key-transparency/distinguished");

    if (lastTreeHeadSize != null) {
      webTarget = webTarget.queryParam("lastTreeHeadSize", lastTreeHeadSize);
    }

    try (Response response = webTarget.request().get()) {
      assertEquals(200, response.getStatus());

      final KeyTransparencyDistinguishedKeyResponse distinguishedKeyResponse = response.readEntity(
          KeyTransparencyDistinguishedKeyResponse.class);
      assertNotNull(distinguishedKeyResponse.serializedResponse());

      verify(keyTransparencyServiceClient, times(1))
          .getDistinguishedKey(eq(Optional.ofNullable(lastTreeHeadSize)));
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
    when(keyTransparencyServiceClient.getDistinguishedKey(any()))
        .thenThrow(new StatusRuntimeException(grpcStatus));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/key-transparency/distinguished")
        .request();
    try (Response response = request.get()) {
      assertEquals(httpStatus, response.getStatus());
      verify(keyTransparencyServiceClient).getDistinguishedKey(any());
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
    when(keyTransparencyServiceClient.getDistinguishedKey(any()))
        .thenReturn(DistinguishedResponse.getDefaultInstance());

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
        rateLimiters, RateLimiters.For.KEY_TRANSPARENCY_DISTINGUISHED_PER_IP, "127.0.0.1", Duration.ofMinutes(10)
    );
    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/key-transparency/distinguished")
        .request();
    try (Response response = request.get()) {
      assertEquals(429, response.getStatus());
      verifyNoInteractions(keyTransparencyServiceClient);
    }
  }

  private static String createRequestJson(final Object request) {
    try {
      return SystemMapper.jsonMapper().writeValueAsString(request);
    } catch (final JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }
  }
}
