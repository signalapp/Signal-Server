/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.signal.libsignal.zkgroup.ServerSecretParams;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.entities.CheckKeysRequest;
import org.whispersystems.textsecuregcm.entities.ECPreKey;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.entities.PreKeyCount;
import org.whispersystems.textsecuregcm.entities.PreKeyResponse;
import org.whispersystems.textsecuregcm.entities.PreKeyResponseItem;
import org.whispersystems.textsecuregcm.entities.SetKeysRequest;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.PniServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.mappers.CompletionExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.RateLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.ServerRejectedExceptionMapper;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.KeysManager;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.tests.util.KeysHelper;
import org.whispersystems.textsecuregcm.util.ByteArrayAdapter;
import org.whispersystems.textsecuregcm.util.CompletableFutureTestUtil;
import org.whispersystems.textsecuregcm.util.HeaderUtils;
import org.whispersystems.textsecuregcm.util.TestClock;

@ExtendWith(DropwizardExtensionsSupport.class)
class KeysControllerTest {

  private static final String EXISTS_NUMBER = "+14152222222";
  private static final UUID   EXISTS_UUID   = UUID.randomUUID();
  private static final UUID   EXISTS_PNI    = UUID.randomUUID();
  private static final AciServiceIdentifier EXISTS_ACI = new AciServiceIdentifier(EXISTS_UUID);
  private static final PniServiceIdentifier EXISTS_PNI_SERVICE_ID = new PniServiceIdentifier(EXISTS_PNI);

  private static final UUID   OTHER_UUID   = UUID.randomUUID();
  private static final AciServiceIdentifier OTHER_ACI = new AciServiceIdentifier(OTHER_UUID);

  private static final UUID   NOT_EXISTS_UUID   = UUID.randomUUID();
  private static final AciServiceIdentifier NOT_EXISTS_ACI = new AciServiceIdentifier(NOT_EXISTS_UUID);

  private static final byte SAMPLE_DEVICE_ID = 1;

  private static final int SAMPLE_REGISTRATION_ID  =  999;
  private static final int SAMPLE_PNI_REGISTRATION_ID = 1717;

  private final ECKeyPair IDENTITY_KEY_PAIR = ECKeyPair.generate();
  private final IdentityKey IDENTITY_KEY = new IdentityKey(IDENTITY_KEY_PAIR.getPublicKey());

  private final ECKeyPair PNI_IDENTITY_KEY_PAIR = ECKeyPair.generate();
  private final IdentityKey PNI_IDENTITY_KEY = new IdentityKey(PNI_IDENTITY_KEY_PAIR.getPublicKey());

  private final ECPreKey SAMPLE_KEY = KeysHelper.ecPreKey(1234);
  private final ECPreKey SAMPLE_KEY_PNI = KeysHelper.ecPreKey(7777);

  private final KEMSignedPreKey SAMPLE_PQ_KEY = KeysHelper.signedKEMPreKey(2424, ECKeyPair.generate());
  private final KEMSignedPreKey SAMPLE_PQ_KEY_PNI = KeysHelper.signedKEMPreKey(8888, ECKeyPair.generate());

  private final ECSignedPreKey SAMPLE_SIGNED_KEY = KeysHelper.signedECPreKey(1111, IDENTITY_KEY_PAIR);
  private final ECSignedPreKey SAMPLE_SIGNED_PNI_KEY = KeysHelper.signedECPreKey(5555, PNI_IDENTITY_KEY_PAIR);


  private final static KeysManager KEYS = mock(KeysManager.class);
  private final static AccountsManager accounts = mock(AccountsManager.class);
  private final static Account existsAccount = mock(Account.class);

  private static final RateLimiters rateLimiters = mock(RateLimiters.class);
  private static final RateLimiter rateLimiter = mock(RateLimiter.class);

  private static final ServerSecretParams serverSecretParams = ServerSecretParams.generate();

  private static final TestClock clock = TestClock.now();

  private static final ResourceExtension resources = ResourceExtension.builder()
      .addProperty(ServerProperties.UNWRAP_COMPLETION_STAGE_IN_WRITER_ENABLE, Boolean.TRUE)
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(CompletionExceptionMapper.class)
      .addProvider(new AuthValueFactoryProvider.Binder<>(AuthenticatedDevice.class))
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(new ServerRejectedExceptionMapper())
      .addResource(new KeysController(rateLimiters, KEYS, accounts, serverSecretParams, clock))
      .addResource(new RateLimitExceededExceptionMapper())
      .build();

  private record WeaklyTypedPreKey(long keyId,

                                   @JsonSerialize(using = ByteArrayAdapter.Serializing.class)
                                   @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
                                   byte[] publicKey) {
  }

  private record WeaklyTypedSignedPreKey(long keyId,

                                         @JsonSerialize(using = ByteArrayAdapter.Serializing.class)
                                         @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
                                         byte[] publicKey,

                                         @JsonSerialize(using = ByteArrayAdapter.Serializing.class)
                                         @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
                                         byte[] signature) {

    static WeaklyTypedSignedPreKey fromSignedPreKey(final SignedPreKey<?> signedPreKey) {
      return new WeaklyTypedSignedPreKey(signedPreKey.keyId(), signedPreKey.serializedPublicKey(), signedPreKey.signature());
    }
  }

  private record WeaklyTypedPreKeyState(List<WeaklyTypedPreKey> preKeys,
                                        WeaklyTypedSignedPreKey signedPreKey,
                                        List<WeaklyTypedSignedPreKey> pqPreKeys,
                                        WeaklyTypedSignedPreKey pqLastResortPreKey,

                                        @JsonSerialize(using = ByteArrayAdapter.Serializing.class)
                                        @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
                                        byte[] identityKey) {
  }

  private Device createSampleDevice(byte deviceId, int registrationId, int pniRegistrationId) {
    final Device sampleDevice = mock(Device.class);
    when(sampleDevice.getRegistrationId(IdentityType.ACI)).thenReturn(registrationId);
    when(sampleDevice.getRegistrationId(IdentityType.PNI)).thenReturn(pniRegistrationId);
    when(sampleDevice.getId()).thenReturn(deviceId);

    return sampleDevice;
  }

  @BeforeEach
  void setup() {
    clock.unpin();

    AccountsHelper.setupMockUpdate(accounts);

    final Device sampleDevice =
        createSampleDevice(SAMPLE_DEVICE_ID, SAMPLE_REGISTRATION_ID, SAMPLE_PNI_REGISTRATION_ID);

    final KeysManager.DevicePreKeys aciKeys =
        new KeysManager.DevicePreKeys(SAMPLE_SIGNED_KEY, Optional.of(SAMPLE_KEY), SAMPLE_PQ_KEY);
    final KeysManager.DevicePreKeys pniKeys =
        new KeysManager.DevicePreKeys(SAMPLE_SIGNED_PNI_KEY, Optional.of(SAMPLE_KEY_PNI), SAMPLE_PQ_KEY_PNI);

    when(KEYS.takeDevicePreKeys(eq(SAMPLE_DEVICE_ID), eq(EXISTS_ACI), any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(aciKeys)));
    when(KEYS.takeDevicePreKeys(eq(SAMPLE_DEVICE_ID), eq(EXISTS_PNI_SERVICE_ID), any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(pniKeys)));

    when(existsAccount.getUuid()).thenReturn(EXISTS_UUID);
    when(existsAccount.isIdentifiedBy(new AciServiceIdentifier(EXISTS_UUID))).thenReturn(true);
    when(existsAccount.getPhoneNumberIdentifier()).thenReturn(EXISTS_PNI);
    when(existsAccount.isIdentifiedBy(new PniServiceIdentifier(EXISTS_PNI))).thenReturn(true);
    when(existsAccount.getIdentifier(IdentityType.ACI)).thenReturn(EXISTS_UUID);
    when(existsAccount.getIdentifier(IdentityType.PNI)).thenReturn(EXISTS_PNI);
    when(existsAccount.getDevice(SAMPLE_DEVICE_ID)).thenReturn(Optional.of(sampleDevice));
    when(existsAccount.getDevices()).thenReturn(List.of(sampleDevice));
    when(existsAccount.getIdentityKey(IdentityType.ACI)).thenReturn(IDENTITY_KEY);
    when(existsAccount.getIdentityKey(IdentityType.PNI)).thenReturn(PNI_IDENTITY_KEY);
    when(existsAccount.getNumber()).thenReturn(EXISTS_NUMBER);
    when(existsAccount.getUnidentifiedAccessKey()).thenReturn(Optional.of("1337".getBytes()));

    when(accounts.getByServiceIdentifier(any())).thenReturn(Optional.empty());
    when(accounts.getByServiceIdentifierAsync(any())).thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    when(accounts.getByServiceIdentifier(new AciServiceIdentifier(EXISTS_UUID))).thenReturn(Optional.of(existsAccount));
    when(accounts.getByServiceIdentifier(new PniServiceIdentifier(EXISTS_PNI))).thenReturn(Optional.of(existsAccount));

    when(accounts.getByServiceIdentifierAsync(new AciServiceIdentifier(EXISTS_UUID)))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(existsAccount)));

    when(accounts.getByServiceIdentifierAsync(new PniServiceIdentifier(EXISTS_PNI)))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(existsAccount)));

    when(accounts.getByAccountIdentifier(AuthHelper.VALID_UUID)).thenReturn(Optional.of(AuthHelper.VALID_ACCOUNT));
    when(accounts.getByAccountIdentifierAsync(AuthHelper.VALID_UUID))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(AuthHelper.VALID_ACCOUNT)));

    when(rateLimiters.getPreKeysLimiter()).thenReturn(rateLimiter);

    when(KEYS.storeEcOneTimePreKeys(any(), anyByte(), any()))
        .thenReturn(CompletableFutureTestUtil.almostCompletedFuture(null));
    when(KEYS.storeKemOneTimePreKeys(any(), anyByte(), any()))
        .thenReturn(CompletableFutureTestUtil.almostCompletedFuture(null));
    when(KEYS.storePqLastResort(any(), anyByte(), any()))
        .thenReturn(CompletableFutureTestUtil.almostCompletedFuture(null));
    when(KEYS.storeEcSignedPreKeys(any(), anyByte(), any()))
        .thenReturn(CompletableFutureTestUtil.almostCompletedFuture(null));
  }

  @AfterEach
  void teardown() {
    reset(
        KEYS,
        accounts,
        existsAccount,
        rateLimiters,
        rateLimiter
    );

    clearInvocations(AuthHelper.VALID_DEVICE);
  }

  @Test
  void validKeyStatusTest() {
    when(KEYS.getEcCount(AuthHelper.VALID_UUID, SAMPLE_DEVICE_ID)).thenReturn(CompletableFuture.completedFuture(5));
    when(KEYS.getPqCount(AuthHelper.VALID_UUID, SAMPLE_DEVICE_ID)).thenReturn(CompletableFuture.completedFuture(5));
    when(KEYS.getEcSignedPreKey(AuthHelper.VALID_UUID, AuthHelper.VALID_DEVICE.getId()))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(KeysHelper.signedECPreKey(123, IDENTITY_KEY_PAIR))));

    PreKeyCount result = resources.getJerseyTest()
                                  .target("/v2/keys")
                                  .request()
                                  .header("Authorization",
                                          AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                                  .get(PreKeyCount.class);

    assertThat(result.getCount()).isEqualTo(5);
    assertThat(result.getPqCount()).isEqualTo(5);

    verify(KEYS).getEcCount(AuthHelper.VALID_UUID, SAMPLE_DEVICE_ID);
    verify(KEYS).getPqCount(AuthHelper.VALID_UUID, SAMPLE_DEVICE_ID);
  }

  @Test
  void validSingleRequestTestV2() {
    PreKeyResponse result = resources.getJerseyTest()
        .target(String.format("/v2/keys/%s/1", EXISTS_UUID))
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(PreKeyResponse.class);

    assertThat(result.getIdentityKey()).isEqualTo(existsAccount.getIdentityKey(IdentityType.ACI));
    assertThat(result.getDevicesCount()).isEqualTo(1);
    assertEquals(SAMPLE_KEY, result.getDevice(SAMPLE_DEVICE_ID).getPreKey());
    assertThat(result.getDevice(SAMPLE_DEVICE_ID).getPqPreKey()).isEqualTo(SAMPLE_PQ_KEY);
    assertThat(result.getDevice(SAMPLE_DEVICE_ID).getRegistrationId()).isEqualTo(SAMPLE_REGISTRATION_ID);
    assertEquals(SAMPLE_SIGNED_KEY, result.getDevice(SAMPLE_DEVICE_ID).getSignedPreKey());

    verify(KEYS).takeDevicePreKeys(eq(SAMPLE_DEVICE_ID), eq(EXISTS_ACI), any());
    verifyNoMoreInteractions(KEYS);
  }

  @Test
  void validSingleRequestPqTestV2() {
    PreKeyResponse result = resources.getJerseyTest()
        .target(String.format("/v2/keys/%s/1", EXISTS_UUID))
        .queryParam("pq", "true")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(PreKeyResponse.class);

    assertThat(result.getIdentityKey()).isEqualTo(existsAccount.getIdentityKey(IdentityType.ACI));
    assertThat(result.getDevicesCount()).isEqualTo(1);
    assertEquals(SAMPLE_KEY, result.getDevice(SAMPLE_DEVICE_ID).getPreKey());
    assertEquals(SAMPLE_PQ_KEY, result.getDevice(SAMPLE_DEVICE_ID).getPqPreKey());
    assertThat(result.getDevice(SAMPLE_DEVICE_ID).getRegistrationId()).isEqualTo(SAMPLE_REGISTRATION_ID);
    assertEquals(SAMPLE_SIGNED_KEY, result.getDevice(SAMPLE_DEVICE_ID).getSignedPreKey());

    verify(KEYS).takeDevicePreKeys(eq(SAMPLE_DEVICE_ID), eq(EXISTS_ACI), any());
    verifyNoMoreInteractions(KEYS);
  }

  @Test
  void validSingleRequestByPhoneNumberIdentifierTestV2() {
    PreKeyResponse result = resources.getJerseyTest()
        .target(String.format("/v2/keys/PNI:%s/1", EXISTS_PNI))
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(PreKeyResponse.class);

    assertThat(result.getIdentityKey()).isEqualTo(existsAccount.getIdentityKey(IdentityType.PNI));
    assertThat(result.getDevicesCount()).isEqualTo(1);
    assertEquals(SAMPLE_KEY_PNI, result.getDevice(SAMPLE_DEVICE_ID).getPreKey());
    assertThat(result.getDevice(SAMPLE_DEVICE_ID).getPqPreKey()).isEqualTo(SAMPLE_PQ_KEY_PNI);
    assertThat(result.getDevice(SAMPLE_DEVICE_ID).getRegistrationId()).isEqualTo(SAMPLE_PNI_REGISTRATION_ID);
    assertEquals(SAMPLE_SIGNED_PNI_KEY, result.getDevice(SAMPLE_DEVICE_ID).getSignedPreKey());

    verify(KEYS).takeDevicePreKeys(eq(SAMPLE_DEVICE_ID), eq(EXISTS_PNI_SERVICE_ID), any());
    verifyNoMoreInteractions(KEYS);
  }

  @Test
  void validSingleRequestPqByPhoneNumberIdentifierTestV2() {
    PreKeyResponse result = resources.getJerseyTest()
        .target(String.format("/v2/keys/PNI:%s/1", EXISTS_PNI))
        .queryParam("pq", "true")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(PreKeyResponse.class);

    assertThat(result.getIdentityKey()).isEqualTo(existsAccount.getIdentityKey(IdentityType.PNI));
    assertThat(result.getDevicesCount()).isEqualTo(1);
    assertEquals(SAMPLE_KEY_PNI, result.getDevice(SAMPLE_DEVICE_ID).getPreKey());
    assertThat(result.getDevice(SAMPLE_DEVICE_ID).getPqPreKey()).isEqualTo(SAMPLE_PQ_KEY_PNI);
    assertThat(result.getDevice(SAMPLE_DEVICE_ID).getRegistrationId()).isEqualTo(SAMPLE_PNI_REGISTRATION_ID);
    assertEquals(SAMPLE_SIGNED_PNI_KEY, result.getDevice(SAMPLE_DEVICE_ID).getSignedPreKey());

    verify(KEYS).takeDevicePreKeys(eq(SAMPLE_DEVICE_ID), eq(EXISTS_PNI_SERVICE_ID), any());
    verifyNoMoreInteractions(KEYS);
  }

  @Test
  void testGetKeysRateLimited() throws RateLimitExceededException {
    Duration retryAfter = Duration.ofSeconds(31);
    doThrow(new RateLimitExceededException(retryAfter)).when(rateLimiter).validate(anyString());

    Response result = resources.getJerseyTest()
        .target(String.format("/v2/keys/PNI:%s/*", EXISTS_PNI))
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get();

    final String expectedRatelimitKey = String.format("%s.%s__%s.%s.%s",
        AuthHelper.VALID_UUID,
        AuthHelper.VALID_DEVICE.getId(),
        EXISTS_PNI,
        "*",
        "*");

    assertThat(result.getStatus()).isEqualTo(429);
    assertThat(result.getHeaderString("Retry-After")).isEqualTo(String.valueOf(retryAfter.toSeconds()));
    verify(rateLimiter).validate(expectedRatelimitKey);
  }

  @Test
  void testUnidentifiedRequest() {
    PreKeyResponse result = resources.getJerseyTest()
        .target(String.format("/v2/keys/%s/1", EXISTS_UUID))
        .queryParam("pq", "true")
        .request()
        .header(HeaderUtils.UNIDENTIFIED_ACCESS_KEY, AuthHelper.getUnidentifiedAccessHeader("1337".getBytes()))
        .get(PreKeyResponse.class);

    assertThat(result.getIdentityKey()).isEqualTo(existsAccount.getIdentityKey(IdentityType.ACI));
    assertThat(result.getDevicesCount()).isEqualTo(1);
    assertEquals(SAMPLE_KEY, result.getDevice(SAMPLE_DEVICE_ID).getPreKey());
    assertEquals(SAMPLE_PQ_KEY, result.getDevice(SAMPLE_DEVICE_ID).getPqPreKey());
    assertEquals(SAMPLE_SIGNED_KEY, result.getDevice(SAMPLE_DEVICE_ID).getSignedPreKey());

    verify(KEYS).takeDevicePreKeys(eq(SAMPLE_DEVICE_ID), eq(EXISTS_ACI), any());
    verifyNoMoreInteractions(KEYS);
  }

  @Test
  void testNoKeysForDevice() {
    final List<Device> devices = List.of(
        createSampleDevice((byte) 1, 2, 3),
        createSampleDevice((byte) 4, 5, 6));
    // device 1 is missing required prekeys, device 4 is missing an optional EC prekey
    when(KEYS.takeDevicePreKeys(eq((byte) 4), eq(EXISTS_PNI_SERVICE_ID), any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(
            new KeysManager.DevicePreKeys(SAMPLE_SIGNED_PNI_KEY, Optional.empty(), SAMPLE_PQ_KEY_PNI))));
    when(KEYS.takeDevicePreKeys(eq((byte) 1), eq(EXISTS_PNI_SERVICE_ID), any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    when(existsAccount.getDevice((byte) 1)).thenReturn(Optional.of(devices.get(0)));
    when(existsAccount.getDevice((byte) 4)).thenReturn(Optional.of(devices.get(1)));
    when(existsAccount.getDevices()).thenReturn(devices);

    PreKeyResponse results = resources.getJerseyTest()
        .target(String.format("/v2/keys/PNI:%s/*", EXISTS_PNI))
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(PreKeyResponse.class);

    // Should drop device 1 and keep device 4
    assertThat(results.getDevicesCount()).isEqualTo(1);
    final PreKeyResponseItem result = results.getDevice((byte) 4);
    assertEquals(6, result.getRegistrationId());
    assertNull(result.getPreKey());
    assertEquals(SAMPLE_SIGNED_PNI_KEY, result.getSignedPreKey());
    assertEquals(SAMPLE_PQ_KEY_PNI, result.getPqPreKey());
  }

  @ParameterizedTest
  @MethodSource
  void testGetKeysWithGroupSendEndorsement(
      ServiceIdentifier target, ServiceIdentifier authorizedTarget, Duration timeLeft, boolean includeUak, int expectedResponse) throws Exception {

    final Instant expiration = Instant.now().truncatedTo(ChronoUnit.DAYS);
    clock.pin(expiration.minus(timeLeft));

    Invocation.Builder builder = resources.getJerseyTest()
        .target(String.format("/v2/keys/%s/1", target.toServiceIdentifierString()))
        .queryParam("pq", "true")
        .request()
        .header(HeaderUtils.GROUP_SEND_TOKEN, AuthHelper.validGroupSendTokenHeader(serverSecretParams, List.of(authorizedTarget), expiration));

    if (includeUak) {
      builder = builder.header(HeaderUtils.UNIDENTIFIED_ACCESS_KEY, AuthHelper.getUnidentifiedAccessHeader("1337".getBytes()));
    }

    Response response = builder.get();
    assertThat(response.getStatus()).isEqualTo(expectedResponse);

    if (expectedResponse == 200) {
      PreKeyResponse result = response.readEntity(PreKeyResponse.class);

      assertThat(result.getIdentityKey()).isEqualTo(existsAccount.getIdentityKey(IdentityType.ACI));
      assertThat(result.getDevicesCount()).isEqualTo(1);
      assertEquals(SAMPLE_KEY, result.getDevice(SAMPLE_DEVICE_ID).getPreKey());
      assertEquals(SAMPLE_PQ_KEY, result.getDevice(SAMPLE_DEVICE_ID).getPqPreKey());
      assertEquals(SAMPLE_SIGNED_KEY, result.getDevice(SAMPLE_DEVICE_ID).getSignedPreKey());

      verify(KEYS).takeDevicePreKeys(eq(SAMPLE_DEVICE_ID), eq(EXISTS_ACI), any());
    }

    verifyNoMoreInteractions(KEYS);
  }

  private static Stream<Arguments> testGetKeysWithGroupSendEndorsement() {
    return Stream.of(
        // valid endorsement
        Arguments.of(EXISTS_ACI, EXISTS_ACI, Duration.ofHours(1), false, 200),

        // expired endorsement, not authorized
        Arguments.of(EXISTS_ACI, EXISTS_ACI, Duration.ofHours(-1), false, 401),

        // endorsement for the wrong recipient, not authorized
        Arguments.of(EXISTS_ACI, OTHER_ACI, Duration.ofHours(1), false, 401),

        // expired endorsement for the wrong recipient, not authorized
        Arguments.of(EXISTS_ACI, OTHER_ACI, Duration.ofHours(-1), false, 401),

        // valid endorsement for the right recipient but they aren't registered, not found
        Arguments.of(NOT_EXISTS_ACI, NOT_EXISTS_ACI, Duration.ofHours(1), false, 404),

        // expired endorsement for the right recipient but they aren't registered, not authorized (NOT not found)
        Arguments.of(NOT_EXISTS_ACI, NOT_EXISTS_ACI, Duration.ofHours(-1), false, 401),

        // valid endorsement but also a UAK, bad request
        Arguments.of(EXISTS_ACI, EXISTS_ACI, Duration.ofHours(1), true, 400));
  }

  @Test
  void testNoDevices() {

    when(existsAccount.getDevices()).thenReturn(Collections.emptyList());

    Response result = resources.getJerseyTest()
        .target(String.format("/v2/keys/%s/*", EXISTS_UUID))
        .request()
        .header(HeaderUtils.UNIDENTIFIED_ACCESS_KEY, AuthHelper.getUnidentifiedAccessHeader("1337".getBytes()))
        .get();

    assertThat(result).isNotNull();
    assertThat(result.getStatus()).isEqualTo(404);
  }

  @Test
  void testUnauthorizedUnidentifiedRequest() {
    Response response = resources.getJerseyTest()
                                     .target(String.format("/v2/keys/%s/1", EXISTS_UUID))
                                     .request()
                                     .header(HeaderUtils.UNIDENTIFIED_ACCESS_KEY, AuthHelper.getUnidentifiedAccessHeader("9999".getBytes()))
                                     .get();

    assertThat(response.getStatus()).isEqualTo(401);
    verifyNoMoreInteractions(KEYS);
  }

  @Test
  void testMalformedUnidentifiedRequest() {
    Response response = resources.getJerseyTest()
                                 .target(String.format("/v2/keys/%s/1", EXISTS_UUID))
                                 .request()
                                 .header(HeaderUtils.UNIDENTIFIED_ACCESS_KEY, "$$$$$$$$$")
                                 .get();

    assertThat(response.getStatus()).isEqualTo(401);
    verifyNoMoreInteractions(KEYS);
  }


  @ParameterizedTest
  @EnumSource
  void validMultiRequestTestV2(IdentityType identityType) {
    final ServiceIdentifier serviceIdentifier = switch (identityType) {
      case ACI -> new AciServiceIdentifier(EXISTS_UUID);
      case PNI -> new PniServiceIdentifier(EXISTS_PNI);
    };

    final List<Device> devices = new ArrayList<>();
    final List<KeysManager.DevicePreKeys> devicePreKeys = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      devices.add(createSampleDevice((byte) i, i + 100, i + 200));

      final ECSignedPreKey signedEcPreKey = KeysHelper.signedECPreKey(i + 300, IDENTITY_KEY_PAIR);
      final ECPreKey ecPreKey = KeysHelper.ecPreKey(i + 400);
      final KEMSignedPreKey kemSignedPreKey = KeysHelper.signedKEMPreKey(i + 500, ECKeyPair.generate());
      devicePreKeys.add(new KeysManager.DevicePreKeys(signedEcPreKey, Optional.of(ecPreKey), kemSignedPreKey));

      when(existsAccount.getDevice((byte) i)).thenReturn(Optional.of(devices.getLast()));
      when(KEYS.takeDevicePreKeys(eq((byte) i), eq(serviceIdentifier), any()))
          .thenReturn(CompletableFuture.completedFuture(Optional.of(devicePreKeys.getLast())));
    }
    when(existsAccount.getDevices()).thenReturn(devices);

    PreKeyResponse results = resources.getJerseyTest()
        .target(String.format("/v2/keys/%s/*", serviceIdentifier.toServiceIdentifierString()))
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(PreKeyResponse.class);

    assertThat(results.getDevicesCount()).isEqualTo(4);
    assertThat(results.getIdentityKey()).isEqualTo(existsAccount.getIdentityKey(identityType));

    for (int i = 0; i < 4; i++) {
      final PreKeyResponseItem result = results.getDevice((byte) i);
      final KeysManager.DevicePreKeys expectedPreKeys = devicePreKeys.get(i);
      final Device expectedDevice = devices.get(i);

      assertEquals(expectedDevice.getRegistrationId(identityType), result.getRegistrationId());
      assertEquals(expectedPreKeys.ecPreKey().orElseThrow(), result.getPreKey());
      assertEquals(expectedPreKeys.ecSignedPreKey(), result.getSignedPreKey());
      assertEquals(expectedPreKeys.kemSignedPreKey(), result.getPqPreKey());

      verify(KEYS).takeDevicePreKeys(eq((byte) i), eq(serviceIdentifier), any());
    }
    verifyNoMoreInteractions(KEYS);
  }

  @Test
  void invalidRequestTestV2() {
    Response response = resources.getJerseyTest()
                                 .target(String.format("/v2/keys/%s", NOT_EXISTS_UUID))
                                 .request()
                                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                                 .get();

    assertThat(response.getStatusInfo().getStatusCode()).isEqualTo(404);
  }

  @Test
  void anotherInvalidRequestTestV2() {
    when(existsAccount.getDevice((byte) 22)).thenReturn(Optional.empty());
    Response response = resources.getJerseyTest()
                                 .target(String.format("/v2/keys/%s/22", EXISTS_UUID))
                                 .request()
                                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                                 .get();

    assertThat(response.getStatusInfo().getStatusCode()).isEqualTo(404);
  }

  @Test
  void unauthorizedRequestTestV2() {
    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v2/keys/%s/1", EXISTS_UUID))
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.INVALID_PASSWORD))
                 .get();

    assertThat(response.getStatusInfo().getStatusCode()).isEqualTo(401);

    response =
        resources.getJerseyTest()
                 .target(String.format("/v2/keys/%s/1", EXISTS_UUID))
                 .request()
                 .get();

    assertThat(response.getStatusInfo().getStatusCode()).isEqualTo(401);
  }

  @Test
  void putKeysTestV2() {
    final ECPreKey preKey = KeysHelper.ecPreKey(31337);
    final ECSignedPreKey signedPreKey = KeysHelper.signedECPreKey(31338, AuthHelper.VALID_IDENTITY_KEY_PAIR);

    final SetKeysRequest setKeysRequest = new SetKeysRequest(List.of(preKey), signedPreKey, null, null);

    Response response =
        resources.getJerseyTest()
                 .target("/v2/keys")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                 .put(Entity.entity(setKeysRequest, MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(204);

    ArgumentCaptor<List<ECPreKey>> listCaptor = ArgumentCaptor.forClass(List.class);

    verify(KEYS).storeEcOneTimePreKeys(eq(AuthHelper.VALID_UUID), eq(SAMPLE_DEVICE_ID), listCaptor.capture());

    assertThat(listCaptor.getValue()).containsExactly(preKey);

    verify(KEYS).storeEcSignedPreKeys(AuthHelper.VALID_UUID, AuthHelper.VALID_DEVICE.getId(), signedPreKey);
  }

  @Test
  void putKeysTestV2EmptySingleUseKeysList() {
    final ECSignedPreKey signedPreKey = KeysHelper.signedECPreKey(31338, AuthHelper.VALID_IDENTITY_KEY_PAIR);

    final SetKeysRequest setKeysRequest = new SetKeysRequest(List.of(), signedPreKey, List.of(), null);

    try (final Response response =
        resources.getJerseyTest()
            .target("/v2/keys")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(setKeysRequest, MediaType.APPLICATION_JSON_TYPE))) {

      assertThat(response.getStatus()).isEqualTo(204);

      verify(KEYS, never()).storeEcOneTimePreKeys(any(), anyByte(), any());
      verify(KEYS, never()).storeKemOneTimePreKeys(any(), anyByte(), any());
      verify(KEYS).storeEcSignedPreKeys(AuthHelper.VALID_UUID, AuthHelper.VALID_DEVICE.getId(), signedPreKey);
    }
  }

  @Test
  void putKeysPqTestV2() {
    final ECPreKey preKey = KeysHelper.ecPreKey(31337);
    final ECSignedPreKey signedPreKey = KeysHelper.signedECPreKey(31338, AuthHelper.VALID_IDENTITY_KEY_PAIR);
    final KEMSignedPreKey pqPreKey = KeysHelper.signedKEMPreKey(31339, AuthHelper.VALID_IDENTITY_KEY_PAIR);
    final KEMSignedPreKey pqLastResortPreKey = KeysHelper.signedKEMPreKey(31340, AuthHelper.VALID_IDENTITY_KEY_PAIR);

    final SetKeysRequest setKeysRequest =
        new SetKeysRequest(List.of(preKey), signedPreKey, List.of(pqPreKey), pqLastResortPreKey);

    Response response =
        resources.getJerseyTest()
                 .target("/v2/keys")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                 .put(Entity.entity(setKeysRequest, MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(204);

    ArgumentCaptor<List<ECPreKey>> ecCaptor = ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<List<KEMSignedPreKey>> pqCaptor = ArgumentCaptor.forClass(List.class);
    verify(KEYS).storeEcOneTimePreKeys(eq(AuthHelper.VALID_UUID), eq(SAMPLE_DEVICE_ID), ecCaptor.capture());
    verify(KEYS).storeKemOneTimePreKeys(eq(AuthHelper.VALID_UUID), eq(SAMPLE_DEVICE_ID), pqCaptor.capture());
    verify(KEYS).storePqLastResort(AuthHelper.VALID_UUID, SAMPLE_DEVICE_ID, pqLastResortPreKey);

    assertThat(ecCaptor.getValue()).containsExactly(preKey);
    assertThat(pqCaptor.getValue()).containsExactly(pqPreKey);

    verify(KEYS).storeEcSignedPreKeys(AuthHelper.VALID_UUID, AuthHelper.VALID_DEVICE.getId(), signedPreKey);
  }

  @Test
  void putKeysStructurallyInvalidSignedECKey() {
    final ECKeyPair identityKeyPair = ECKeyPair.generate();
    final IdentityKey identityKey = new IdentityKey(identityKeyPair.getPublicKey());
    final KEMSignedPreKey wrongPreKey = KeysHelper.signedKEMPreKey(1, identityKeyPair);
    final WeaklyTypedPreKeyState preKeyState =
        new WeaklyTypedPreKeyState(null, WeaklyTypedSignedPreKey.fromSignedPreKey(wrongPreKey), null, null, identityKey.serialize());

    Response response =
        resources.getJerseyTest()
                 .target("/v2/keys")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                 .put(Entity.entity(preKeyState, MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(400);
  }

  @Test
  void putKeysStructurallyInvalidUnsignedECKey() {
    final ECKeyPair identityKeyPair = ECKeyPair.generate();
    final IdentityKey identityKey = new IdentityKey(identityKeyPair.getPublicKey());
    final WeaklyTypedPreKey wrongPreKey = new WeaklyTypedPreKey(1, "cluck cluck i'm a parrot".getBytes());
    final WeaklyTypedPreKeyState preKeyState =
        new WeaklyTypedPreKeyState(List.of(wrongPreKey), null, null, null, identityKey.serialize());

    Response response =
        resources.getJerseyTest()
                 .target("/v2/keys")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                 .put(Entity.entity(preKeyState, MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(400);
  }

  @Test
  void putKeysStructurallyInvalidPQOneTimeKey() {
    final ECKeyPair identityKeyPair = ECKeyPair.generate();
    final IdentityKey identityKey = new IdentityKey(identityKeyPair.getPublicKey());
    final WeaklyTypedSignedPreKey wrongPreKey = WeaklyTypedSignedPreKey.fromSignedPreKey(KeysHelper.signedECPreKey(1, identityKeyPair));
    final WeaklyTypedPreKeyState preKeyState =
        new WeaklyTypedPreKeyState(null, null, List.of(wrongPreKey), null, identityKey.serialize());

    Response response =
        resources.getJerseyTest()
                 .target("/v2/keys")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                 .put(Entity.entity(preKeyState, MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(400);
  }

  @Test
  void putKeysStructurallyInvalidPQLastResortKey() {
    final ECKeyPair identityKeyPair = ECKeyPair.generate();
    final IdentityKey identityKey = new IdentityKey(identityKeyPair.getPublicKey());
    final WeaklyTypedSignedPreKey wrongPreKey = WeaklyTypedSignedPreKey.fromSignedPreKey(KeysHelper.signedECPreKey(1, identityKeyPair));
    final WeaklyTypedPreKeyState preKeyState =
        new WeaklyTypedPreKeyState(null, null, null, wrongPreKey, identityKey.serialize());

    Response response =
        resources.getJerseyTest()
                 .target("/v2/keys")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                 .put(Entity.entity(preKeyState, MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(400);
  }

  @Test
  void putKeysTooManySingleUseECKeys() {
    final List<ECPreKey> preKeys = IntStream.range(31337, 31438).mapToObj(KeysHelper::ecPreKey).toList();
    final ECSignedPreKey signedPreKey = KeysHelper.signedECPreKey(31338, AuthHelper.VALID_IDENTITY_KEY_PAIR);

    final SetKeysRequest setKeysRequest = new SetKeysRequest(preKeys, signedPreKey, null, null);

    Response response =
        resources.getJerseyTest()
                 .target("/v2/keys")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                 .put(Entity.entity(setKeysRequest, MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(422);

    verifyNoMoreInteractions(KEYS);
  }

  @Test
  void putKeysTooManySingleUseKEMKeys() {
    final List<KEMSignedPreKey> pqPreKeys = IntStream.range(31337, 31438)
        .mapToObj(id -> KeysHelper.signedKEMPreKey(id, AuthHelper.VALID_IDENTITY_KEY_PAIR))
        .toList();

    final SetKeysRequest setKeysRequest = new SetKeysRequest(null, null, pqPreKeys, null);

    Response response =
        resources.getJerseyTest()
                 .target("/v2/keys")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                 .put(Entity.entity(setKeysRequest, MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(422);

    verifyNoMoreInteractions(KEYS);
  }

  @Test
  void putKeysByPhoneNumberIdentifierTestV2() {
    final ECPreKey preKey = KeysHelper.ecPreKey(31337);
    final ECSignedPreKey signedPreKey = KeysHelper.signedECPreKey(31338, AuthHelper.VALID_PNI_IDENTITY_KEY_PAIR);

    final SetKeysRequest setKeysRequest = new SetKeysRequest(List.of(preKey), signedPreKey, null, null);

    Response response =
        resources.getJerseyTest()
            .target("/v2/keys")
            .queryParam("identity", "pni")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(setKeysRequest, MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(204);

    ArgumentCaptor<List<ECPreKey>> listCaptor = ArgumentCaptor.forClass(List.class);
    verify(KEYS).storeEcOneTimePreKeys(eq(AuthHelper.VALID_PNI), eq(SAMPLE_DEVICE_ID), listCaptor.capture());

    assertThat(listCaptor.getValue()).containsExactly(preKey);

    verify(KEYS).storeEcSignedPreKeys(AuthHelper.VALID_PNI, AuthHelper.VALID_DEVICE.getId(), signedPreKey);
  }

  @Test
  void putKeysByPhoneNumberIdentifierPqTestV2() {
    final ECPreKey preKey = KeysHelper.ecPreKey(31337);
    final ECSignedPreKey signedPreKey = KeysHelper.signedECPreKey(31338, AuthHelper.VALID_PNI_IDENTITY_KEY_PAIR);
    final KEMSignedPreKey pqPreKey = KeysHelper.signedKEMPreKey(31339, AuthHelper.VALID_PNI_IDENTITY_KEY_PAIR);
    final KEMSignedPreKey pqLastResortPreKey = KeysHelper.signedKEMPreKey(31340, AuthHelper.VALID_PNI_IDENTITY_KEY_PAIR);

    final SetKeysRequest setKeysRequest =
        new SetKeysRequest(List.of(preKey), signedPreKey, List.of(pqPreKey), pqLastResortPreKey);

    Response response =
        resources.getJerseyTest()
            .target("/v2/keys")
            .queryParam("identity", "pni")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(setKeysRequest, MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(204);

    ArgumentCaptor<List<ECPreKey>> ecCaptor = ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<List<KEMSignedPreKey>> pqCaptor = ArgumentCaptor.forClass(List.class);
    verify(KEYS).storeEcOneTimePreKeys(eq(AuthHelper.VALID_PNI), eq(SAMPLE_DEVICE_ID), ecCaptor.capture());
    verify(KEYS).storeKemOneTimePreKeys(eq(AuthHelper.VALID_PNI), eq(SAMPLE_DEVICE_ID), pqCaptor.capture());
    verify(KEYS).storePqLastResort(AuthHelper.VALID_PNI, SAMPLE_DEVICE_ID, pqLastResortPreKey);

    assertThat(ecCaptor.getValue()).containsExactly(preKey);
    assertThat(pqCaptor.getValue()).containsExactly(pqPreKey);

    verify(KEYS).storeEcSignedPreKeys(AuthHelper.VALID_PNI, AuthHelper.VALID_DEVICE.getId(), signedPreKey);
  }

  @Test
  void putPrekeyWithInvalidSignature() {
    final ECSignedPreKey badSignedPreKey = KeysHelper.signedECPreKey(1, ECKeyPair.generate());
    final SetKeysRequest setKeysRequest = new SetKeysRequest(List.of(), badSignedPreKey, null, null);
    Response response =
        resources.getJerseyTest()
            .target("/v2/keys")
            .queryParam("identity", "aci")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(setKeysRequest, MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(422);
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  @ParameterizedTest
  @MethodSource
  void checkKeys(
      final IdentityKey clientIdentityKey,
      final ECSignedPreKey clientEcSignedPreKey,
      final Optional<ECSignedPreKey> serverEcSignedPreKey,
      final KEMSignedPreKey clientLastResortKey,
      final Optional<KEMSignedPreKey> serverLastResortKey,
      final int expectedStatus) throws NoSuchAlgorithmException {

    when(KEYS.getEcSignedPreKey(AuthHelper.VALID_UUID, Device.PRIMARY_ID))
        .thenReturn(CompletableFuture.completedFuture(serverEcSignedPreKey));

    when(KEYS.getLastResort(AuthHelper.VALID_UUID, Device.PRIMARY_ID))
        .thenReturn(CompletableFuture.completedFuture(serverLastResortKey));

    final CheckKeysRequest checkKeysRequest =
        new CheckKeysRequest(IdentityType.ACI, getKeyDigest(clientIdentityKey, clientEcSignedPreKey, clientLastResortKey));

    try (final Response response =
        resources.getJerseyTest()
            .target("/v2/keys/check")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .post(Entity.entity(checkKeysRequest, MediaType.APPLICATION_JSON_TYPE))) {

      assertEquals(expectedStatus, response.getStatus());
    }
  }

  private static List<Arguments> checkKeys() {
    final ECSignedPreKey ecSignedPreKey = KeysHelper.signedECPreKey(17, AuthHelper.VALID_IDENTITY_KEY_PAIR);
    final KEMSignedPreKey lastResortKey = KeysHelper.signedKEMPreKey(19, AuthHelper.VALID_IDENTITY_KEY_PAIR);

    return List.of(
        // All keys match
        Arguments.of(
            AuthHelper.VALID_IDENTITY,
            ecSignedPreKey,
            Optional.of(ecSignedPreKey),
            lastResortKey,
            Optional.of(lastResortKey),
            200),

        // Signed EC pre-key not found
        Arguments.of(
            AuthHelper.VALID_IDENTITY,
            ecSignedPreKey,
            Optional.empty(),
            lastResortKey,
            Optional.of(lastResortKey),
            409),

        // Last-resort key not found
        Arguments.of(
            AuthHelper.VALID_IDENTITY,
            ecSignedPreKey,
            Optional.of(ecSignedPreKey),
            lastResortKey,
            Optional.empty(),
            409),

        // Mismatched identity key
        Arguments.of(
            new IdentityKey(ECKeyPair.generate().getPublicKey()),
            ecSignedPreKey,
            Optional.of(ecSignedPreKey),
            lastResortKey,
            Optional.of(lastResortKey),
            409),

        // Mismatched EC signed pre-key ID
        Arguments.of(
            AuthHelper.VALID_IDENTITY,
            new ECSignedPreKey(ecSignedPreKey.keyId() + 1, ecSignedPreKey.publicKey(), ecSignedPreKey.signature()),
            Optional.of(ecSignedPreKey),
            lastResortKey,
            Optional.of(lastResortKey),
            409),

        // Mismatched EC signed pre-key content
        Arguments.of(
            AuthHelper.VALID_IDENTITY,
            KeysHelper.signedECPreKey(ecSignedPreKey.keyId(), AuthHelper.VALID_IDENTITY_KEY_PAIR),
            Optional.of(ecSignedPreKey),
            lastResortKey,
            Optional.of(lastResortKey),
            409),
        // Mismatched last-resort key ID
        Arguments.of(
            AuthHelper.VALID_IDENTITY,
            ecSignedPreKey,
            Optional.of(ecSignedPreKey),
            new KEMSignedPreKey(lastResortKey.keyId() + 1, lastResortKey.publicKey(), lastResortKey.signature()),
            Optional.of(lastResortKey),
            409),

        // Mismatched last-resort key content
        Arguments.of(
            AuthHelper.VALID_IDENTITY,
            ecSignedPreKey,
            Optional.of(ecSignedPreKey),
            KeysHelper.signedKEMPreKey(lastResortKey.keyId(), AuthHelper.VALID_IDENTITY_KEY_PAIR),
            Optional.of(lastResortKey),
            409)
    );
  }

  private static byte[] getKeyDigest(final IdentityKey identityKey, final ECSignedPreKey ecSignedPreKey, final KEMSignedPreKey lastResortKey)
      throws NoSuchAlgorithmException {

    final MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
    messageDigest.update(identityKey.serialize());

    {
      final ByteBuffer ecSignedPreKeyIdBuffer = ByteBuffer.allocate(Long.BYTES);
      ecSignedPreKeyIdBuffer.putLong(ecSignedPreKey.keyId());
      ecSignedPreKeyIdBuffer.flip();

      messageDigest.update(ecSignedPreKeyIdBuffer);
      messageDigest.update(ecSignedPreKey.serializedPublicKey());
    }

    {
      final ByteBuffer lastResortKeyIdBuffer = ByteBuffer.allocate(Long.BYTES);
      lastResortKeyIdBuffer.putLong(lastResortKey.keyId());
      lastResortKeyIdBuffer.flip();

      messageDigest.update(lastResortKeyIdBuffer);
      messageDigest.update(lastResortKey.serializedPublicKey());
    }

    return messageDigest.digest();
  }

  @Test
  void checkKeysIncorrectDigestLength() {
    try (final Response response =
        resources.getJerseyTest()
            .target("/v2/keys/check")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .post(Entity.entity(new CheckKeysRequest(IdentityType.ACI, new byte[31]), MediaType.APPLICATION_JSON_TYPE))) {

      assertEquals(422, response.getStatus());
    }

    try (final Response response =
        resources.getJerseyTest()
            .target("/v2/keys/check")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .post(Entity.entity(new CheckKeysRequest(IdentityType.ACI, new byte[33]), MediaType.APPLICATION_JSON_TYPE))) {

      assertEquals(422, response.getStatus());
    }
  }
}
