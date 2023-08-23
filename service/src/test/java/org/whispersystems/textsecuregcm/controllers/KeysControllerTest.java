/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
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
import com.google.common.collect.ImmutableSet;
import io.dropwizard.auth.PolymorphicAuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.DisabledPermittedAuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.OptionalAccess;
import org.whispersystems.textsecuregcm.entities.ECPreKey;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.entities.PreKeyCount;
import org.whispersystems.textsecuregcm.entities.PreKeyResponse;
import org.whispersystems.textsecuregcm.entities.PreKeyState;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.PniServiceIdentifier;
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

@ExtendWith(DropwizardExtensionsSupport.class)
class KeysControllerTest {

  private static final String EXISTS_NUMBER = "+14152222222";
  private static final UUID   EXISTS_UUID   = UUID.randomUUID();
  private static final UUID   EXISTS_PNI    = UUID.randomUUID();

  private static final UUID   NOT_EXISTS_UUID   = UUID.randomUUID();

  private static final int SAMPLE_REGISTRATION_ID  =  999;
  private static final int SAMPLE_REGISTRATION_ID2 = 1002;
  private static final int SAMPLE_REGISTRATION_ID4 = 1555;

  private static final int SAMPLE_PNI_REGISTRATION_ID = 1717;

  private final ECKeyPair IDENTITY_KEY_PAIR = Curve.generateKeyPair();
  private final IdentityKey IDENTITY_KEY = new IdentityKey(IDENTITY_KEY_PAIR.getPublicKey());

  private final ECKeyPair PNI_IDENTITY_KEY_PAIR = Curve.generateKeyPair();
  private final IdentityKey PNI_IDENTITY_KEY = new IdentityKey(PNI_IDENTITY_KEY_PAIR.getPublicKey());

  private final ECPreKey SAMPLE_KEY = KeysHelper.ecPreKey(1234);
  private final ECPreKey SAMPLE_KEY2 = KeysHelper.ecPreKey(5667);
  private final ECPreKey SAMPLE_KEY3 = KeysHelper.ecPreKey(334);
  private final ECPreKey SAMPLE_KEY4 = KeysHelper.ecPreKey(336);

  private final ECPreKey SAMPLE_KEY_PNI = KeysHelper.ecPreKey(7777);

  private final KEMSignedPreKey SAMPLE_PQ_KEY = KeysHelper.signedKEMPreKey(2424, Curve.generateKeyPair());
  private final KEMSignedPreKey SAMPLE_PQ_KEY2 = KeysHelper.signedKEMPreKey(6868, Curve.generateKeyPair());
  private final KEMSignedPreKey SAMPLE_PQ_KEY3 = KeysHelper.signedKEMPreKey(1313, Curve.generateKeyPair());

  private final KEMSignedPreKey SAMPLE_PQ_KEY_PNI = KeysHelper.signedKEMPreKey(8888, Curve.generateKeyPair());

  private final ECSignedPreKey SAMPLE_SIGNED_KEY = KeysHelper.signedECPreKey(1111, IDENTITY_KEY_PAIR);
  private final ECSignedPreKey SAMPLE_SIGNED_KEY2 = KeysHelper.signedECPreKey(2222, IDENTITY_KEY_PAIR);
  private final ECSignedPreKey SAMPLE_SIGNED_KEY3 = KeysHelper.signedECPreKey(3333, IDENTITY_KEY_PAIR);
  private final ECSignedPreKey SAMPLE_SIGNED_PNI_KEY = KeysHelper.signedECPreKey(4444, PNI_IDENTITY_KEY_PAIR);
  private final ECSignedPreKey SAMPLE_SIGNED_PNI_KEY2 = KeysHelper.signedECPreKey(5555, PNI_IDENTITY_KEY_PAIR);
  private final ECSignedPreKey SAMPLE_SIGNED_PNI_KEY3 = KeysHelper.signedECPreKey(6666, PNI_IDENTITY_KEY_PAIR);
  private final ECSignedPreKey VALID_DEVICE_SIGNED_KEY = KeysHelper.signedECPreKey(89898, IDENTITY_KEY_PAIR);
  private final ECSignedPreKey VALID_DEVICE_PNI_SIGNED_KEY = KeysHelper.signedECPreKey(7777, PNI_IDENTITY_KEY_PAIR);

  private final static KeysManager KEYS = mock(KeysManager.class               );
  private final static AccountsManager             accounts                    = mock(AccountsManager.class            );
  private final static Account                     existsAccount               = mock(Account.class                    );

  private static final RateLimiters          rateLimiters  = mock(RateLimiters.class);
  private static final RateLimiter           rateLimiter   = mock(RateLimiter.class );

  private static final ResourceExtension resources = ResourceExtension.builder()
      .addProperty(ServerProperties.UNWRAP_COMPLETION_STAGE_IN_WRITER_ENABLE, Boolean.TRUE)
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(CompletionExceptionMapper.class)
      .addProvider(new PolymorphicAuthValueFactoryProvider.Binder<>(ImmutableSet.of(
          AuthenticatedAccount.class, DisabledPermittedAuthenticatedAccount.class)))
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(new ServerRejectedExceptionMapper())
      .addResource(new KeysController(rateLimiters, KEYS, accounts))
      .addResource(new RateLimitExceededExceptionMapper())
      .build();

  private Device sampleDevice;

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

  @BeforeEach
  void setup() {
    sampleDevice               = mock(Device.class);
    final Device sampleDevice2 = mock(Device.class);
    final Device sampleDevice3 = mock(Device.class);
    final Device sampleDevice4 = mock(Device.class);

    final List<Device> allDevices = List.of(sampleDevice, sampleDevice2, sampleDevice3, sampleDevice4);

    AccountsHelper.setupMockUpdate(accounts);

    when(sampleDevice.getRegistrationId()).thenReturn(SAMPLE_REGISTRATION_ID);
    when(sampleDevice2.getRegistrationId()).thenReturn(SAMPLE_REGISTRATION_ID2);
    when(sampleDevice3.getRegistrationId()).thenReturn(SAMPLE_REGISTRATION_ID2);
    when(sampleDevice4.getRegistrationId()).thenReturn(SAMPLE_REGISTRATION_ID4);
    when(sampleDevice.getPhoneNumberIdentityRegistrationId()).thenReturn(OptionalInt.of(SAMPLE_PNI_REGISTRATION_ID));
    when(sampleDevice.isEnabled()).thenReturn(true);
    when(sampleDevice2.isEnabled()).thenReturn(true);
    when(sampleDevice3.isEnabled()).thenReturn(false);
    when(sampleDevice4.isEnabled()).thenReturn(true);
    when(sampleDevice.getSignedPreKey(IdentityType.ACI)).thenReturn(SAMPLE_SIGNED_KEY);
    when(sampleDevice2.getSignedPreKey(IdentityType.ACI)).thenReturn(SAMPLE_SIGNED_KEY2);
    when(sampleDevice3.getSignedPreKey(IdentityType.ACI)).thenReturn(SAMPLE_SIGNED_KEY3);
    when(sampleDevice4.getSignedPreKey(IdentityType.ACI)).thenReturn(null);
    when(sampleDevice.getSignedPreKey(IdentityType.PNI)).thenReturn(SAMPLE_SIGNED_PNI_KEY);
    when(sampleDevice2.getSignedPreKey(IdentityType.PNI)).thenReturn(SAMPLE_SIGNED_PNI_KEY2);
    when(sampleDevice3.getSignedPreKey(IdentityType.PNI)).thenReturn(SAMPLE_SIGNED_PNI_KEY3);
    when(sampleDevice4.getSignedPreKey(IdentityType.PNI)).thenReturn(null);
    when(sampleDevice.getId()).thenReturn(1L);
    when(sampleDevice2.getId()).thenReturn(2L);
    when(sampleDevice3.getId()).thenReturn(3L);
    when(sampleDevice4.getId()).thenReturn(4L);

    when(existsAccount.getUuid()).thenReturn(EXISTS_UUID);
    when(existsAccount.getPhoneNumberIdentifier()).thenReturn(EXISTS_PNI);
    when(existsAccount.getDevice(1L)).thenReturn(Optional.of(sampleDevice));
    when(existsAccount.getDevice(2L)).thenReturn(Optional.of(sampleDevice2));
    when(existsAccount.getDevice(3L)).thenReturn(Optional.of(sampleDevice3));
    when(existsAccount.getDevice(4L)).thenReturn(Optional.of(sampleDevice4));
    when(existsAccount.getDevice(22L)).thenReturn(Optional.empty());
    when(existsAccount.getDevices()).thenReturn(allDevices);
    when(existsAccount.isEnabled()).thenReturn(true);
    when(existsAccount.getIdentityKey(IdentityType.ACI)).thenReturn(IDENTITY_KEY);
    when(existsAccount.getIdentityKey(IdentityType.PNI)).thenReturn(PNI_IDENTITY_KEY);
    when(existsAccount.getNumber()).thenReturn(EXISTS_NUMBER);
    when(existsAccount.getUnidentifiedAccessKey()).thenReturn(Optional.of("1337".getBytes()));

    when(accounts.getByServiceIdentifier(any())).thenReturn(Optional.empty());

    when(accounts.getByServiceIdentifier(new AciServiceIdentifier(EXISTS_UUID))).thenReturn(Optional.of(existsAccount));
    when(accounts.getByServiceIdentifier(new PniServiceIdentifier(EXISTS_PNI))).thenReturn(Optional.of(existsAccount));

    when(rateLimiters.getPreKeysLimiter()).thenReturn(rateLimiter);

    when(KEYS.store(any(), anyLong(), any(), any(), any(), any())).thenReturn(CompletableFuture.completedFuture(null));
    when(KEYS.getEcSignedPreKey(any(), anyLong())).thenReturn(CompletableFuture.completedFuture(Optional.empty()));
    when(KEYS.storeEcSignedPreKeys(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

    when(KEYS.takeEC(EXISTS_UUID, 1)).thenReturn(CompletableFuture.completedFuture(Optional.of(SAMPLE_KEY)));
    when(KEYS.takePQ(EXISTS_UUID, 1)).thenReturn(CompletableFuture.completedFuture(Optional.of(SAMPLE_PQ_KEY)));
    when(KEYS.takeEC(EXISTS_PNI, 1)).thenReturn(CompletableFuture.completedFuture(Optional.of(SAMPLE_KEY_PNI)));
    when(KEYS.takePQ(EXISTS_PNI, 1)).thenReturn(CompletableFuture.completedFuture(Optional.of(SAMPLE_PQ_KEY_PNI)));

    when(KEYS.getEcCount(AuthHelper.VALID_UUID, 1)).thenReturn(CompletableFuture.completedFuture(5));
    when(KEYS.getPqCount(AuthHelper.VALID_UUID, 1)).thenReturn(CompletableFuture.completedFuture(5));

    when(AuthHelper.VALID_DEVICE.getSignedPreKey(IdentityType.ACI)).thenReturn(VALID_DEVICE_SIGNED_KEY);
    when(AuthHelper.VALID_DEVICE.getSignedPreKey(IdentityType.PNI)).thenReturn(VALID_DEVICE_PNI_SIGNED_KEY);
    when(AuthHelper.VALID_ACCOUNT.getIdentityKey(IdentityType.ACI)).thenReturn(null);
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
    PreKeyCount result = resources.getJerseyTest()
                                  .target("/v2/keys")
                                  .request()
                                  .header("Authorization",
                                          AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                                  .get(PreKeyCount.class);

    assertThat(result.getCount()).isEqualTo(5);
    assertThat(result.getPqCount()).isEqualTo(5);

    verify(KEYS).getEcCount(AuthHelper.VALID_UUID, 1);
    verify(KEYS).getPqCount(AuthHelper.VALID_UUID, 1);
  }

  @Test
  void putSignedPreKeyV2() {
    ECSignedPreKey test = KeysHelper.signedECPreKey(9998, IDENTITY_KEY_PAIR);
    Response response = resources.getJerseyTest()
                                 .target("/v2/keys/signed")
                                 .request()
                                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                                 .put(Entity.entity(test, MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(204);

    verify(AuthHelper.VALID_DEVICE).setSignedPreKey(eq(test));
    verify(AuthHelper.VALID_DEVICE, never()).setPhoneNumberIdentitySignedPreKey(any());
    verify(accounts).updateDevice(eq(AuthHelper.VALID_ACCOUNT), anyLong(), any());
    verify(KEYS).storeEcSignedPreKeys(AuthHelper.VALID_UUID, Map.of(Device.MASTER_ID, test));
  }

  @Test
  void putPhoneNumberIdentitySignedPreKeyV2() {
    final ECSignedPreKey replacementKey = KeysHelper.signedECPreKey(9998, PNI_IDENTITY_KEY_PAIR);

    Response response = resources.getJerseyTest()
        .target("/v2/keys/signed")
        .queryParam("identity", "pni")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .put(Entity.entity(replacementKey, MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(204);

    verify(AuthHelper.VALID_DEVICE).setPhoneNumberIdentitySignedPreKey(eq(replacementKey));
    verify(AuthHelper.VALID_DEVICE, never()).setSignedPreKey(any());
    verify(accounts).updateDevice(eq(AuthHelper.VALID_ACCOUNT), anyLong(), any());
    verify(KEYS).storeEcSignedPreKeys(AuthHelper.VALID_PNI, Map.of(Device.MASTER_ID, replacementKey));
  }

  @Test
  void disabledPutSignedPreKeyV2() {
    ECSignedPreKey test = KeysHelper.signedECPreKey(9999, IDENTITY_KEY_PAIR);
    Response response = resources.getJerseyTest()
                                 .target("/v2/keys/signed")
                                 .request()
                                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.DISABLED_UUID, AuthHelper.DISABLED_PASSWORD))
                                 .put(Entity.entity(test, MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(401);
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
    assertEquals(SAMPLE_KEY, result.getDevice(1).getPreKey());
    assertThat(result.getDevice(1).getPqPreKey()).isNull();
    assertThat(result.getDevice(1).getRegistrationId()).isEqualTo(SAMPLE_REGISTRATION_ID);
    assertEquals(existsAccount.getDevice(1).get().getSignedPreKey(IdentityType.ACI),
        result.getDevice(1).getSignedPreKey());

    verify(KEYS).takeEC(EXISTS_UUID, 1);
    verify(KEYS).getEcSignedPreKey(EXISTS_UUID, 1);
    verifyNoMoreInteractions(KEYS);
  }

  @Test
  void validSingleRequestPqTestNoPqKeysV2() {
    when(KEYS.takePQ(EXISTS_UUID, 1)).thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    PreKeyResponse result = resources.getJerseyTest()
        .target(String.format("/v2/keys/%s/1", EXISTS_UUID))
        .queryParam("pq", "true")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(PreKeyResponse.class);

    assertThat(result.getIdentityKey()).isEqualTo(existsAccount.getIdentityKey(IdentityType.ACI));
    assertThat(result.getDevicesCount()).isEqualTo(1);
    assertEquals(SAMPLE_KEY, result.getDevice(1).getPreKey());
    assertThat(result.getDevice(1).getPqPreKey()).isNull();
    assertThat(result.getDevice(1).getRegistrationId()).isEqualTo(SAMPLE_REGISTRATION_ID);
    assertEquals(existsAccount.getDevice(1).get().getSignedPreKey(IdentityType.ACI),
        result.getDevice(1).getSignedPreKey());

    verify(KEYS).takeEC(EXISTS_UUID, 1);
    verify(KEYS).takePQ(EXISTS_UUID, 1);
    verify(KEYS).getEcSignedPreKey(EXISTS_UUID, 1);
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
    assertEquals(SAMPLE_KEY, result.getDevice(1).getPreKey());
    assertEquals(SAMPLE_PQ_KEY, result.getDevice(1).getPqPreKey());
    assertThat(result.getDevice(1).getRegistrationId()).isEqualTo(SAMPLE_REGISTRATION_ID);
    assertEquals(existsAccount.getDevice(1).get().getSignedPreKey(IdentityType.ACI),
        result.getDevice(1).getSignedPreKey());

    verify(KEYS).takeEC(EXISTS_UUID, 1);
    verify(KEYS).takePQ(EXISTS_UUID, 1);
    verify(KEYS).getEcSignedPreKey(EXISTS_UUID, 1);
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
    assertEquals(SAMPLE_KEY_PNI, result.getDevice(1).getPreKey());
    assertThat(result.getDevice(1).getPqPreKey()).isNull();
    assertThat(result.getDevice(1).getRegistrationId()).isEqualTo(SAMPLE_PNI_REGISTRATION_ID);
    assertEquals(existsAccount.getDevice(1).get().getSignedPreKey(IdentityType.PNI),
        result.getDevice(1).getSignedPreKey());

    verify(KEYS).takeEC(EXISTS_PNI, 1);
    verify(KEYS).getEcSignedPreKey(EXISTS_PNI, 1);
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
    assertEquals(SAMPLE_KEY_PNI, result.getDevice(1).getPreKey());
    assertThat(result.getDevice(1).getPqPreKey()).isEqualTo(SAMPLE_PQ_KEY_PNI);
    assertThat(result.getDevice(1).getRegistrationId()).isEqualTo(SAMPLE_PNI_REGISTRATION_ID);
    assertEquals(existsAccount.getDevice(1).get().getSignedPreKey(IdentityType.PNI),
        result.getDevice(1).getSignedPreKey());

    verify(KEYS).takeEC(EXISTS_PNI, 1);
    verify(KEYS).takePQ(EXISTS_PNI, 1);
    verify(KEYS).getEcSignedPreKey(EXISTS_PNI, 1);
    verifyNoMoreInteractions(KEYS);
  }

  @Test
  void validSingleRequestByPhoneNumberIdentifierNoPniRegistrationIdTestV2() {
    when(sampleDevice.getPhoneNumberIdentityRegistrationId()).thenReturn(OptionalInt.empty());

    PreKeyResponse result = resources.getJerseyTest()
        .target(String.format("/v2/keys/PNI:%s/1", EXISTS_PNI))
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(PreKeyResponse.class);

    assertThat(result.getIdentityKey()).isEqualTo(existsAccount.getIdentityKey(IdentityType.PNI));
    assertThat(result.getDevicesCount()).isEqualTo(1);
    assertEquals(SAMPLE_KEY_PNI, result.getDevice(1).getPreKey());
    assertThat(result.getDevice(1).getPqPreKey()).isNull();
    assertThat(result.getDevice(1).getRegistrationId()).isEqualTo(SAMPLE_REGISTRATION_ID);
    assertEquals(existsAccount.getDevice(1).get().getSignedPreKey(IdentityType.PNI),
        result.getDevice(1).getSignedPreKey());

    verify(KEYS).takeEC(EXISTS_PNI, 1);
    verify(KEYS).getEcSignedPreKey(EXISTS_PNI, 1);
    verifyNoMoreInteractions(KEYS);
  }

  @Test
  void testGetKeysRateLimited() throws RateLimitExceededException {
    Duration retryAfter = Duration.ofSeconds(31);
    doThrow(new RateLimitExceededException(retryAfter, true)).when(rateLimiter).validate(anyString());

    Response result = resources.getJerseyTest()
        .target(String.format("/v2/keys/PNI:%s/*", EXISTS_PNI))
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get();

    assertThat(result.getStatus()).isEqualTo(413);
    assertThat(result.getHeaderString("Retry-After")).isEqualTo(String.valueOf(retryAfter.toSeconds()));
  }

  @Test
  void testUnidentifiedRequest() {
    PreKeyResponse result = resources.getJerseyTest()
        .target(String.format("/v2/keys/%s/1", EXISTS_UUID))
        .queryParam("pq", "true")
        .request()
        .header(OptionalAccess.UNIDENTIFIED, AuthHelper.getUnidentifiedAccessHeader("1337".getBytes()))
        .get(PreKeyResponse.class);

    assertThat(result.getIdentityKey()).isEqualTo(existsAccount.getIdentityKey(IdentityType.ACI));
    assertThat(result.getDevicesCount()).isEqualTo(1);
    assertEquals(SAMPLE_KEY, result.getDevice(1).getPreKey());
    assertEquals(SAMPLE_PQ_KEY, result.getDevice(1).getPqPreKey());
    assertEquals(existsAccount.getDevice(1).get().getSignedPreKey(IdentityType.ACI),
        result.getDevice(1).getSignedPreKey());

    verify(KEYS).takeEC(EXISTS_UUID, 1);
    verify(KEYS).takePQ(EXISTS_UUID, 1);
    verify(KEYS).getEcSignedPreKey(EXISTS_UUID, 1);
    verifyNoMoreInteractions(KEYS);
  }

  @Test
  void testNoDevices() {

    when(existsAccount.getDevices()).thenReturn(Collections.emptyList());

    Response result = resources.getJerseyTest()
        .target(String.format("/v2/keys/%s/*", EXISTS_UUID))
        .request()
        .header(OptionalAccess.UNIDENTIFIED, AuthHelper.getUnidentifiedAccessHeader("1337".getBytes()))
        .get();

    assertThat(result).isNotNull();
    assertThat(result.getStatus()).isEqualTo(404);
  }

  @Test
  void testUnauthorizedUnidentifiedRequest() {
    Response response = resources.getJerseyTest()
                                     .target(String.format("/v2/keys/%s/1", EXISTS_UUID))
                                     .request()
                                     .header(OptionalAccess.UNIDENTIFIED, AuthHelper.getUnidentifiedAccessHeader("9999".getBytes()))
                                     .get();

    assertThat(response.getStatus()).isEqualTo(401);
    verifyNoMoreInteractions(KEYS);
  }

  @Test
  void testMalformedUnidentifiedRequest() {
    Response response = resources.getJerseyTest()
                                 .target(String.format("/v2/keys/%s/1", EXISTS_UUID))
                                 .request()
                                 .header(OptionalAccess.UNIDENTIFIED, "$$$$$$$$$")
                                 .get();

    assertThat(response.getStatus()).isEqualTo(401);
    verifyNoMoreInteractions(KEYS);
  }


  @Test
  void validMultiRequestTestV2() {
    when(KEYS.takeEC(EXISTS_UUID, 1)).thenReturn(CompletableFuture.completedFuture(Optional.of(SAMPLE_KEY)));
    when(KEYS.takeEC(EXISTS_UUID, 2)).thenReturn(CompletableFuture.completedFuture(Optional.of(SAMPLE_KEY2)));
    when(KEYS.takeEC(EXISTS_UUID, 3)).thenReturn(CompletableFuture.completedFuture(Optional.of(SAMPLE_KEY3)));
    when(KEYS.takeEC(EXISTS_UUID, 4)).thenReturn(CompletableFuture.completedFuture(Optional.of(SAMPLE_KEY4)));

    PreKeyResponse results = resources.getJerseyTest()
        .target(String.format("/v2/keys/%s/*", EXISTS_UUID))
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(PreKeyResponse.class);

    assertThat(results.getDevicesCount()).isEqualTo(3);
    assertThat(results.getIdentityKey()).isEqualTo(existsAccount.getIdentityKey(IdentityType.ACI));

    ECSignedPreKey signedPreKey = results.getDevice(1).getSignedPreKey();
    ECPreKey preKey = results.getDevice(1).getPreKey();
    long registrationId = results.getDevice(1).getRegistrationId();
    long deviceId = results.getDevice(1).getDeviceId();

    assertEquals(SAMPLE_KEY, preKey);
    assertThat(registrationId).isEqualTo(SAMPLE_REGISTRATION_ID);
    assertEquals(SAMPLE_SIGNED_KEY, signedPreKey);
    assertThat(deviceId).isEqualTo(1);

    signedPreKey = results.getDevice(2).getSignedPreKey();
    preKey = results.getDevice(2).getPreKey();
    registrationId = results.getDevice(2).getRegistrationId();
    deviceId = results.getDevice(2).getDeviceId();

    assertEquals(SAMPLE_KEY2, preKey);
    assertThat(registrationId).isEqualTo(SAMPLE_REGISTRATION_ID2);
    assertEquals(SAMPLE_SIGNED_KEY2, signedPreKey);
    assertThat(deviceId).isEqualTo(2);

    signedPreKey = results.getDevice(4).getSignedPreKey();
    preKey = results.getDevice(4).getPreKey();
    registrationId = results.getDevice(4).getRegistrationId();
    deviceId = results.getDevice(4).getDeviceId();

    assertEquals(SAMPLE_KEY4, preKey);
    assertThat(registrationId).isEqualTo(SAMPLE_REGISTRATION_ID4);
    assertThat(signedPreKey).isNull();
    assertThat(deviceId).isEqualTo(4);

    verify(KEYS).takeEC(EXISTS_UUID, 1);
    verify(KEYS).takeEC(EXISTS_UUID, 2);
    verify(KEYS).takeEC(EXISTS_UUID, 4);
    verify(KEYS).getEcSignedPreKey(EXISTS_UUID, 1);
    verify(KEYS).getEcSignedPreKey(EXISTS_UUID, 2);
    verify(KEYS).getEcSignedPreKey(EXISTS_UUID, 4);
    verifyNoMoreInteractions(KEYS);
  }

  @Test
  void validMultiRequestPqTestV2() {
    when(KEYS.takeEC(any(), anyLong())).thenReturn(CompletableFuture.completedFuture(Optional.empty()));
    when(KEYS.takePQ(any(), anyLong())).thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    when(KEYS.takeEC(EXISTS_UUID, 1)).thenReturn(CompletableFuture.completedFuture(Optional.of(SAMPLE_KEY)));
    when(KEYS.takeEC(EXISTS_UUID, 3)).thenReturn(CompletableFuture.completedFuture(Optional.of(SAMPLE_KEY3)));
    when(KEYS.takeEC(EXISTS_UUID, 4)).thenReturn(CompletableFuture.completedFuture(Optional.of(SAMPLE_KEY4)));
    when(KEYS.takePQ(EXISTS_UUID, 1)).thenReturn(CompletableFuture.completedFuture(Optional.of(SAMPLE_PQ_KEY)));
    when(KEYS.takePQ(EXISTS_UUID, 2)).thenReturn(CompletableFuture.completedFuture(Optional.of(SAMPLE_PQ_KEY2)));
    when(KEYS.takePQ(EXISTS_UUID, 3)).thenReturn(CompletableFuture.completedFuture(Optional.of(SAMPLE_PQ_KEY3)));

    PreKeyResponse results = resources.getJerseyTest()
        .target(String.format("/v2/keys/%s/*", EXISTS_UUID))
        .queryParam("pq", "true")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(PreKeyResponse.class);

    assertThat(results.getDevicesCount()).isEqualTo(3);
    assertThat(results.getIdentityKey()).isEqualTo(existsAccount.getIdentityKey(IdentityType.ACI));

    ECSignedPreKey signedPreKey = results.getDevice(1).getSignedPreKey();
    ECPreKey preKey = results.getDevice(1).getPreKey();
    KEMSignedPreKey pqPreKey = results.getDevice(1).getPqPreKey();
    long registrationId = results.getDevice(1).getRegistrationId();
    long deviceId = results.getDevice(1).getDeviceId();

    assertEquals(SAMPLE_KEY, preKey);
    assertEquals(SAMPLE_PQ_KEY, pqPreKey);
    assertThat(registrationId).isEqualTo(SAMPLE_REGISTRATION_ID);
    assertEquals(SAMPLE_SIGNED_KEY, signedPreKey);
    assertThat(deviceId).isEqualTo(1);

    signedPreKey = results.getDevice(2).getSignedPreKey();
    preKey = results.getDevice(2).getPreKey();
    pqPreKey = results.getDevice(2).getPqPreKey();
    registrationId = results.getDevice(2).getRegistrationId();
    deviceId = results.getDevice(2).getDeviceId();

    assertThat(preKey).isNull();
    assertEquals(SAMPLE_PQ_KEY2, pqPreKey);
    assertThat(registrationId).isEqualTo(SAMPLE_REGISTRATION_ID2);
    assertEquals(SAMPLE_SIGNED_KEY2, signedPreKey);
    assertThat(deviceId).isEqualTo(2);

    signedPreKey = results.getDevice(4).getSignedPreKey();
    preKey = results.getDevice(4).getPreKey();
    pqPreKey = results.getDevice(4).getPqPreKey();
    registrationId = results.getDevice(4).getRegistrationId();
    deviceId = results.getDevice(4).getDeviceId();

    assertEquals(SAMPLE_KEY4, preKey);
    assertThat(pqPreKey).isNull();
    assertThat(registrationId).isEqualTo(SAMPLE_REGISTRATION_ID4);
    assertThat(signedPreKey).isNull();
    assertThat(deviceId).isEqualTo(4);

    verify(KEYS).takeEC(EXISTS_UUID, 1);
    verify(KEYS).takePQ(EXISTS_UUID, 1);
    verify(KEYS).takeEC(EXISTS_UUID, 2);
    verify(KEYS).takePQ(EXISTS_UUID, 2);
    verify(KEYS).takeEC(EXISTS_UUID, 4);
    verify(KEYS).takePQ(EXISTS_UUID, 4);
    verify(KEYS).getEcSignedPreKey(EXISTS_UUID, 1);
    verify(KEYS).getEcSignedPreKey(EXISTS_UUID, 2);
    verify(KEYS).getEcSignedPreKey(EXISTS_UUID, 4);
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
    final ECKeyPair identityKeyPair = Curve.generateKeyPair();
    final ECSignedPreKey signedPreKey = KeysHelper.signedECPreKey(31338, identityKeyPair);
    final IdentityKey identityKey = new IdentityKey(identityKeyPair.getPublicKey());

    PreKeyState preKeyState = new PreKeyState(identityKey, signedPreKey, List.of(preKey));

    Response response =
        resources.getJerseyTest()
                 .target("/v2/keys")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                 .put(Entity.entity(preKeyState, MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(204);

    ArgumentCaptor<List<ECPreKey>> listCaptor = ArgumentCaptor.forClass(List.class);
    verify(KEYS).store(eq(AuthHelper.VALID_UUID), eq(1L), listCaptor.capture(), isNull(), eq(signedPreKey), isNull());

    assertThat(listCaptor.getValue()).containsExactly(preKey);

    verify(AuthHelper.VALID_ACCOUNT).setIdentityKey(eq(identityKey));
    verify(AuthHelper.VALID_DEVICE).setSignedPreKey(eq(signedPreKey));
    verify(accounts).update(eq(AuthHelper.VALID_ACCOUNT), any());
  }

  @Test
  void putKeysPqTestV2() {
    final ECPreKey preKey = KeysHelper.ecPreKey(31337);
    final ECKeyPair identityKeyPair = Curve.generateKeyPair();
    final ECSignedPreKey signedPreKey = KeysHelper.signedECPreKey(31338, identityKeyPair);
    final KEMSignedPreKey pqPreKey = KeysHelper.signedKEMPreKey(31339, identityKeyPair);
    final KEMSignedPreKey pqLastResortPreKey = KeysHelper.signedKEMPreKey(31340, identityKeyPair);
    final IdentityKey identityKey = new IdentityKey(identityKeyPair.getPublicKey());

    PreKeyState preKeyState = new PreKeyState(identityKey, signedPreKey, List.of(preKey), List.of(pqPreKey), pqLastResortPreKey);

    Response response =
        resources.getJerseyTest()
                 .target("/v2/keys")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                 .put(Entity.entity(preKeyState, MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(204);

    ArgumentCaptor<List<ECPreKey>> ecCaptor = ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<List<KEMSignedPreKey>> pqCaptor = ArgumentCaptor.forClass(List.class);
    verify(KEYS).store(eq(AuthHelper.VALID_UUID), eq(1L), ecCaptor.capture(), pqCaptor.capture(), eq(signedPreKey), eq(pqLastResortPreKey));

    assertThat(ecCaptor.getValue()).containsExactly(preKey);
    assertThat(pqCaptor.getValue()).containsExactly(pqPreKey);

    verify(AuthHelper.VALID_ACCOUNT).setIdentityKey(eq(identityKey));
    verify(AuthHelper.VALID_DEVICE).setSignedPreKey(eq(signedPreKey));
    verify(accounts).update(eq(AuthHelper.VALID_ACCOUNT), any());
  }

  @Test
  void putKeysStructurallyInvalidSignedECKey() {
    final ECKeyPair identityKeyPair = Curve.generateKeyPair();
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
    final ECKeyPair identityKeyPair = Curve.generateKeyPair();
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
    final ECKeyPair identityKeyPair = Curve.generateKeyPair();
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
    final ECKeyPair identityKeyPair = Curve.generateKeyPair();
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
  void putKeysByPhoneNumberIdentifierTestV2() {
    final ECPreKey preKey = KeysHelper.ecPreKey(31337);
    final ECKeyPair identityKeyPair = Curve.generateKeyPair();
    final ECSignedPreKey signedPreKey = KeysHelper.signedECPreKey(31338, identityKeyPair);
    final IdentityKey identityKey = new IdentityKey(identityKeyPair.getPublicKey());

    PreKeyState preKeyState = new PreKeyState(identityKey, signedPreKey, List.of(preKey));

    Response response =
        resources.getJerseyTest()
            .target("/v2/keys")
            .queryParam("identity", "pni")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(preKeyState, MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(204);

    ArgumentCaptor<List<ECPreKey>> listCaptor = ArgumentCaptor.forClass(List.class);
    verify(KEYS).store(eq(AuthHelper.VALID_PNI), eq(1L), listCaptor.capture(), isNull(), eq(signedPreKey), isNull());

    assertThat(listCaptor.getValue()).containsExactly(preKey);

    verify(AuthHelper.VALID_ACCOUNT).setPhoneNumberIdentityKey(eq(identityKey));
    verify(AuthHelper.VALID_DEVICE).setPhoneNumberIdentitySignedPreKey(eq(signedPreKey));
    verify(accounts).update(eq(AuthHelper.VALID_ACCOUNT), any());
  }

  @Test
  void putKeysByPhoneNumberIdentifierPqTestV2() {
    final ECPreKey preKey = KeysHelper.ecPreKey(31337);
    final ECKeyPair identityKeyPair = Curve.generateKeyPair();
    final ECSignedPreKey signedPreKey = KeysHelper.signedECPreKey(31338, identityKeyPair);
    final KEMSignedPreKey pqPreKey = KeysHelper.signedKEMPreKey(31339, identityKeyPair);
    final KEMSignedPreKey pqLastResortPreKey = KeysHelper.signedKEMPreKey(31340, identityKeyPair);
    final IdentityKey identityKey = new IdentityKey(identityKeyPair.getPublicKey());

    PreKeyState preKeyState = new PreKeyState(identityKey, signedPreKey, List.of(preKey), List.of(pqPreKey), pqLastResortPreKey);

    Response response =
        resources.getJerseyTest()
            .target("/v2/keys")
            .queryParam("identity", "pni")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(preKeyState, MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(204);

    ArgumentCaptor<List<ECPreKey>> ecCaptor = ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<List<KEMSignedPreKey>> pqCaptor = ArgumentCaptor.forClass(List.class);
    verify(KEYS).store(eq(AuthHelper.VALID_PNI), eq(1L), ecCaptor.capture(), pqCaptor.capture(), eq(signedPreKey), eq(pqLastResortPreKey));

    assertThat(ecCaptor.getValue()).containsExactly(preKey);
    assertThat(pqCaptor.getValue()).containsExactly(pqPreKey);

    verify(AuthHelper.VALID_ACCOUNT).setPhoneNumberIdentityKey(eq(identityKey));
    verify(AuthHelper.VALID_DEVICE).setPhoneNumberIdentitySignedPreKey(eq(signedPreKey));
    verify(accounts).update(eq(AuthHelper.VALID_ACCOUNT), any());
  }

  @Test
  void putPrekeyWithInvalidSignature() {
    final ECSignedPreKey badSignedPreKey = KeysHelper.signedECPreKey(1, Curve.generateKeyPair());
    PreKeyState preKeyState = new PreKeyState(IDENTITY_KEY, badSignedPreKey, List.of());
    Response response =
        resources.getJerseyTest()
            .target("/v2/keys")
            .queryParam("identity", "aci")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(preKeyState, MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(422);
  }

  @Test
  void disabledPutKeysTestV2() {
    final ECPreKey preKey = KeysHelper.ecPreKey(31337);
    final ECKeyPair identityKeyPair = Curve.generateKeyPair();
    final ECSignedPreKey signedPreKey = KeysHelper.signedECPreKey(31338, identityKeyPair);
    final IdentityKey identityKey = new IdentityKey(identityKeyPair.getPublicKey());

    PreKeyState preKeyState = new PreKeyState(identityKey, signedPreKey, List.of(preKey));

    Response response =
        resources.getJerseyTest()
            .target("/v2/keys")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.DISABLED_UUID, AuthHelper.DISABLED_PASSWORD))
            .put(Entity.entity(preKeyState, MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(204);

    ArgumentCaptor<List<ECPreKey>> listCaptor = ArgumentCaptor.forClass(List.class);
    verify(KEYS).store(eq(AuthHelper.DISABLED_UUID), eq(1L), listCaptor.capture(), isNull(), eq(signedPreKey), isNull());

    List<ECPreKey> capturedList = listCaptor.getValue();
    assertThat(capturedList.size()).isEqualTo(1);
    assertThat(capturedList.get(0).keyId()).isEqualTo(31337);
    assertThat(capturedList.get(0).publicKey()).isEqualTo(preKey.publicKey());

    verify(AuthHelper.DISABLED_ACCOUNT).setIdentityKey(eq(identityKey));
    verify(AuthHelper.DISABLED_DEVICE).setSignedPreKey(eq(signedPreKey));
    verify(accounts).update(eq(AuthHelper.DISABLED_ACCOUNT), any());
  }

  @Test
  void putIdentityKeyNonPrimary() {
    final ECPreKey preKey = KeysHelper.ecPreKey(31337);
    final ECSignedPreKey signedPreKey = KeysHelper.signedECPreKey(31338, IDENTITY_KEY_PAIR);

    List<ECPreKey> preKeys = List.of(preKey);

    PreKeyState preKeyState = new PreKeyState(IDENTITY_KEY, signedPreKey, preKeys);

    Response response =
        resources.getJerseyTest()
                 .target("/v2/keys")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_3, 2L, AuthHelper.VALID_PASSWORD_3_LINKED))
                 .put(Entity.entity(preKeyState, MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(403);
  }
}
