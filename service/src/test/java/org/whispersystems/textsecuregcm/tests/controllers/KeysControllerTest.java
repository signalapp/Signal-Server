/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.controllers;

import static org.assertj.core.api.Assertions.assertThat;
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

import com.google.common.collect.ImmutableSet;
import io.dropwizard.auth.PolymorphicAuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.DisabledPermittedAuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.OptionalAccess;
import org.whispersystems.textsecuregcm.controllers.KeysController;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.entities.PreKey;
import org.whispersystems.textsecuregcm.entities.PreKeyCount;
import org.whispersystems.textsecuregcm.entities.PreKeyResponse;
import org.whispersystems.textsecuregcm.entities.PreKeyState;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.mappers.RateLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.ServerRejectedExceptionMapper;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.Keys;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.tests.util.KeysHelper;

@ExtendWith(DropwizardExtensionsSupport.class)
class KeysControllerTest {

  private static final String EXISTS_NUMBER = "+14152222222";
  private static final UUID   EXISTS_UUID   = UUID.randomUUID();
  private static final UUID   EXISTS_PNI    = UUID.randomUUID();

  private static final String NOT_EXISTS_NUMBER = "+14152222220";
  private static final UUID   NOT_EXISTS_UUID   = UUID.randomUUID();

  private static final int SAMPLE_REGISTRATION_ID  =  999;
  private static final int SAMPLE_REGISTRATION_ID2 = 1002;
  private static final int SAMPLE_REGISTRATION_ID4 = 1555;

  private static final int SAMPLE_PNI_REGISTRATION_ID = 1717;

  private final ECKeyPair IDENTITY_KEY_PAIR = Curve.generateKeyPair();
  private final String IDENTITY_KEY = KeysHelper.serializeIdentityKey(IDENTITY_KEY_PAIR);
  
  private final ECKeyPair PNI_IDENTITY_KEY_PAIR = Curve.generateKeyPair();
  private final String PNI_IDENTITY_KEY = KeysHelper.serializeIdentityKey(PNI_IDENTITY_KEY_PAIR);
  
  private final PreKey SAMPLE_KEY = new PreKey(1234, "test1");
  private final PreKey SAMPLE_KEY2 = new PreKey(5667, "test3");
  private final PreKey SAMPLE_KEY3 = new PreKey(334, "test5");
  private final PreKey SAMPLE_KEY4 = new PreKey(336, "test6");

  private final PreKey SAMPLE_KEY_PNI = new PreKey(7777, "test7");

  private final SignedPreKey SAMPLE_PQ_KEY = new SignedPreKey(2424, "test1", "sig");
  private final SignedPreKey SAMPLE_PQ_KEY2 = new SignedPreKey(6868, "test3", "sig");
  private final SignedPreKey SAMPLE_PQ_KEY3 = new SignedPreKey(1313, "test5", "sig");

  private final SignedPreKey SAMPLE_PQ_KEY_PNI = new SignedPreKey(8888, "test7", "sig");

  private final SignedPreKey SAMPLE_SIGNED_KEY = KeysHelper.signedPreKey(1111, IDENTITY_KEY_PAIR);
  private final SignedPreKey SAMPLE_SIGNED_KEY2 = KeysHelper.signedPreKey(2222, IDENTITY_KEY_PAIR);
  private final SignedPreKey SAMPLE_SIGNED_KEY3 = KeysHelper.signedPreKey(3333, IDENTITY_KEY_PAIR);
  private final SignedPreKey SAMPLE_SIGNED_PNI_KEY = KeysHelper.signedPreKey(4444, PNI_IDENTITY_KEY_PAIR);
  private final SignedPreKey SAMPLE_SIGNED_PNI_KEY2 = KeysHelper.signedPreKey(5555, PNI_IDENTITY_KEY_PAIR);
  private final SignedPreKey SAMPLE_SIGNED_PNI_KEY3 = KeysHelper.signedPreKey(6666, PNI_IDENTITY_KEY_PAIR);
  private final SignedPreKey VALID_DEVICE_SIGNED_KEY = KeysHelper.signedPreKey(89898, IDENTITY_KEY_PAIR);
  private final SignedPreKey VALID_DEVICE_PNI_SIGNED_KEY = KeysHelper.signedPreKey(7777, PNI_IDENTITY_KEY_PAIR);

  private final static Keys KEYS = mock(Keys.class               );
  private final static AccountsManager             accounts                    = mock(AccountsManager.class            );
  private final static Account                     existsAccount               = mock(Account.class                    );

  private static final RateLimiters          rateLimiters  = mock(RateLimiters.class);
  private static final RateLimiter           rateLimiter   = mock(RateLimiter.class );

  private static final ResourceExtension resources = ResourceExtension.builder()
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(new PolymorphicAuthValueFactoryProvider.Binder<>(ImmutableSet.of(
          AuthenticatedAccount.class, DisabledPermittedAuthenticatedAccount.class)))
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(new ServerRejectedExceptionMapper())
      .addResource(new KeysController(rateLimiters, KEYS, accounts))
      .addResource(new RateLimitExceededExceptionMapper())
      .build();

  private Device sampleDevice;

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
    when(sampleDevice.getSignedPreKey()).thenReturn(SAMPLE_SIGNED_KEY);
    when(sampleDevice2.getSignedPreKey()).thenReturn(SAMPLE_SIGNED_KEY2);
    when(sampleDevice3.getSignedPreKey()).thenReturn(SAMPLE_SIGNED_KEY3);
    when(sampleDevice4.getSignedPreKey()).thenReturn(null);
    when(sampleDevice.getPhoneNumberIdentitySignedPreKey()).thenReturn(SAMPLE_SIGNED_PNI_KEY);
    when(sampleDevice2.getPhoneNumberIdentitySignedPreKey()).thenReturn(SAMPLE_SIGNED_PNI_KEY2);
    when(sampleDevice3.getPhoneNumberIdentitySignedPreKey()).thenReturn(SAMPLE_SIGNED_PNI_KEY3);
    when(sampleDevice4.getPhoneNumberIdentitySignedPreKey()).thenReturn(null);
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
    when(existsAccount.getIdentityKey()).thenReturn(IDENTITY_KEY);
    when(existsAccount.getPhoneNumberIdentityKey()).thenReturn(PNI_IDENTITY_KEY);
    when(existsAccount.getNumber()).thenReturn(EXISTS_NUMBER);
    when(existsAccount.getUnidentifiedAccessKey()).thenReturn(Optional.of("1337".getBytes()));

    when(accounts.getByE164(EXISTS_NUMBER)).thenReturn(Optional.of(existsAccount));
    when(accounts.getByAccountIdentifier(EXISTS_UUID)).thenReturn(Optional.of(existsAccount));
    when(accounts.getByPhoneNumberIdentifier(EXISTS_PNI)).thenReturn(Optional.of(existsAccount));

    when(accounts.getByE164(NOT_EXISTS_NUMBER)).thenReturn(Optional.empty());
    when(accounts.getByAccountIdentifier(NOT_EXISTS_UUID)).thenReturn(Optional.empty());

    when(rateLimiters.getPreKeysLimiter()).thenReturn(rateLimiter);

    when(KEYS.takeEC(EXISTS_UUID, 1)).thenReturn(Optional.of(SAMPLE_KEY));
    when(KEYS.takePQ(EXISTS_UUID, 1)).thenReturn(Optional.of(SAMPLE_PQ_KEY));
    when(KEYS.takeEC(EXISTS_PNI, 1)).thenReturn(Optional.of(SAMPLE_KEY_PNI));
    when(KEYS.takePQ(EXISTS_PNI, 1)).thenReturn(Optional.of(SAMPLE_PQ_KEY_PNI));

    when(KEYS.getEcCount(AuthHelper.VALID_UUID, 1)).thenReturn(5);
    when(KEYS.getPqCount(AuthHelper.VALID_UUID, 1)).thenReturn(5);

    when(AuthHelper.VALID_DEVICE.getSignedPreKey()).thenReturn(VALID_DEVICE_SIGNED_KEY);
    when(AuthHelper.VALID_DEVICE.getPhoneNumberIdentitySignedPreKey()).thenReturn(VALID_DEVICE_PNI_SIGNED_KEY);
    when(AuthHelper.VALID_ACCOUNT.getIdentityKey()).thenReturn(null);
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
  void getSignedPreKeyV2() {
    SignedPreKey result = resources.getJerseyTest()
                                   .target("/v2/keys/signed")
                                   .request()
                                   .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                                   .get(SignedPreKey.class);

    assertKeysMatch(VALID_DEVICE_SIGNED_KEY, result);
  }

  @Test
  void getPhoneNumberIdentifierSignedPreKeyV2() {
    SignedPreKey result = resources.getJerseyTest()
        .target("/v2/keys/signed")
        .queryParam("identity", "pni")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(SignedPreKey.class);

    assertKeysMatch(VALID_DEVICE_PNI_SIGNED_KEY, result);
  }

  @Test
  void putSignedPreKeyV2() {
    SignedPreKey   test     = KeysHelper.signedPreKey(9998, IDENTITY_KEY_PAIR);
    Response response = resources.getJerseyTest()
                                 .target("/v2/keys/signed")
                                 .request()
                                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                                 .put(Entity.entity(test, MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(204);

    verify(AuthHelper.VALID_DEVICE).setSignedPreKey(eq(test));
    verify(AuthHelper.VALID_DEVICE, never()).setPhoneNumberIdentitySignedPreKey(any());
    verify(accounts).updateDevice(eq(AuthHelper.VALID_ACCOUNT), anyLong(), any());
  }

  @Test
  void putPhoneNumberIdentitySignedPreKeyV2() {
    final SignedPreKey replacementKey = KeysHelper.signedPreKey(9998, PNI_IDENTITY_KEY_PAIR);

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
  }

  @Test
  void disabledPutSignedPreKeyV2() {
    SignedPreKey   test     = KeysHelper.signedPreKey(9999, IDENTITY_KEY_PAIR);
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

    assertThat(result.getIdentityKey()).isEqualTo(existsAccount.getIdentityKey());
    assertThat(result.getDevicesCount()).isEqualTo(1);
    assertKeysMatch(SAMPLE_KEY, result.getDevice(1).getPreKey());
    assertThat(result.getDevice(1).getPqPreKey()).isNull();
    assertThat(result.getDevice(1).getRegistrationId()).isEqualTo(SAMPLE_REGISTRATION_ID);
    assertKeysMatch(existsAccount.getDevice(1).get().getSignedPreKey(), result.getDevice(1).getSignedPreKey());

    verify(KEYS).takeEC(EXISTS_UUID, 1);
    verifyNoMoreInteractions(KEYS);
  }

  @Test
  void validSingleRequestPqTestNoPqKeysV2() {
    when(KEYS.takePQ(EXISTS_UUID, 1)).thenReturn(Optional.<SignedPreKey>empty());

    PreKeyResponse result = resources.getJerseyTest()
        .target(String.format("/v2/keys/%s/1", EXISTS_UUID))
        .queryParam("pq", "true")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(PreKeyResponse.class);

    assertThat(result.getIdentityKey()).isEqualTo(existsAccount.getIdentityKey());
    assertThat(result.getDevicesCount()).isEqualTo(1);
    assertKeysMatch(SAMPLE_KEY, result.getDevice(1).getPreKey());
    assertThat(result.getDevice(1).getPqPreKey()).isNull();
    assertThat(result.getDevice(1).getRegistrationId()).isEqualTo(SAMPLE_REGISTRATION_ID);
    assertKeysMatch(existsAccount.getDevice(1).get().getSignedPreKey(), result.getDevice(1).getSignedPreKey());

    verify(KEYS).takeEC(EXISTS_UUID, 1);
    verify(KEYS).takePQ(EXISTS_UUID, 1);
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

    assertThat(result.getIdentityKey()).isEqualTo(existsAccount.getIdentityKey());
    assertThat(result.getDevicesCount()).isEqualTo(1);
    assertKeysMatch(SAMPLE_KEY, result.getDevice(1).getPreKey());
    assertKeysMatch(SAMPLE_PQ_KEY, result.getDevice(1).getPqPreKey());
    assertThat(result.getDevice(1).getRegistrationId()).isEqualTo(SAMPLE_REGISTRATION_ID);
    assertKeysMatch(existsAccount.getDevice(1).get().getSignedPreKey(), result.getDevice(1).getSignedPreKey());

    verify(KEYS).takeEC(EXISTS_UUID, 1);
    verify(KEYS).takePQ(EXISTS_UUID, 1);
    verifyNoMoreInteractions(KEYS);
  }

  @Test
  void validSingleRequestByPhoneNumberIdentifierTestV2() {
    PreKeyResponse result = resources.getJerseyTest()
        .target(String.format("/v2/keys/%s/1", EXISTS_PNI))
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(PreKeyResponse.class);

    assertThat(result.getIdentityKey()).isEqualTo(existsAccount.getPhoneNumberIdentityKey());
    assertThat(result.getDevicesCount()).isEqualTo(1);
    assertKeysMatch(SAMPLE_KEY_PNI, result.getDevice(1).getPreKey());
    assertThat(result.getDevice(1).getPqPreKey()).isNull();
    assertThat(result.getDevice(1).getRegistrationId()).isEqualTo(SAMPLE_PNI_REGISTRATION_ID);
    assertKeysMatch(existsAccount.getDevice(1).get().getPhoneNumberIdentitySignedPreKey(), result.getDevice(1).getSignedPreKey());

    verify(KEYS).takeEC(EXISTS_PNI, 1);
    verifyNoMoreInteractions(KEYS);
  }

  @Test
  void validSingleRequestPqByPhoneNumberIdentifierTestV2() {
    PreKeyResponse result = resources.getJerseyTest()
        .target(String.format("/v2/keys/%s/1", EXISTS_PNI))
        .queryParam("pq", "true")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(PreKeyResponse.class);

    assertThat(result.getIdentityKey()).isEqualTo(existsAccount.getPhoneNumberIdentityKey());
    assertThat(result.getDevicesCount()).isEqualTo(1);
    assertKeysMatch(SAMPLE_KEY_PNI, result.getDevice(1).getPreKey());
    assertThat(result.getDevice(1).getPqPreKey()).isEqualTo(SAMPLE_PQ_KEY_PNI);
    assertThat(result.getDevice(1).getRegistrationId()).isEqualTo(SAMPLE_PNI_REGISTRATION_ID);
    assertKeysMatch(existsAccount.getDevice(1).get().getPhoneNumberIdentitySignedPreKey(), result.getDevice(1).getSignedPreKey());

    verify(KEYS).takeEC(EXISTS_PNI, 1);
    verify(KEYS).takePQ(EXISTS_PNI, 1);
    verifyNoMoreInteractions(KEYS);
  }

  @Test
  void validSingleRequestByPhoneNumberIdentifierNoPniRegistrationIdTestV2() {
    when(sampleDevice.getPhoneNumberIdentityRegistrationId()).thenReturn(OptionalInt.empty());

    PreKeyResponse result = resources.getJerseyTest()
        .target(String.format("/v2/keys/%s/1", EXISTS_PNI))
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(PreKeyResponse.class);

    assertThat(result.getIdentityKey()).isEqualTo(existsAccount.getPhoneNumberIdentityKey());
    assertThat(result.getDevicesCount()).isEqualTo(1);
    assertKeysMatch(SAMPLE_KEY_PNI, result.getDevice(1).getPreKey());
    assertThat(result.getDevice(1).getPqPreKey()).isNull();
    assertThat(result.getDevice(1).getRegistrationId()).isEqualTo(SAMPLE_REGISTRATION_ID);
    assertKeysMatch(existsAccount.getDevice(1).get().getPhoneNumberIdentitySignedPreKey(), result.getDevice(1).getSignedPreKey());

    verify(KEYS).takeEC(EXISTS_PNI, 1);
    verifyNoMoreInteractions(KEYS);
  }

  @Test
  void testGetKeysRateLimited() throws RateLimitExceededException {
    Duration retryAfter = Duration.ofSeconds(31);
    doThrow(new RateLimitExceededException(retryAfter, true)).when(rateLimiter).validate(anyString());

    Response result = resources.getJerseyTest()
        .target(String.format("/v2/keys/%s/*", EXISTS_PNI))
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

    assertThat(result.getIdentityKey()).isEqualTo(existsAccount.getIdentityKey());
    assertThat(result.getDevicesCount()).isEqualTo(1);
    assertKeysMatch(SAMPLE_KEY, result.getDevice(1).getPreKey());
    assertKeysMatch(SAMPLE_PQ_KEY, result.getDevice(1).getPqPreKey());
    assertKeysMatch(existsAccount.getDevice(1).get().getSignedPreKey(), result.getDevice(1).getSignedPreKey());

    verify(KEYS).takeEC(EXISTS_UUID, 1);
    verify(KEYS).takePQ(EXISTS_UUID, 1);
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
    when(KEYS.takeEC(EXISTS_UUID, 1)).thenReturn(Optional.of(SAMPLE_KEY));
    when(KEYS.takeEC(EXISTS_UUID, 2)).thenReturn(Optional.of(SAMPLE_KEY2));
    when(KEYS.takeEC(EXISTS_UUID, 3)).thenReturn(Optional.of(SAMPLE_KEY3));
    when(KEYS.takeEC(EXISTS_UUID, 4)).thenReturn(Optional.of(SAMPLE_KEY4));

    PreKeyResponse results = resources.getJerseyTest()
        .target(String.format("/v2/keys/%s/*", EXISTS_UUID))
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(PreKeyResponse.class);

    assertThat(results.getDevicesCount()).isEqualTo(3);
    assertThat(results.getIdentityKey()).isEqualTo(existsAccount.getIdentityKey());

    PreKey signedPreKey = results.getDevice(1).getSignedPreKey();
    PreKey preKey = results.getDevice(1).getPreKey();
    long registrationId = results.getDevice(1).getRegistrationId();
    long deviceId = results.getDevice(1).getDeviceId();

    assertKeysMatch(SAMPLE_KEY, preKey);
    assertThat(registrationId).isEqualTo(SAMPLE_REGISTRATION_ID);
    assertKeysMatch(SAMPLE_SIGNED_KEY, signedPreKey);
    assertThat(deviceId).isEqualTo(1);

    signedPreKey = results.getDevice(2).getSignedPreKey();
    preKey = results.getDevice(2).getPreKey();
    registrationId = results.getDevice(2).getRegistrationId();
    deviceId = results.getDevice(2).getDeviceId();

    assertKeysMatch(SAMPLE_KEY2, preKey);
    assertThat(registrationId).isEqualTo(SAMPLE_REGISTRATION_ID2);
    assertKeysMatch(SAMPLE_SIGNED_KEY2, signedPreKey);
    assertThat(deviceId).isEqualTo(2);

    signedPreKey = results.getDevice(4).getSignedPreKey();
    preKey = results.getDevice(4).getPreKey();
    registrationId = results.getDevice(4).getRegistrationId();
    deviceId = results.getDevice(4).getDeviceId();

    assertKeysMatch(SAMPLE_KEY4, preKey);
    assertThat(registrationId).isEqualTo(SAMPLE_REGISTRATION_ID4);
    assertThat(signedPreKey).isNull();
    assertThat(deviceId).isEqualTo(4);

    verify(KEYS).takeEC(EXISTS_UUID, 1);
    verify(KEYS).takeEC(EXISTS_UUID, 2);
    verify(KEYS).takeEC(EXISTS_UUID, 4);
    verifyNoMoreInteractions(KEYS);
  }

  @Test
  void validMultiRequestPqTestV2() {
    when(KEYS.takeEC(EXISTS_UUID, 1)).thenReturn(Optional.of(SAMPLE_KEY));
    when(KEYS.takeEC(EXISTS_UUID, 3)).thenReturn(Optional.of(SAMPLE_KEY3));
    when(KEYS.takeEC(EXISTS_UUID, 4)).thenReturn(Optional.of(SAMPLE_KEY4));
    when(KEYS.takePQ(EXISTS_UUID, 1)).thenReturn(Optional.of(SAMPLE_PQ_KEY));
    when(KEYS.takePQ(EXISTS_UUID, 2)).thenReturn(Optional.of(SAMPLE_PQ_KEY2));
    when(KEYS.takePQ(EXISTS_UUID, 3)).thenReturn(Optional.of(SAMPLE_PQ_KEY3));
    when(KEYS.takePQ(EXISTS_UUID, 4)).thenReturn(Optional.<SignedPreKey>empty());

    PreKeyResponse results = resources.getJerseyTest()
        .target(String.format("/v2/keys/%s/*", EXISTS_UUID))
        .queryParam("pq", "true")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(PreKeyResponse.class);

    assertThat(results.getDevicesCount()).isEqualTo(3);
    assertThat(results.getIdentityKey()).isEqualTo(existsAccount.getIdentityKey());

    PreKey signedPreKey = results.getDevice(1).getSignedPreKey();
    PreKey preKey = results.getDevice(1).getPreKey();
    SignedPreKey pqPreKey = results.getDevice(1).getPqPreKey();
    long registrationId = results.getDevice(1).getRegistrationId();
    long deviceId = results.getDevice(1).getDeviceId();

    assertKeysMatch(SAMPLE_KEY, preKey);
    assertKeysMatch(SAMPLE_PQ_KEY, pqPreKey);
    assertThat(registrationId).isEqualTo(SAMPLE_REGISTRATION_ID);
    assertKeysMatch(SAMPLE_SIGNED_KEY, signedPreKey);
    assertThat(deviceId).isEqualTo(1);

    signedPreKey = results.getDevice(2).getSignedPreKey();
    preKey = results.getDevice(2).getPreKey();
    pqPreKey = results.getDevice(2).getPqPreKey();
    registrationId = results.getDevice(2).getRegistrationId();
    deviceId = results.getDevice(2).getDeviceId();

    assertThat(preKey).isNull();
    assertKeysMatch(SAMPLE_PQ_KEY2, pqPreKey);
    assertThat(registrationId).isEqualTo(SAMPLE_REGISTRATION_ID2);
    assertKeysMatch(SAMPLE_SIGNED_KEY2, signedPreKey);
    assertThat(deviceId).isEqualTo(2);

    signedPreKey = results.getDevice(4).getSignedPreKey();
    preKey = results.getDevice(4).getPreKey();
    pqPreKey = results.getDevice(4).getPqPreKey();
    registrationId = results.getDevice(4).getRegistrationId();
    deviceId = results.getDevice(4).getDeviceId();

    assertKeysMatch(SAMPLE_KEY4, preKey);
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
    final PreKey preKey = new PreKey(31337, "foobar");
    final ECKeyPair identityKeyPair = Curve.generateKeyPair();
    final SignedPreKey signedPreKey = KeysHelper.signedPreKey(31338, identityKeyPair);
    final String identityKey = KeysHelper.serializeIdentityKey(identityKeyPair);

    PreKeyState preKeyState = new PreKeyState(identityKey, signedPreKey, List.of(preKey));

    Response response =
        resources.getJerseyTest()
                 .target("/v2/keys")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                 .put(Entity.entity(preKeyState, MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(204);

    ArgumentCaptor<List<PreKey>> listCaptor = ArgumentCaptor.forClass(List.class);
    verify(KEYS).store(eq(AuthHelper.VALID_UUID), eq(1L), listCaptor.capture(), isNull(), isNull());

    assertThat(listCaptor.getValue()).containsExactly(preKey);

    verify(AuthHelper.VALID_ACCOUNT).setIdentityKey(eq(identityKey));
    verify(AuthHelper.VALID_DEVICE).setSignedPreKey(eq(signedPreKey));
    verify(accounts).update(eq(AuthHelper.VALID_ACCOUNT), any());
  }

  @Test
  void putKeysPqTestV2() {
    final PreKey preKey = new PreKey(31337, "foobar");
    final ECKeyPair identityKeyPair = Curve.generateKeyPair();
    final SignedPreKey signedPreKey = KeysHelper.signedPreKey(31338, identityKeyPair);
    final SignedPreKey pqPreKey = KeysHelper.signedPreKey(31339, identityKeyPair);
    final SignedPreKey pqLastResortPreKey = KeysHelper.signedPreKey(31340, identityKeyPair);
    final String identityKey = KeysHelper.serializeIdentityKey(identityKeyPair);

    PreKeyState preKeyState = new PreKeyState(identityKey, signedPreKey, List.of(preKey), List.of(pqPreKey), pqLastResortPreKey);

    Response response =
        resources.getJerseyTest()
                 .target("/v2/keys")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                 .put(Entity.entity(preKeyState, MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(204);

    ArgumentCaptor<List<PreKey>> ecCaptor = ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<List<SignedPreKey>> pqCaptor = ArgumentCaptor.forClass(List.class);
    verify(KEYS).store(eq(AuthHelper.VALID_UUID), eq(1L), ecCaptor.capture(), pqCaptor.capture(), eq(pqLastResortPreKey));

    assertThat(ecCaptor.getValue()).containsExactly(preKey);
    assertThat(pqCaptor.getValue()).containsExactly(pqPreKey);

    verify(AuthHelper.VALID_ACCOUNT).setIdentityKey(eq(identityKey));
    verify(AuthHelper.VALID_DEVICE).setSignedPreKey(eq(signedPreKey));
    verify(accounts).update(eq(AuthHelper.VALID_ACCOUNT), any());
  }

  @Test
  void putKeysByPhoneNumberIdentifierTestV2() {
    final PreKey preKey = new PreKey(31337, "foobar");
    final ECKeyPair identityKeyPair = Curve.generateKeyPair();
    final SignedPreKey signedPreKey = KeysHelper.signedPreKey(31338, identityKeyPair);
    final String identityKey = KeysHelper.serializeIdentityKey(identityKeyPair);

    PreKeyState preKeyState = new PreKeyState(identityKey, signedPreKey, List.of(preKey));

    Response response =
        resources.getJerseyTest()
            .target("/v2/keys")
            .queryParam("identity", "pni")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(preKeyState, MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(204);

    ArgumentCaptor<List<PreKey>> listCaptor = ArgumentCaptor.forClass(List.class);
    verify(KEYS).store(eq(AuthHelper.VALID_PNI), eq(1L), listCaptor.capture(), isNull(), isNull());

    assertThat(listCaptor.getValue()).containsExactly(preKey);

    verify(AuthHelper.VALID_ACCOUNT).setPhoneNumberIdentityKey(eq(identityKey));
    verify(AuthHelper.VALID_DEVICE).setPhoneNumberIdentitySignedPreKey(eq(signedPreKey));
    verify(accounts).update(eq(AuthHelper.VALID_ACCOUNT), any());
  }

  @Test
  void putKeysByPhoneNumberIdentifierPqTestV2() {
    final PreKey preKey = new PreKey(31337, "foobar");
    final ECKeyPair identityKeyPair = Curve.generateKeyPair();
    final SignedPreKey signedPreKey = KeysHelper.signedPreKey(31338, identityKeyPair);
    final SignedPreKey pqPreKey = KeysHelper.signedPreKey(31339, identityKeyPair);
    final SignedPreKey pqLastResortPreKey = KeysHelper.signedPreKey(31340, identityKeyPair);
    final String identityKey = KeysHelper.serializeIdentityKey(identityKeyPair);

    PreKeyState preKeyState = new PreKeyState(identityKey, signedPreKey, List.of(preKey), List.of(pqPreKey), pqLastResortPreKey);

    Response response =
        resources.getJerseyTest()
            .target("/v2/keys")
            .queryParam("identity", "pni")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(preKeyState, MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(204);

    ArgumentCaptor<List<PreKey>> ecCaptor = ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<List<SignedPreKey>> pqCaptor = ArgumentCaptor.forClass(List.class);
    verify(KEYS).store(eq(AuthHelper.VALID_PNI), eq(1L), ecCaptor.capture(), pqCaptor.capture(), eq(pqLastResortPreKey));

    assertThat(ecCaptor.getValue()).containsExactly(preKey);
    assertThat(pqCaptor.getValue()).containsExactly(pqPreKey);

    verify(AuthHelper.VALID_ACCOUNT).setPhoneNumberIdentityKey(eq(identityKey));
    verify(AuthHelper.VALID_DEVICE).setPhoneNumberIdentitySignedPreKey(eq(signedPreKey));
    verify(accounts).update(eq(AuthHelper.VALID_ACCOUNT), any());
  }

  @Test
  void putPrekeyWithInvalidSignature() {
    final SignedPreKey badSignedPreKey = new SignedPreKey(1L, "foo", "bar");
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
    final PreKey       preKey       = new PreKey(31337, "foobar");
    final ECKeyPair identityKeyPair = Curve.generateKeyPair();
    final SignedPreKey signedPreKey = KeysHelper.signedPreKey(31338, identityKeyPair);
    final String       identityKey  = KeysHelper.serializeIdentityKey(identityKeyPair);

    List<PreKey> preKeys = new LinkedList<PreKey>() {{
      add(preKey);
    }};

    PreKeyState preKeyState = new PreKeyState(identityKey, signedPreKey, preKeys);

    Response response =
        resources.getJerseyTest()
            .target("/v2/keys")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.DISABLED_UUID, AuthHelper.DISABLED_PASSWORD))
            .put(Entity.entity(preKeyState, MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(204);

    ArgumentCaptor<List<PreKey>> listCaptor = ArgumentCaptor.forClass(List.class);
    verify(KEYS).store(eq(AuthHelper.DISABLED_UUID), eq(1L), listCaptor.capture(), isNull(), isNull());

    List<PreKey> capturedList = listCaptor.getValue();
    assertThat(capturedList.size()).isEqualTo(1);
    assertThat(capturedList.get(0).getKeyId()).isEqualTo(31337);
    assertThat(capturedList.get(0).getPublicKey()).isEqualTo("foobar");

    verify(AuthHelper.DISABLED_ACCOUNT).setIdentityKey(eq(identityKey));
    verify(AuthHelper.DISABLED_DEVICE).setSignedPreKey(eq(signedPreKey));
    verify(accounts).update(eq(AuthHelper.DISABLED_ACCOUNT), any());
  }

  @Test
  void putIdentityKeyNonPrimary() {
    final PreKey       preKey       = new PreKey(31337, "foobar");
    final SignedPreKey signedPreKey = KeysHelper.signedPreKey(31338, IDENTITY_KEY_PAIR);

    List<PreKey> preKeys = List.of(preKey);

    PreKeyState preKeyState = new PreKeyState(IDENTITY_KEY, signedPreKey, preKeys);

    Response response =
        resources.getJerseyTest()
                 .target("/v2/keys")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_3, 2L, AuthHelper.VALID_PASSWORD_3_LINKED))
                 .put(Entity.entity(preKeyState, MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(403);
  }

  private void assertKeysMatch(PreKey expected, PreKey actual) {
    assertThat(actual.getKeyId()).isEqualTo(expected.getKeyId());
    assertThat(actual.getPublicKey()).isEqualTo(expected.getPublicKey());
    if (expected instanceof final SignedPreKey signedExpected) {
      final SignedPreKey signedActual = (SignedPreKey) actual;
      assertThat(signedActual.getSignature()).isEqualTo(signedExpected.getSignature());
    }
  }
}
