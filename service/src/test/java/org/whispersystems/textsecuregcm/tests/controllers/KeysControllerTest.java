/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.controllers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
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

import com.google.common.collect.ImmutableSet;
import io.dropwizard.auth.PolymorphicAuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
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
import org.whispersystems.textsecuregcm.mappers.RateLimitChallengeExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.RateLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.ServerRejectedExceptionMapper;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.Keys;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;

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

  private final PreKey SAMPLE_KEY    = new PreKey(1234, "test1");
  private final PreKey SAMPLE_KEY2   = new PreKey(5667, "test3");
  private final PreKey SAMPLE_KEY3   = new PreKey(334, "test5");
  private final PreKey SAMPLE_KEY4   = new PreKey(336, "test6");

  private final PreKey SAMPLE_KEY_PNI = new PreKey(7777, "test7");

  private final SignedPreKey SAMPLE_SIGNED_KEY       = new SignedPreKey( 1111, "foofoo", "sig11"    );
  private final SignedPreKey SAMPLE_SIGNED_KEY2      = new SignedPreKey( 2222, "foobar", "sig22"    );
  private final SignedPreKey SAMPLE_SIGNED_KEY3      = new SignedPreKey( 3333, "barfoo", "sig33"    );
  private final SignedPreKey SAMPLE_SIGNED_PNI_KEY   = new SignedPreKey( 4444, "foofoopni", "sig44" );
  private final SignedPreKey SAMPLE_SIGNED_PNI_KEY2  = new SignedPreKey( 5555, "foobarpni", "sig55" );
  private final SignedPreKey SAMPLE_SIGNED_PNI_KEY3  = new SignedPreKey( 6666, "barfoopni", "sig66" );
  private final SignedPreKey VALID_DEVICE_SIGNED_KEY = new SignedPreKey(89898, "zoofarb", "sigvalid");
  private final SignedPreKey VALID_DEVICE_PNI_SIGNED_KEY = new SignedPreKey(7777, "zoofarber", "sigvalidest");

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

  @BeforeEach
  void setup() {
    final Device sampleDevice  = mock(Device.class);
    final Device sampleDevice2 = mock(Device.class);
    final Device sampleDevice3 = mock(Device.class);
    final Device sampleDevice4 = mock(Device.class);

    Set<Device> allDevices = new HashSet<>() {{
      add(sampleDevice);
      add(sampleDevice2);
      add(sampleDevice3);
      add(sampleDevice4);
    }};

    AccountsHelper.setupMockUpdate(accounts);

    when(sampleDevice.getRegistrationId()).thenReturn(SAMPLE_REGISTRATION_ID);
    when(sampleDevice2.getRegistrationId()).thenReturn(SAMPLE_REGISTRATION_ID2);
    when(sampleDevice3.getRegistrationId()).thenReturn(SAMPLE_REGISTRATION_ID2);
    when(sampleDevice4.getRegistrationId()).thenReturn(SAMPLE_REGISTRATION_ID4);
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
    when(existsAccount.getIdentityKey()).thenReturn("existsidentitykey");
    when(existsAccount.getPhoneNumberIdentityKey()).thenReturn("existspniidentitykey");
    when(existsAccount.getNumber()).thenReturn(EXISTS_NUMBER);
    when(existsAccount.getUnidentifiedAccessKey()).thenReturn(Optional.of("1337".getBytes()));

    when(accounts.getByE164(EXISTS_NUMBER)).thenReturn(Optional.of(existsAccount));
    when(accounts.getByAccountIdentifier(EXISTS_UUID)).thenReturn(Optional.of(existsAccount));
    when(accounts.getByPhoneNumberIdentifier(EXISTS_PNI)).thenReturn(Optional.of(existsAccount));

    when(accounts.getByE164(NOT_EXISTS_NUMBER)).thenReturn(Optional.empty());
    when(accounts.getByAccountIdentifier(NOT_EXISTS_UUID)).thenReturn(Optional.empty());

    when(rateLimiters.getPreKeysLimiter()).thenReturn(rateLimiter);

    when(KEYS.take(EXISTS_UUID, 1)).thenReturn(Optional.of(SAMPLE_KEY));
    when(KEYS.take(EXISTS_PNI, 1)).thenReturn(Optional.of(SAMPLE_KEY_PNI));

    when(KEYS.getCount(AuthHelper.VALID_UUID, 1)).thenReturn(5);

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

    assertThat(result.getCount()).isEqualTo(4);

    verify(KEYS).getCount(AuthHelper.VALID_UUID, 1);
  }


  @Test
  void getSignedPreKeyV2() {
    SignedPreKey result = resources.getJerseyTest()
                                   .target("/v2/keys/signed")
                                   .request()
                                   .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                                   .get(SignedPreKey.class);

    assertThat(result.getSignature()).isEqualTo(VALID_DEVICE_SIGNED_KEY.getSignature());
    assertThat(result.getKeyId()).isEqualTo(VALID_DEVICE_SIGNED_KEY.getKeyId());
    assertThat(result.getPublicKey()).isEqualTo(VALID_DEVICE_SIGNED_KEY.getPublicKey());
  }

  @Test
  void getPhoneNumberIdentifierSignedPreKeyV2() {
    SignedPreKey result = resources.getJerseyTest()
        .target("/v2/keys/signed")
        .queryParam("identity", "pni")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(SignedPreKey.class);

    assertThat(result.getSignature()).isEqualTo(VALID_DEVICE_PNI_SIGNED_KEY.getSignature());
    assertThat(result.getKeyId()).isEqualTo(VALID_DEVICE_PNI_SIGNED_KEY.getKeyId());
    assertThat(result.getPublicKey()).isEqualTo(VALID_DEVICE_PNI_SIGNED_KEY.getPublicKey());
  }

  @Test
  void putSignedPreKeyV2() {
    SignedPreKey   test     = new SignedPreKey(9998, "fooozzz", "baaarzzz");
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
    final SignedPreKey replacementKey = new SignedPreKey(9998, "fooozzz", "baaarzzz");

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
    SignedPreKey   test     = new SignedPreKey(9999, "fooozzz", "baaarzzz");
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
    assertThat(result.getDevice(1).getPreKey().getKeyId()).isEqualTo(SAMPLE_KEY.getKeyId());
    assertThat(result.getDevice(1).getPreKey().getPublicKey()).isEqualTo(SAMPLE_KEY.getPublicKey());
    assertThat(result.getDevice(1).getSignedPreKey()).isEqualTo(existsAccount.getDevice(1).get().getSignedPreKey());

    verify(KEYS).take(EXISTS_UUID, 1);
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
    assertThat(result.getDevice(1).getPreKey().getKeyId()).isEqualTo(SAMPLE_KEY_PNI.getKeyId());
    assertThat(result.getDevice(1).getPreKey().getPublicKey()).isEqualTo(SAMPLE_KEY_PNI.getPublicKey());
    assertThat(result.getDevice(1).getSignedPreKey()).isEqualTo(existsAccount.getDevice(1).get().getPhoneNumberIdentitySignedPreKey());

    verify(KEYS).take(EXISTS_PNI, 1);
    verifyNoMoreInteractions(KEYS);
  }

  @Test
  void testGetKeysRateLimited() throws RateLimitExceededException {
    Duration retryAfter = Duration.ofSeconds(31);
    doThrow(new RateLimitExceededException(retryAfter)).when(rateLimiter).validate(anyString());

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
                                     .request()
                                     .header(OptionalAccess.UNIDENTIFIED, AuthHelper.getUnidentifiedAccessHeader("1337".getBytes()))
                                     .get(PreKeyResponse.class);

    assertThat(result.getIdentityKey()).isEqualTo(existsAccount.getIdentityKey());
    assertThat(result.getDevicesCount()).isEqualTo(1);
    assertThat(result.getDevice(1).getPreKey().getKeyId()).isEqualTo(SAMPLE_KEY.getKeyId());
    assertThat(result.getDevice(1).getPreKey().getPublicKey()).isEqualTo(SAMPLE_KEY.getPublicKey());
    assertThat(result.getDevice(1).getSignedPreKey()).isEqualTo(existsAccount.getDevice(1).get().getSignedPreKey());

    verify(KEYS).take(EXISTS_UUID, 1);
    verifyNoMoreInteractions(KEYS);
  }

  @Test
  void testNoDevices() {

    when(existsAccount.getDevices()).thenReturn(Collections.emptySet());

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
    when(KEYS.take(EXISTS_UUID, 1)).thenReturn(Optional.of(SAMPLE_KEY));
    when(KEYS.take(EXISTS_UUID, 2)).thenReturn(Optional.of(SAMPLE_KEY2));
    when(KEYS.take(EXISTS_UUID, 3)).thenReturn(Optional.of(SAMPLE_KEY3));
    when(KEYS.take(EXISTS_UUID, 4)).thenReturn(Optional.of(SAMPLE_KEY4));

    PreKeyResponse results = resources.getJerseyTest()
                                      .target(String.format("/v2/keys/%s/*", EXISTS_UUID))
                                      .request()
                                      .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                                      .get(PreKeyResponse.class);

    assertThat(results.getDevicesCount()).isEqualTo(3);
    assertThat(results.getIdentityKey()).isEqualTo(existsAccount.getIdentityKey());

    PreKey signedPreKey   = results.getDevice(1).getSignedPreKey();
    PreKey preKey         = results.getDevice(1).getPreKey();
    long   registrationId = results.getDevice(1).getRegistrationId();
    long   deviceId       = results.getDevice(1).getDeviceId();

    assertThat(preKey.getKeyId()).isEqualTo(SAMPLE_KEY.getKeyId());
    assertThat(preKey.getPublicKey()).isEqualTo(SAMPLE_KEY.getPublicKey());
    assertThat(registrationId).isEqualTo(SAMPLE_REGISTRATION_ID);
    assertThat(signedPreKey.getKeyId()).isEqualTo(SAMPLE_SIGNED_KEY.getKeyId());
    assertThat(signedPreKey.getPublicKey()).isEqualTo(SAMPLE_SIGNED_KEY.getPublicKey());
    assertThat(deviceId).isEqualTo(1);

    signedPreKey   = results.getDevice(2).getSignedPreKey();
    preKey         = results.getDevice(2).getPreKey();
    registrationId = results.getDevice(2).getRegistrationId();
    deviceId       = results.getDevice(2).getDeviceId();

    assertThat(preKey.getKeyId()).isEqualTo(SAMPLE_KEY2.getKeyId());
    assertThat(preKey.getPublicKey()).isEqualTo(SAMPLE_KEY2.getPublicKey());
    assertThat(registrationId).isEqualTo(SAMPLE_REGISTRATION_ID2);
    assertThat(signedPreKey.getKeyId()).isEqualTo(SAMPLE_SIGNED_KEY2.getKeyId());
    assertThat(signedPreKey.getPublicKey()).isEqualTo(SAMPLE_SIGNED_KEY2.getPublicKey());
    assertThat(deviceId).isEqualTo(2);

    signedPreKey   = results.getDevice(4).getSignedPreKey();
    preKey         = results.getDevice(4).getPreKey();
    registrationId = results.getDevice(4).getRegistrationId();
    deviceId       = results.getDevice(4).getDeviceId();

    assertThat(preKey.getKeyId()).isEqualTo(SAMPLE_KEY4.getKeyId());
    assertThat(preKey.getPublicKey()).isEqualTo(SAMPLE_KEY4.getPublicKey());
    assertThat(registrationId).isEqualTo(SAMPLE_REGISTRATION_ID4);
    assertThat(signedPreKey).isNull();
    assertThat(deviceId).isEqualTo(4);

    verify(KEYS).take(EXISTS_UUID, 1);
    verify(KEYS).take(EXISTS_UUID, 2);
    verify(KEYS).take(EXISTS_UUID, 3);
    verify(KEYS).take(EXISTS_UUID, 4);
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
    final PreKey       preKey       = new PreKey(31337, "foobar");
    final SignedPreKey signedPreKey = new SignedPreKey(31338, "foobaz", "myvalidsig");
    final String       identityKey  = "barbar";

    List<PreKey> preKeys = new LinkedList<PreKey>() {{
      add(preKey);
    }};

    PreKeyState preKeyState = new PreKeyState(identityKey, signedPreKey, preKeys);

    Response response =
        resources.getJerseyTest()
                 .target("/v2/keys")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                 .put(Entity.entity(preKeyState, MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(204);

    ArgumentCaptor<List<PreKey>> listCaptor = ArgumentCaptor.forClass(List.class);
    verify(KEYS).store(eq(AuthHelper.VALID_UUID), eq(1L), listCaptor.capture());

    List<PreKey> capturedList = listCaptor.getValue();
    assertThat(capturedList.size()).isEqualTo(1);
    assertThat(capturedList.get(0).getKeyId()).isEqualTo(31337);
    assertThat(capturedList.get(0).getPublicKey()).isEqualTo("foobar");

    verify(AuthHelper.VALID_ACCOUNT).setIdentityKey(eq("barbar"));
    verify(AuthHelper.VALID_DEVICE).setSignedPreKey(eq(signedPreKey));
    verify(accounts).update(eq(AuthHelper.VALID_ACCOUNT), any());
  }

  @Test
  void putKeysByPhoneNumberIdentifierTestV2() {
    final SignedPreKey signedPreKey = new SignedPreKey(31338, "foobaz", "myvalidsig");
    final String       identityKey  = "barbar";

    List<PreKey> preKeys = List.of(new PreKey(31337, "foobar"));

    PreKeyState preKeyState = new PreKeyState(identityKey, signedPreKey, preKeys);

    Response response =
        resources.getJerseyTest()
            .target("/v2/keys")
            .queryParam("identity", "pni")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(preKeyState, MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(204);

    ArgumentCaptor<List<PreKey>> listCaptor = ArgumentCaptor.forClass(List.class);
    verify(KEYS).store(eq(AuthHelper.VALID_PNI), eq(1L), listCaptor.capture());

    List<PreKey> capturedList = listCaptor.getValue();
    assertThat(capturedList.size()).isEqualTo(1);
    assertThat(capturedList.get(0).getKeyId()).isEqualTo(31337);
    assertThat(capturedList.get(0).getPublicKey()).isEqualTo("foobar");

    verify(AuthHelper.VALID_ACCOUNT).setPhoneNumberIdentityKey(eq("barbar"));
    verify(AuthHelper.VALID_DEVICE).setPhoneNumberIdentitySignedPreKey(eq(signedPreKey));
    verify(accounts).update(eq(AuthHelper.VALID_ACCOUNT), any());
  }

  @Test
  void disabledPutKeysTestV2() {
    final PreKey       preKey       = new PreKey(31337, "foobar");
    final SignedPreKey signedPreKey = new SignedPreKey(31338, "foobaz", "myvalidsig");
    final String       identityKey  = "barbar";

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
    verify(KEYS).store(eq(AuthHelper.DISABLED_UUID), eq(1L), listCaptor.capture());

    List<PreKey> capturedList = listCaptor.getValue();
    assertThat(capturedList.size()).isEqualTo(1);
    assertThat(capturedList.get(0).getKeyId()).isEqualTo(31337);
    assertThat(capturedList.get(0).getPublicKey()).isEqualTo("foobar");

    verify(AuthHelper.DISABLED_ACCOUNT).setIdentityKey(eq("barbar"));
    verify(AuthHelper.DISABLED_DEVICE).setSignedPreKey(eq(signedPreKey));
    verify(accounts).update(eq(AuthHelper.DISABLED_ACCOUNT), any());
  }

  @Test
  void putIdentityKeyNonPrimary() {
    final PreKey       preKey       = new PreKey(31337, "foobar");
    final SignedPreKey signedPreKey = new SignedPreKey(31338, "foobaz", "myvalidsig");
    final String       identityKey  = "barbar";

    List<PreKey> preKeys = List.of(preKey);

    PreKeyState preKeyState = new PreKeyState(identityKey, signedPreKey, preKeys);

    Response response =
        resources.getJerseyTest()
                 .target("/v2/keys")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_3, 2L, AuthHelper.VALID_PASSWORD_3_LINKED))
                 .put(Entity.entity(preKeyState, MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(403);
  }
}
