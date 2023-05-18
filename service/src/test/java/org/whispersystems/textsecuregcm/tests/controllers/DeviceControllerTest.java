/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.tests.controllers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.google.common.net.HttpHeaders;
import io.dropwizard.auth.PolymorphicAuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import javax.ws.rs.Path;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.DisabledPermittedAuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.StoredVerificationCode;
import org.whispersystems.textsecuregcm.auth.WebsocketRefreshApplicationEventListener;
import org.whispersystems.textsecuregcm.controllers.DeviceController;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.entities.ApnRegistrationId;
import org.whispersystems.textsecuregcm.entities.DeviceActivationRequest;
import org.whispersystems.textsecuregcm.entities.DeviceResponse;
import org.whispersystems.textsecuregcm.entities.GcmRegistrationId;
import org.whispersystems.textsecuregcm.entities.LinkDeviceRequest;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.mappers.DeviceLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.push.ClientPresenceManager;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.Device.DeviceCapabilities;
import org.whispersystems.textsecuregcm.storage.Keys;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.storage.StoredVerificationCodeManager;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.tests.util.KeysHelper;
import org.whispersystems.textsecuregcm.util.VerificationCode;

@ExtendWith(DropwizardExtensionsSupport.class)
class DeviceControllerTest {

  @Path("/v1/devices")
  static class DumbVerificationDeviceController extends DeviceController {

    public DumbVerificationDeviceController(StoredVerificationCodeManager pendingDevices,
        AccountsManager accounts,
        MessagesManager messages,
        Keys keys,
        RateLimiters rateLimiters,
        Map<String, Integer> deviceConfiguration) {
      super(pendingDevices, accounts, messages, keys, rateLimiters, deviceConfiguration);
    }

    @Override
    protected VerificationCode generateVerificationCode() {
      return new VerificationCode(5678901);
    }
  }

  private static StoredVerificationCodeManager pendingDevicesManager = mock(StoredVerificationCodeManager.class);
  private static AccountsManager accountsManager = mock(AccountsManager.class);
  private static MessagesManager messagesManager = mock(MessagesManager.class);
  private static Keys keys = mock(Keys.class);
  private static RateLimiters rateLimiters = mock(RateLimiters.class);
  private static RateLimiter rateLimiter = mock(RateLimiter.class);
  private static Account account = mock(Account.class);
  private static Account maxedAccount = mock(Account.class);
  private static Device masterDevice = mock(Device.class);
  private static ClientPresenceManager clientPresenceManager = mock(ClientPresenceManager.class);

  private static Map<String, Integer> deviceConfiguration = new HashMap<>();

  private static final ResourceExtension resources = ResourceExtension.builder()
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(new PolymorphicAuthValueFactoryProvider.Binder<>(
          ImmutableSet.of(AuthenticatedAccount.class, DisabledPermittedAuthenticatedAccount.class)))
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addProvider(new WebsocketRefreshApplicationEventListener(accountsManager, clientPresenceManager))
      .addProvider(new DeviceLimitExceededExceptionMapper())
      .addResource(new DumbVerificationDeviceController(pendingDevicesManager,
          accountsManager,
          messagesManager,
          keys,
          rateLimiters,
          deviceConfiguration))
      .build();


  @BeforeEach
  void setup() {
    when(rateLimiters.getSmsDestinationLimiter()).thenReturn(rateLimiter);
    when(rateLimiters.getVoiceDestinationLimiter()).thenReturn(rateLimiter);
    when(rateLimiters.getVerifyLimiter()).thenReturn(rateLimiter);
    when(rateLimiters.getAllocateDeviceLimiter()).thenReturn(rateLimiter);
    when(rateLimiters.getVerifyDeviceLimiter()).thenReturn(rateLimiter);

    when(masterDevice.getId()).thenReturn(1L);

    when(account.getNextDeviceId()).thenReturn(42L);
    when(account.getNumber()).thenReturn(AuthHelper.VALID_NUMBER);
    when(account.getUuid()).thenReturn(AuthHelper.VALID_UUID);
    when(account.getPhoneNumberIdentifier()).thenReturn(AuthHelper.VALID_PNI);
    when(account.isEnabled()).thenReturn(false);
    when(account.isSenderKeySupported()).thenReturn(true);
    when(account.isAnnouncementGroupSupported()).thenReturn(true);
    when(account.isChangeNumberSupported()).thenReturn(true);
    when(account.isPniSupported()).thenReturn(true);
    when(account.isStoriesSupported()).thenReturn(true);
    when(account.isGiftBadgesSupported()).thenReturn(true);
    when(account.isPaymentActivationSupported()).thenReturn(false);

    when(pendingDevicesManager.getCodeForNumber(AuthHelper.VALID_NUMBER)).thenReturn(
        Optional.of(new StoredVerificationCode("5678901", System.currentTimeMillis(), null, null)));
    when(pendingDevicesManager.getCodeForNumber(AuthHelper.VALID_NUMBER_TWO)).thenReturn(Optional.empty());
    when(accountsManager.getByE164(AuthHelper.VALID_NUMBER)).thenReturn(Optional.of(account));
    when(accountsManager.getByE164(AuthHelper.VALID_NUMBER_TWO)).thenReturn(Optional.of(maxedAccount));

    AccountsHelper.setupMockUpdate(accountsManager);
  }

  @AfterEach
  void teardown() {
    reset(
        pendingDevicesManager,
        accountsManager,
        messagesManager,
        keys,
        rateLimiters,
        rateLimiter,
        account,
        maxedAccount,
        masterDevice,
        clientPresenceManager
    );
  }

  @Test
  void validDeviceRegisterTest() {
    when(accountsManager.getByAccountIdentifier(AuthHelper.VALID_UUID)).thenReturn(Optional.of(AuthHelper.VALID_ACCOUNT));

    final Device existingDevice = mock(Device.class);
    when(existingDevice.getId()).thenReturn(Device.MASTER_ID);
    when(AuthHelper.VALID_ACCOUNT.getDevices()).thenReturn(List.of(existingDevice));

    VerificationCode deviceCode = resources.getJerseyTest()
        .target("/v1/devices/provisioning/code")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(VerificationCode.class);

    assertThat(deviceCode).isEqualTo(new VerificationCode(5678901));

    DeviceResponse response = resources.getJerseyTest()
        .target("/v1/devices/5678901")
        .request()
        .header("Authorization", AuthHelper.getProvisioningAuthHeader(AuthHelper.VALID_NUMBER, "password1"))
        .put(Entity.entity(new AccountAttributes(false, 1234, null,
                    null, true, null),
                MediaType.APPLICATION_JSON_TYPE),
            DeviceResponse.class);

    assertThat(response.getDeviceId()).isEqualTo(42L);

    verify(pendingDevicesManager).remove(AuthHelper.VALID_NUMBER);
    verify(messagesManager).clear(eq(AuthHelper.VALID_UUID), eq(42L));
    verify(clientPresenceManager).disconnectPresence(AuthHelper.VALID_UUID, Device.MASTER_ID);
  }

  @Test
  void verifyDeviceWithNullAccountAttributes() {
    when(accountsManager.getByAccountIdentifier(AuthHelper.VALID_UUID)).thenReturn(Optional.of(AuthHelper.VALID_ACCOUNT));

    final Device existingDevice = mock(Device.class);
    when(existingDevice.getId()).thenReturn(Device.MASTER_ID);
    when(AuthHelper.VALID_ACCOUNT.getDevices()).thenReturn(List.of(existingDevice));

    VerificationCode deviceCode = resources.getJerseyTest()
        .target("/v1/devices/provisioning/code")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(VerificationCode.class);

    assertThat(deviceCode).isEqualTo(new VerificationCode(5678901));

    final Response response = resources.getJerseyTest()
        .target("/v1/devices/5678901")
        .request()
        .header("Authorization", AuthHelper.getProvisioningAuthHeader(AuthHelper.VALID_NUMBER, "password1"))
        .put(Entity.json(""));

    assertThat(response.getStatus()).isNotEqualTo(500);
  }

  @Test
  void verifyDeviceTokenBadCredentials() {
    final Response response = resources.getJerseyTest()
        .target("/v1/devices/5678901")
        .request()
        .header("Authorization", "This is not a valid authorization header")
        .put(Entity.entity(new AccountAttributes(false, 1234, null,
                null, true, null),
            MediaType.APPLICATION_JSON_TYPE));

    assertEquals(401, response.getStatus());
  }

  @ParameterizedTest
  @MethodSource
  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  void linkDeviceAtomic(final boolean fetchesMessages,
                        final Optional<ApnRegistrationId> apnRegistrationId,
                        final Optional<GcmRegistrationId> gcmRegistrationId,
                        final Optional<String> expectedApnsToken,
                        final Optional<String> expectedApnsVoipToken,
                        final Optional<String> expectedGcmToken) {

    when(accountsManager.getByAccountIdentifier(AuthHelper.VALID_UUID)).thenReturn(Optional.of(AuthHelper.VALID_ACCOUNT));

    final Device existingDevice = mock(Device.class);
    when(existingDevice.getId()).thenReturn(Device.MASTER_ID);
    when(AuthHelper.VALID_ACCOUNT.getDevices()).thenReturn(List.of(existingDevice));

    VerificationCode deviceCode = resources.getJerseyTest()
        .target("/v1/devices/provisioning/code")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(VerificationCode.class);

    assertThat(deviceCode).isEqualTo(new VerificationCode(5678901));

    final Optional<SignedPreKey> aciSignedPreKey;
    final Optional<SignedPreKey> pniSignedPreKey;
    final Optional<SignedPreKey> aciPqLastResortPreKey;
    final Optional<SignedPreKey> pniPqLastResortPreKey;

    final ECKeyPair aciIdentityKeyPair = Curve.generateKeyPair();
    final ECKeyPair pniIdentityKeyPair = Curve.generateKeyPair();

    aciSignedPreKey = Optional.of(KeysHelper.signedECPreKey(1, aciIdentityKeyPair));
    pniSignedPreKey = Optional.of(KeysHelper.signedECPreKey(2, pniIdentityKeyPair));
    aciPqLastResortPreKey = Optional.of(KeysHelper.signedKEMPreKey(3, aciIdentityKeyPair));
    pniPqLastResortPreKey = Optional.of(KeysHelper.signedKEMPreKey(4, pniIdentityKeyPair));

    when(account.getIdentityKey()).thenReturn(KeysHelper.serializeIdentityKey(aciIdentityKeyPair));
    when(account.getPhoneNumberIdentityKey()).thenReturn(KeysHelper.serializeIdentityKey(pniIdentityKeyPair));

    final LinkDeviceRequest request = new LinkDeviceRequest("5678901",
        new AccountAttributes(fetchesMessages, 1234, null, null, true, null),
        new DeviceActivationRequest(aciSignedPreKey, pniSignedPreKey, aciPqLastResortPreKey, pniPqLastResortPreKey, apnRegistrationId, gcmRegistrationId));

    final DeviceResponse response = resources.getJerseyTest()
        .target("/v1/devices/link")
        .request()
        .header("Authorization", AuthHelper.getProvisioningAuthHeader(AuthHelper.VALID_NUMBER, "password1"))
        .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE), DeviceResponse.class);

    assertThat(response.getDeviceId()).isEqualTo(42L);

    final ArgumentCaptor<Device> deviceCaptor = ArgumentCaptor.forClass(Device.class);
    verify(account).addDevice(deviceCaptor.capture());

    final Device device = deviceCaptor.getValue();

    assertEquals(aciSignedPreKey.get(), device.getSignedPreKey());
    assertEquals(pniSignedPreKey.get(), device.getPhoneNumberIdentitySignedPreKey());
    assertEquals(fetchesMessages, device.getFetchesMessages());

    expectedApnsToken.ifPresentOrElse(expectedToken -> assertEquals(expectedToken, device.getApnId()),
        () -> assertNull(device.getApnId()));

    expectedApnsVoipToken.ifPresentOrElse(expectedToken -> assertEquals(expectedToken, device.getVoipApnId()),
        () -> assertNull(device.getVoipApnId()));

    expectedGcmToken.ifPresentOrElse(expectedToken -> assertEquals(expectedToken, device.getGcmId()),
        () -> assertNull(device.getGcmId()));

    verify(pendingDevicesManager).remove(AuthHelper.VALID_NUMBER);
    verify(messagesManager).clear(eq(AuthHelper.VALID_UUID), eq(42L));
    verify(clientPresenceManager).disconnectPresence(AuthHelper.VALID_UUID, Device.MASTER_ID);
    verify(keys).storePqLastResort(AuthHelper.VALID_UUID, Map.of(response.getDeviceId(), aciPqLastResortPreKey.get()));
    verify(keys).storePqLastResort(AuthHelper.VALID_PNI, Map.of(response.getDeviceId(), pniPqLastResortPreKey.get()));
  }

  private static Stream<Arguments> linkDeviceAtomic() {
    final String apnsToken = "apns-token";
    final String apnsVoipToken = "apns-voip-token";
    final String gcmToken = "gcm-token";

    return Stream.of(
        Arguments.of(true, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()),
        Arguments.of(false, Optional.of(new ApnRegistrationId(apnsToken, null)), Optional.empty(), Optional.of(apnsToken), Optional.empty(), Optional.empty()),
        Arguments.of(false, Optional.of(new ApnRegistrationId(apnsToken, apnsVoipToken)), Optional.empty(), Optional.of(apnsToken), Optional.of(apnsVoipToken), Optional.empty()),
        Arguments.of(false, Optional.empty(), Optional.of(new GcmRegistrationId(gcmToken)), Optional.empty(), Optional.empty(), Optional.of(gcmToken))
    );
  }

  @ParameterizedTest
  @MethodSource
  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  void linkDeviceAtomicConflictingChannel(final boolean fetchesMessages,
                                          final Optional<ApnRegistrationId> apnRegistrationId,
                                          final Optional<GcmRegistrationId> gcmRegistrationId) {
    when(accountsManager.getByAccountIdentifier(AuthHelper.VALID_UUID)).thenReturn(Optional.of(AuthHelper.VALID_ACCOUNT));

    final Device existingDevice = mock(Device.class);
    when(existingDevice.getId()).thenReturn(Device.MASTER_ID);
    when(AuthHelper.VALID_ACCOUNT.getDevices()).thenReturn(List.of(existingDevice));

    VerificationCode deviceCode = resources.getJerseyTest()
        .target("/v1/devices/provisioning/code")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(VerificationCode.class);

    assertThat(deviceCode).isEqualTo(new VerificationCode(5678901));

    final Optional<SignedPreKey> aciSignedPreKey;
    final Optional<SignedPreKey> pniSignedPreKey;
    final Optional<SignedPreKey> aciPqLastResortPreKey;
    final Optional<SignedPreKey> pniPqLastResortPreKey;

    final ECKeyPair aciIdentityKeyPair = Curve.generateKeyPair();
    final ECKeyPair pniIdentityKeyPair = Curve.generateKeyPair();

    aciSignedPreKey = Optional.of(KeysHelper.signedECPreKey(1, aciIdentityKeyPair));
    pniSignedPreKey = Optional.of(KeysHelper.signedECPreKey(2, pniIdentityKeyPair));
    aciPqLastResortPreKey = Optional.of(KeysHelper.signedKEMPreKey(3, aciIdentityKeyPair));
    pniPqLastResortPreKey = Optional.of(KeysHelper.signedKEMPreKey(4, pniIdentityKeyPair));

    when(account.getIdentityKey()).thenReturn(KeysHelper.serializeIdentityKey(aciIdentityKeyPair));
    when(account.getPhoneNumberIdentityKey()).thenReturn(KeysHelper.serializeIdentityKey(pniIdentityKeyPair));

    final LinkDeviceRequest request = new LinkDeviceRequest("5678901",
        new AccountAttributes(fetchesMessages, 1234, null, null, true, null),
        new DeviceActivationRequest(aciSignedPreKey, pniSignedPreKey, aciPqLastResortPreKey, pniPqLastResortPreKey, apnRegistrationId, gcmRegistrationId));

    try (final Response response = resources.getJerseyTest()
        .target("/v1/devices/link")
        .request()
        .header("Authorization", AuthHelper.getProvisioningAuthHeader(AuthHelper.VALID_NUMBER, "password1"))
        .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE))) {

      assertEquals(422, response.getStatus());
    }
  }

  private static Stream<Arguments> linkDeviceAtomicConflictingChannel() {
    return Stream.of(
        Arguments.of(true, Optional.of(new ApnRegistrationId("apns-token", null)), Optional.of(new GcmRegistrationId("gcm-token"))),
        Arguments.of(true, Optional.empty(), Optional.of(new GcmRegistrationId("gcm-token"))),
        Arguments.of(true, Optional.of(new ApnRegistrationId("apns-token", null)), Optional.empty()),
        Arguments.of(false, Optional.of(new ApnRegistrationId("apns-token", null)), Optional.of(new GcmRegistrationId("gcm-token")))
    );
  }

  @ParameterizedTest
  @MethodSource
  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  void linkDeviceAtomicMissingProperty(final String aciIdentityKey,
                                       final String pniIdentityKey,
                                       final Optional<SignedPreKey> aciSignedPreKey,
                                       final Optional<SignedPreKey> pniSignedPreKey,
                                       final Optional<SignedPreKey> aciPqLastResortPreKey,
                                       final Optional<SignedPreKey> pniPqLastResortPreKey) {

    when(accountsManager.getByAccountIdentifier(AuthHelper.VALID_UUID)).thenReturn(Optional.of(AuthHelper.VALID_ACCOUNT));

    final Device existingDevice = mock(Device.class);
    when(existingDevice.getId()).thenReturn(Device.MASTER_ID);
    when(AuthHelper.VALID_ACCOUNT.getDevices()).thenReturn(List.of(existingDevice));

    VerificationCode deviceCode = resources.getJerseyTest()
        .target("/v1/devices/provisioning/code")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(VerificationCode.class);

    assertThat(deviceCode).isEqualTo(new VerificationCode(5678901));

    when(account.getIdentityKey()).thenReturn(aciIdentityKey);
    when(account.getPhoneNumberIdentityKey()).thenReturn(pniIdentityKey);

    final LinkDeviceRequest request = new LinkDeviceRequest("5678901",
        new AccountAttributes(true, 1234, null, null, true, null),
        new DeviceActivationRequest(aciSignedPreKey, pniSignedPreKey, aciPqLastResortPreKey, pniPqLastResortPreKey, Optional.empty(), Optional.empty()));

    try (final Response response = resources.getJerseyTest()
        .target("/v1/devices/link")
        .request()
        .header("Authorization", AuthHelper.getProvisioningAuthHeader(AuthHelper.VALID_NUMBER, "password1"))
        .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE))) {

      assertEquals(422, response.getStatus());
    }
  }

  private static Stream<Arguments> linkDeviceAtomicMissingProperty() {
    final ECKeyPair aciIdentityKeyPair = Curve.generateKeyPair();
    final ECKeyPair pniIdentityKeyPair = Curve.generateKeyPair();

    final Optional<SignedPreKey> aciSignedPreKey = Optional.of(KeysHelper.signedECPreKey(1, aciIdentityKeyPair));
    final Optional<SignedPreKey> pniSignedPreKey = Optional.of(KeysHelper.signedECPreKey(2, pniIdentityKeyPair));
    final Optional<SignedPreKey> aciPqLastResortPreKey = Optional.of(KeysHelper.signedKEMPreKey(3, aciIdentityKeyPair));
    final Optional<SignedPreKey> pniPqLastResortPreKey = Optional.of(KeysHelper.signedKEMPreKey(4, pniIdentityKeyPair));

    final String aciIdentityKey = KeysHelper.serializeIdentityKey(aciIdentityKeyPair);
    final String pniIdentityKey = KeysHelper.serializeIdentityKey(pniIdentityKeyPair);

    return Stream.of(
        Arguments.of(aciIdentityKey, pniIdentityKey, Optional.empty(), pniSignedPreKey, aciPqLastResortPreKey, pniPqLastResortPreKey),
        Arguments.of(aciIdentityKey, pniIdentityKey, aciSignedPreKey, Optional.empty(), aciPqLastResortPreKey, pniPqLastResortPreKey),
        Arguments.of(aciIdentityKey, pniIdentityKey, aciSignedPreKey, pniSignedPreKey, Optional.empty(), pniPqLastResortPreKey),
        Arguments.of(aciIdentityKey, pniIdentityKey, aciSignedPreKey, pniSignedPreKey, aciPqLastResortPreKey, Optional.empty())
    );
  }

  @ParameterizedTest
  @MethodSource
  void linkDeviceAtomicInvalidSignature(final String aciIdentityKey,
                                        final String pniIdentityKey,
                                        final SignedPreKey aciSignedPreKey,
                                        final SignedPreKey pniSignedPreKey,
                                        final SignedPreKey aciPqLastResortPreKey,
                                        final SignedPreKey pniPqLastResortPreKey) {

    when(accountsManager.getByAccountIdentifier(AuthHelper.VALID_UUID)).thenReturn(Optional.of(AuthHelper.VALID_ACCOUNT));

    final Device existingDevice = mock(Device.class);
    when(existingDevice.getId()).thenReturn(Device.MASTER_ID);
    when(AuthHelper.VALID_ACCOUNT.getDevices()).thenReturn(List.of(existingDevice));

    VerificationCode deviceCode = resources.getJerseyTest()
        .target("/v1/devices/provisioning/code")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(VerificationCode.class);

    assertThat(deviceCode).isEqualTo(new VerificationCode(5678901));

    when(account.getIdentityKey()).thenReturn(aciIdentityKey);
    when(account.getPhoneNumberIdentityKey()).thenReturn(pniIdentityKey);

    final LinkDeviceRequest request = new LinkDeviceRequest("5678901",
        new AccountAttributes(true, 1234, null, null, true, null),
        new DeviceActivationRequest(Optional.of(aciSignedPreKey), Optional.of(pniSignedPreKey), Optional.of(aciPqLastResortPreKey), Optional.of(pniPqLastResortPreKey), Optional.empty(), Optional.empty()));

    try (final Response response = resources.getJerseyTest()
        .target("/v1/devices/link")
        .request()
        .header("Authorization", AuthHelper.getProvisioningAuthHeader(AuthHelper.VALID_NUMBER, "password1"))
        .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE))) {

      assertEquals(422, response.getStatus());
    }
  }

  private static Stream<Arguments> linkDeviceAtomicInvalidSignature() {
    final ECKeyPair aciIdentityKeyPair = Curve.generateKeyPair();
    final ECKeyPair pniIdentityKeyPair = Curve.generateKeyPair();

    final SignedPreKey aciSignedPreKey = KeysHelper.signedECPreKey(1, aciIdentityKeyPair);
    final SignedPreKey pniSignedPreKey = KeysHelper.signedECPreKey(2, pniIdentityKeyPair);
    final SignedPreKey aciPqLastResortPreKey = KeysHelper.signedKEMPreKey(3, aciIdentityKeyPair);
    final SignedPreKey pniPqLastResortPreKey = KeysHelper.signedKEMPreKey(4, pniIdentityKeyPair);

    final String aciIdentityKey = KeysHelper.serializeIdentityKey(aciIdentityKeyPair);
    final String pniIdentityKey = KeysHelper.serializeIdentityKey(pniIdentityKeyPair);

    return Stream.of(
        Arguments.of(aciIdentityKey, pniIdentityKey, signedPreKeyWithBadSignature(aciSignedPreKey), pniSignedPreKey, aciPqLastResortPreKey, pniPqLastResortPreKey),
        Arguments.of(aciIdentityKey, pniIdentityKey, aciSignedPreKey, signedPreKeyWithBadSignature(pniSignedPreKey), aciPqLastResortPreKey, pniPqLastResortPreKey),
        Arguments.of(aciIdentityKey, pniIdentityKey, aciSignedPreKey, pniSignedPreKey, signedPreKeyWithBadSignature(aciPqLastResortPreKey), pniPqLastResortPreKey),
        Arguments.of(aciIdentityKey, pniIdentityKey, aciSignedPreKey, pniSignedPreKey, aciPqLastResortPreKey, signedPreKeyWithBadSignature(pniPqLastResortPreKey))
    );
  }

  private static SignedPreKey signedPreKeyWithBadSignature(final SignedPreKey signedPreKey) {
    return new SignedPreKey(signedPreKey.getKeyId(),
        signedPreKey.getPublicKey(),
        Base64.getEncoder().encodeToString("incorrect-signature".getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  void disabledDeviceRegisterTest() {
    Response response = resources.getJerseyTest()
        .target("/v1/devices/provisioning/code")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.DISABLED_UUID, AuthHelper.DISABLED_PASSWORD))
        .get();

    assertThat(response.getStatus()).isEqualTo(401);
  }

  @Test
  void invalidDeviceRegisterTest() {
    VerificationCode deviceCode = resources.getJerseyTest()
        .target("/v1/devices/provisioning/code")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(VerificationCode.class);

    assertThat(deviceCode).isEqualTo(new VerificationCode(5678901));

    Response response = resources.getJerseyTest()
        .target("/v1/devices/5678902")
        .request()
        .header("Authorization", AuthHelper.getProvisioningAuthHeader(AuthHelper.VALID_NUMBER, "password1"))
        .put(Entity.entity(new AccountAttributes(false, 1234, null, null, true, null),
            MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(403);

    verifyNoMoreInteractions(messagesManager);
  }

  @Test
  void oldDeviceRegisterTest() {
    Response response = resources.getJerseyTest()
        .target("/v1/devices/1112223")
        .request()
        .header("Authorization",
            AuthHelper.getProvisioningAuthHeader(AuthHelper.VALID_NUMBER_TWO, AuthHelper.VALID_PASSWORD_TWO))
        .put(Entity.entity(new AccountAttributes(false, 1234, null, null, true, null),
            MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(403);

    verifyNoMoreInteractions(messagesManager);
  }

  @Test
  void maxDevicesTest() {
    Response response = resources.getJerseyTest()
        .target("/v1/devices/provisioning/code")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_TWO, AuthHelper.VALID_PASSWORD_TWO))
        .get();

    assertEquals(411, response.getStatus());
    verifyNoMoreInteractions(messagesManager);
  }

  @Test
  void longNameTest() {
    Response response = resources.getJerseyTest()
        .target("/v1/devices/5678901")
        .request()
        .header("Authorization", AuthHelper.getProvisioningAuthHeader(AuthHelper.VALID_NUMBER, "password1"))
        .put(Entity.entity(new AccountAttributes(false, 1234,
                "this is a really long name that is longer than 80 characters it's so long that it's even longer than 204 characters. that's a lot of characters. we're talking lots and lots and lots of characters. 12345678",
                null, true, null),
            MediaType.APPLICATION_JSON_TYPE));

    assertEquals(response.getStatus(), 422);
    verifyNoMoreInteractions(messagesManager);
  }

  @Test
  void deviceDowngradeSenderKeyTest() {
    DeviceCapabilities deviceCapabilities = new DeviceCapabilities(true, true, false, true,
        true, true, true, true, true);
    AccountAttributes accountAttributes =
        new AccountAttributes(false, 1234, null, null, true, deviceCapabilities);
    Response response = resources
        .getJerseyTest()
        .target("/v1/devices/5678901")
        .request()
        .header("Authorization", AuthHelper.getProvisioningAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
        .header(HttpHeaders.USER_AGENT, "Signal-Android/5.42.8675309 Android/30")
        .put(Entity.entity(accountAttributes, MediaType.APPLICATION_JSON_TYPE));
    assertThat(response.getStatus()).isEqualTo(409);

    deviceCapabilities = new DeviceCapabilities(true, true, true, true, true, true, true, true, true);
    accountAttributes = new AccountAttributes(false, 1234, null, null, true, deviceCapabilities);
    response = resources
        .getJerseyTest()
        .target("/v1/devices/5678901")
        .request()
        .header("Authorization", AuthHelper.getProvisioningAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
        .header(HttpHeaders.USER_AGENT, "Signal-Android/5.42.8675309 Android/30")
        .put(Entity.entity(accountAttributes, MediaType.APPLICATION_JSON_TYPE));
    assertThat(response.getStatus()).isEqualTo(200);
  }

  @Test
  void deviceDowngradeAnnouncementGroupTest() {
    DeviceCapabilities deviceCapabilities = new DeviceCapabilities(true, true, true, false,
        true, true, true, true, true);
    AccountAttributes accountAttributes =
        new AccountAttributes(false, 1234, null, null, true, deviceCapabilities);
    Response response = resources
        .getJerseyTest()
        .target("/v1/devices/5678901")
        .request()
        .header("Authorization", AuthHelper.getProvisioningAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
        .header(HttpHeaders.USER_AGENT, "Signal-Android/5.42.8675309 Android/30")
        .put(Entity.entity(accountAttributes, MediaType.APPLICATION_JSON_TYPE));
    assertThat(response.getStatus()).isEqualTo(409);

    deviceCapabilities = new DeviceCapabilities(true, true, true, true, true, true, true, true, true);
    accountAttributes = new AccountAttributes(false, 1234, null, null, true, deviceCapabilities);
    response = resources
        .getJerseyTest()
        .target("/v1/devices/5678901")
        .request()
        .header("Authorization", AuthHelper.getProvisioningAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
        .header(HttpHeaders.USER_AGENT, "Signal-Android/5.42.8675309 Android/30")
        .put(Entity.entity(accountAttributes, MediaType.APPLICATION_JSON_TYPE));
    assertThat(response.getStatus()).isEqualTo(200);
  }

  @Test
  void deviceDowngradeChangeNumberTest() {
    DeviceCapabilities deviceCapabilities = new DeviceCapabilities(true, true, true, true,
        false, true, true, true, true);
    AccountAttributes accountAttributes =
        new AccountAttributes(false, 1234, null, null, true, deviceCapabilities);
    Response response = resources
        .getJerseyTest()
        .target("/v1/devices/5678901")
        .request()
        .header("Authorization",
            AuthHelper.getProvisioningAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
        .header(HttpHeaders.USER_AGENT, "Signal-Android/5.42.8675309 Android/30")
        .put(Entity.entity(accountAttributes, MediaType.APPLICATION_JSON_TYPE));
    assertThat(response.getStatus()).isEqualTo(409);

    deviceCapabilities = new DeviceCapabilities(true, true, true, true, true, true, true, true, true);
    accountAttributes = new AccountAttributes(false, 1234, null, null, true, deviceCapabilities);
    response = resources
        .getJerseyTest()
        .target("/v1/devices/5678901")
        .request()
        .header("Authorization",
            AuthHelper.getProvisioningAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
        .header(HttpHeaders.USER_AGENT, "Signal-Android/5.42.8675309 Android/30")
        .put(Entity.entity(accountAttributes, MediaType.APPLICATION_JSON_TYPE));
    assertThat(response.getStatus()).isEqualTo(200);
  }

  @Test
  void deviceDowngradePniTest() {
    DeviceCapabilities deviceCapabilities = new DeviceCapabilities(true, true, true, true, true,
        false, true, true, true);
    AccountAttributes accountAttributes =
        new AccountAttributes(false, 1234, null, null, true, deviceCapabilities);
    Response response = resources
        .getJerseyTest()
        .target("/v1/devices/5678901")
        .request()
        .header("Authorization", AuthHelper.getProvisioningAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
        .header(HttpHeaders.USER_AGENT, "Signal-Android/5.42.8675309 Android/30")
        .put(Entity.entity(accountAttributes, MediaType.APPLICATION_JSON_TYPE));
    assertThat(response.getStatus()).isEqualTo(409);

    deviceCapabilities = new DeviceCapabilities(true, true, true, true, true, true, true, true, true);
    accountAttributes = new AccountAttributes(false, 1234, null, null, true, deviceCapabilities);
    response = resources
        .getJerseyTest()
        .target("/v1/devices/5678901")
        .request()
        .header("Authorization",
            AuthHelper.getProvisioningAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
        .header(HttpHeaders.USER_AGENT, "Signal-Android/5.42.8675309 Android/30")
        .put(Entity.entity(accountAttributes, MediaType.APPLICATION_JSON_TYPE));
    assertThat(response.getStatus()).isEqualTo(200);
  }

  @Test
  void deviceDowngradeStoriesTest() {
    DeviceCapabilities deviceCapabilities = new DeviceCapabilities(true, true, true, true, true,
        true, false, true, true);
    AccountAttributes accountAttributes =
        new AccountAttributes(false, 1234, null, null, true, deviceCapabilities);
    Response response = resources
        .getJerseyTest()
        .target("/v1/devices/5678901")
        .request()
        .header("Authorization",
            AuthHelper.getProvisioningAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
        .header(HttpHeaders.USER_AGENT, "Signal-Android/5.42.8675309 Android/30")
        .put(Entity.entity(accountAttributes, MediaType.APPLICATION_JSON_TYPE));
    assertThat(response.getStatus()).isEqualTo(409);

    deviceCapabilities = new DeviceCapabilities(true, true, true, true, true, true, true, true, true);
    accountAttributes = new AccountAttributes(false, 1234, null, null, true, deviceCapabilities);
    response = resources
        .getJerseyTest()
        .target("/v1/devices/5678901")
        .request()
        .header("Authorization",
            AuthHelper.getProvisioningAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
        .header(HttpHeaders.USER_AGENT, "Signal-Android/5.42.8675309 Android/30")
        .put(Entity.entity(accountAttributes, MediaType.APPLICATION_JSON_TYPE));
    assertThat(response.getStatus()).isEqualTo(200);
  }

  @Test
  void deviceDowngradeGiftBadgesTest() {
    DeviceCapabilities deviceCapabilities = new DeviceCapabilities(true, true, true, true, true, true, true, false, true);
    AccountAttributes accountAttributes = new AccountAttributes(false, 1234, null, null, true, deviceCapabilities);
    Response response = resources
        .getJerseyTest()
        .target("/v1/devices/5678901")
        .request()
        .header("Authorization", AuthHelper.getProvisioningAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
        .header(HttpHeaders.USER_AGENT, "Signal-Android/5.42.8675309 Android/30")
        .put(Entity.entity(accountAttributes, MediaType.APPLICATION_JSON_TYPE));
    assertThat(response.getStatus()).isEqualTo(409);

    deviceCapabilities = new DeviceCapabilities(true, true, true, true, true, true, true, true, true);
    accountAttributes = new AccountAttributes(false, 1234, null, null, true, deviceCapabilities);
    response = resources
        .getJerseyTest()
        .target("/v1/devices/5678901")
        .request()
        .header("Authorization",
            AuthHelper.getProvisioningAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
        .header(HttpHeaders.USER_AGENT, "Signal-Android/5.42.8675309 Android/30")
        .put(Entity.entity(accountAttributes, MediaType.APPLICATION_JSON_TYPE));
    assertThat(response.getStatus()).isEqualTo(200);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void deviceDowngradePaymentActivationTest(boolean paymentActivation) {
    // Update when we start returning true value of capability & restricting downgrades
    DeviceCapabilities deviceCapabilities = new DeviceCapabilities(true, true, true, true, true, true, true, true, paymentActivation);
    AccountAttributes accountAttributes = new AccountAttributes(false, 1234, null, null, true, deviceCapabilities);
    Response response = resources
        .getJerseyTest()
        .target("/v1/devices/5678901")
        .request()
        .header("Authorization", AuthHelper.getProvisioningAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
        .header(HttpHeaders.USER_AGENT, "Signal-Android/5.42.8675309 Android/30")
        .put(Entity.entity(accountAttributes, MediaType.APPLICATION_JSON_TYPE));
    assertThat(response.getStatus()).isEqualTo(200);
  }

  @Test
  void deviceRemovalClearsMessagesAndKeys() {

    // this is a static mock, so it might have previous invocations
    clearInvocations(AuthHelper.VALID_ACCOUNT);

    final long deviceId = 2;

    final Response response = resources
        .getJerseyTest()
        .target("/v1/devices/" + deviceId)
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .header(HttpHeaders.USER_AGENT, "Signal-Android/5.42.8675309 Android/30")
        .delete();

    assertThat(response.getStatus()).isEqualTo(204);

    verify(messagesManager, times(2)).clear(AuthHelper.VALID_UUID, deviceId);
    verify(accountsManager, times(1)).update(eq(AuthHelper.VALID_ACCOUNT), any());
    verify(AuthHelper.VALID_ACCOUNT).removeDevice(deviceId);
    verify(keys).delete(AuthHelper.VALID_UUID, deviceId);
  }

}
