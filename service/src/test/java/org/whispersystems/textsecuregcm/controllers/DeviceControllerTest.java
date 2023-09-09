/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.controllers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
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
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.DisabledPermittedAuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.WebsocketRefreshApplicationEventListener;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.entities.ApnRegistrationId;
import org.whispersystems.textsecuregcm.entities.DeviceActivationRequest;
import org.whispersystems.textsecuregcm.entities.DeviceResponse;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.entities.GcmRegistrationId;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.entities.LinkDeviceRequest;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.mappers.DeviceLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.push.ClientPresenceManager;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.Device.DeviceCapabilities;
import org.whispersystems.textsecuregcm.storage.KeysManager;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.tests.util.KeysHelper;
import org.whispersystems.textsecuregcm.tests.util.RedisClusterHelper;
import org.whispersystems.textsecuregcm.util.TestClock;
import org.whispersystems.textsecuregcm.util.VerificationCode;

@ExtendWith(DropwizardExtensionsSupport.class)
class DeviceControllerTest {

  private static AccountsManager accountsManager = mock(AccountsManager.class);
  private static MessagesManager messagesManager = mock(MessagesManager.class);
  private static KeysManager keysManager = mock(KeysManager.class);
  private static RateLimiters rateLimiters = mock(RateLimiters.class);
  private static RateLimiter rateLimiter = mock(RateLimiter.class);
  private static RedisAdvancedClusterCommands<String, String> commands = mock(RedisAdvancedClusterCommands.class);
  private static Account account = mock(Account.class);
  private static Account maxedAccount = mock(Account.class);
  private static Device masterDevice = mock(Device.class);
  private static ClientPresenceManager clientPresenceManager = mock(ClientPresenceManager.class);
  private static Map<String, Integer> deviceConfiguration = new HashMap<>();
  private static TestClock testClock = TestClock.now();

  private static DeviceController deviceController = new DeviceController(
      generateLinkDeviceSecret(),
      accountsManager,
      messagesManager,
      keysManager,
      rateLimiters,
      RedisClusterHelper.builder().stringCommands(commands).build(),
      deviceConfiguration,
      testClock);

  private static final ResourceExtension resources = ResourceExtension.builder()
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(new PolymorphicAuthValueFactoryProvider.Binder<>(
          ImmutableSet.of(AuthenticatedAccount.class, DisabledPermittedAuthenticatedAccount.class)))
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addProvider(new WebsocketRefreshApplicationEventListener(accountsManager, clientPresenceManager))
      .addProvider(new DeviceLimitExceededExceptionMapper())
      .addResource(deviceController)
      .build();

  private static byte[] generateLinkDeviceSecret() {
    final byte[] linkDeviceSecret = new byte[32];
    new SecureRandom().nextBytes(linkDeviceSecret);

    return linkDeviceSecret;
  }

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
    when(account.isPniSupported()).thenReturn(true);
    when(account.isPaymentActivationSupported()).thenReturn(false);

    when(accountsManager.getByAccountIdentifier(AuthHelper.VALID_UUID)).thenReturn(Optional.of(account));
    when(accountsManager.getByE164(AuthHelper.VALID_NUMBER)).thenReturn(Optional.of(account));
    when(accountsManager.getByE164(AuthHelper.VALID_NUMBER_TWO)).thenReturn(Optional.of(maxedAccount));

    AccountsHelper.setupMockUpdate(accountsManager);

    when(keysManager.storePqLastResort(any(), any())).thenReturn(CompletableFuture.completedFuture(null));
    when(keysManager.delete(any(), anyLong())).thenReturn(CompletableFuture.completedFuture(null));

    when(messagesManager.clear(any(), anyLong())).thenReturn(CompletableFuture.completedFuture(null));
  }

  @AfterEach
  void teardown() {
    reset(
        accountsManager,
        messagesManager,
        keysManager,
        rateLimiters,
        rateLimiter,
        commands,
        account,
        maxedAccount,
        masterDevice,
        clientPresenceManager
    );

    testClock.unpin();
  }

  @Test
  void validDeviceRegisterTest() {
    final Device existingDevice = mock(Device.class);
    when(existingDevice.getId()).thenReturn(Device.MASTER_ID);
    when(existingDevice.isEnabled()).thenReturn(true);
    when(AuthHelper.VALID_ACCOUNT.getDevices()).thenReturn(List.of(existingDevice));

    VerificationCode deviceCode = resources.getJerseyTest()
        .target("/v1/devices/provisioning/code")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(VerificationCode.class);

    DeviceResponse response = resources.getJerseyTest()
        .target("/v1/devices/" + deviceCode.verificationCode())
        .request()
        .header("Authorization", AuthHelper.getProvisioningAuthHeader(AuthHelper.VALID_NUMBER, "password1"))
        .put(Entity.entity(new AccountAttributes(false, 1234, null,
                    null, true, null),
                MediaType.APPLICATION_JSON_TYPE),
            DeviceResponse.class);

    assertThat(response.getDeviceId()).isEqualTo(42L);

    verify(messagesManager).clear(eq(AuthHelper.VALID_UUID), eq(42L));
    verify(commands).set(anyString(), anyString(), any());
  }

  @Test
  void validDeviceRegisterTestSignedTokenUsed() {
    when(accountsManager.getByAccountIdentifier(AuthHelper.VALID_UUID)).thenReturn(Optional.of(account));

    final Device existingDevice = mock(Device.class);
    when(existingDevice.getId()).thenReturn(Device.MASTER_ID);
    when(AuthHelper.VALID_ACCOUNT.getDevices()).thenReturn(List.of(existingDevice));

    final String verificationToken = deviceController.generateVerificationToken(AuthHelper.VALID_UUID);

    when(commands.get(anyString())).thenReturn("");

    final Response response = resources.getJerseyTest()
            .target("/v1/devices/" + verificationToken)
            .request()
            .header("Authorization", AuthHelper.getProvisioningAuthHeader(AuthHelper.VALID_NUMBER, "password1"))
            .put(Entity.entity(new AccountAttributes(false, 1234, null,
                                    null, true, null),
                            MediaType.APPLICATION_JSON_TYPE));

    assertEquals(Response.Status.FORBIDDEN.getStatusCode(), response.getStatus());
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

    final Response response = resources.getJerseyTest()
        .target("/v1/devices/" + deviceCode.verificationCode())
        .request()
        .header("Authorization", AuthHelper.getProvisioningAuthHeader(AuthHelper.VALID_NUMBER, "password1"))
        .put(Entity.json(""));

    assertThat(response.getStatus()).isNotEqualTo(500);
  }

  @Test
  void verifyDeviceTokenBadCredentials() {
    final String verificationToken = deviceController.generateVerificationToken(AuthHelper.VALID_UUID);

    final Response response = resources.getJerseyTest()
        .target("/v1/devices/" + verificationToken)
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

    final Device existingDevice = mock(Device.class);
    when(existingDevice.getId()).thenReturn(Device.MASTER_ID);
    when(AuthHelper.VALID_ACCOUNT.getDevices()).thenReturn(List.of(existingDevice));

    VerificationCode deviceCode = resources.getJerseyTest()
        .target("/v1/devices/provisioning/code")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(VerificationCode.class);

    final Optional<ECSignedPreKey> aciSignedPreKey;
    final Optional<ECSignedPreKey> pniSignedPreKey;
    final Optional<KEMSignedPreKey> aciPqLastResortPreKey;
    final Optional<KEMSignedPreKey> pniPqLastResortPreKey;

    final ECKeyPair aciIdentityKeyPair = Curve.generateKeyPair();
    final ECKeyPair pniIdentityKeyPair = Curve.generateKeyPair();

    aciSignedPreKey = Optional.of(KeysHelper.signedECPreKey(1, aciIdentityKeyPair));
    pniSignedPreKey = Optional.of(KeysHelper.signedECPreKey(2, pniIdentityKeyPair));
    aciPqLastResortPreKey = Optional.of(KeysHelper.signedKEMPreKey(3, aciIdentityKeyPair));
    pniPqLastResortPreKey = Optional.of(KeysHelper.signedKEMPreKey(4, pniIdentityKeyPair));

    when(account.getIdentityKey(IdentityType.ACI)).thenReturn(new IdentityKey(aciIdentityKeyPair.getPublicKey()));
    when(account.getIdentityKey(IdentityType.PNI)).thenReturn(new IdentityKey(pniIdentityKeyPair.getPublicKey()));

    when(keysManager.storeEcSignedPreKeys(any(), any())).thenReturn(CompletableFuture.completedFuture(null));
    when(keysManager.storePqLastResort(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

    final LinkDeviceRequest request = new LinkDeviceRequest(deviceCode.verificationCode(),
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

    assertEquals(aciSignedPreKey.get(), device.getSignedPreKey(IdentityType.ACI));
    assertEquals(pniSignedPreKey.get(), device.getSignedPreKey(IdentityType.PNI));
    assertEquals(fetchesMessages, device.getFetchesMessages());

    expectedApnsToken.ifPresentOrElse(expectedToken -> assertEquals(expectedToken, device.getApnId()),
        () -> assertNull(device.getApnId()));

    expectedApnsVoipToken.ifPresentOrElse(expectedToken -> assertEquals(expectedToken, device.getVoipApnId()),
        () -> assertNull(device.getVoipApnId()));

    expectedGcmToken.ifPresentOrElse(expectedToken -> assertEquals(expectedToken, device.getGcmId()),
        () -> assertNull(device.getGcmId()));

    verify(messagesManager).clear(eq(AuthHelper.VALID_UUID), eq(42L));
    verify(keysManager).storeEcSignedPreKeys(AuthHelper.VALID_UUID, Map.of(response.getDeviceId(), aciSignedPreKey.get()));
    verify(keysManager).storeEcSignedPreKeys(AuthHelper.VALID_PNI, Map.of(response.getDeviceId(), pniSignedPreKey.get()));
    verify(keysManager).storePqLastResort(AuthHelper.VALID_UUID, Map.of(response.getDeviceId(), aciPqLastResortPreKey.get()));
    verify(keysManager).storePqLastResort(AuthHelper.VALID_PNI, Map.of(response.getDeviceId(), pniPqLastResortPreKey.get()));
    verify(commands).set(anyString(), anyString(), any());
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

  @Test
  void linkDeviceAtomicWithVerificationTokenUsed() {

    when(accountsManager.getByAccountIdentifier(AuthHelper.VALID_UUID)).thenReturn(Optional.of(account));

    final Device existingDevice = mock(Device.class);
    when(existingDevice.getId()).thenReturn(Device.MASTER_ID);
    when(AuthHelper.VALID_ACCOUNT.getDevices()).thenReturn(List.of(existingDevice));

    final Optional<ECSignedPreKey> aciSignedPreKey;
    final Optional<ECSignedPreKey> pniSignedPreKey;
    final Optional<KEMSignedPreKey> aciPqLastResortPreKey;
    final Optional<KEMSignedPreKey> pniPqLastResortPreKey;

    final ECKeyPair aciIdentityKeyPair = Curve.generateKeyPair();
    final ECKeyPair pniIdentityKeyPair = Curve.generateKeyPair();

    aciSignedPreKey = Optional.of(KeysHelper.signedECPreKey(1, aciIdentityKeyPair));
    pniSignedPreKey = Optional.of(KeysHelper.signedECPreKey(2, pniIdentityKeyPair));
    aciPqLastResortPreKey = Optional.of(KeysHelper.signedKEMPreKey(3, aciIdentityKeyPair));
    pniPqLastResortPreKey = Optional.of(KeysHelper.signedKEMPreKey(4, pniIdentityKeyPair));

    when(account.getIdentityKey(IdentityType.ACI)).thenReturn(new IdentityKey(aciIdentityKeyPair.getPublicKey()));
    when(account.getIdentityKey(IdentityType.PNI)).thenReturn(new IdentityKey(pniIdentityKeyPair.getPublicKey()));

    when(keysManager.storeEcSignedPreKeys(any(), any())).thenReturn(CompletableFuture.completedFuture(null));
    when(keysManager.storePqLastResort(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

    when(commands.get(anyString())).thenReturn("");

    final LinkDeviceRequest request = new LinkDeviceRequest(deviceController.generateVerificationToken(AuthHelper.VALID_UUID),
            new AccountAttributes(false, 1234, null, null, true, null),
            new DeviceActivationRequest(aciSignedPreKey, pniSignedPreKey, aciPqLastResortPreKey, pniPqLastResortPreKey, Optional.empty(), Optional.of(new GcmRegistrationId("gcm-id"))));

    try (final Response response = resources.getJerseyTest()
            .target("/v1/devices/link")
            .request()
            .header("Authorization", AuthHelper.getProvisioningAuthHeader(AuthHelper.VALID_NUMBER, "password1"))
            .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE))) {

      assertEquals(Response.Status.FORBIDDEN.getStatusCode(), response.getStatus());
    }
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

    final Optional<ECSignedPreKey> aciSignedPreKey;
    final Optional<ECSignedPreKey> pniSignedPreKey;
    final Optional<KEMSignedPreKey> aciPqLastResortPreKey;
    final Optional<KEMSignedPreKey> pniPqLastResortPreKey;

    final ECKeyPair aciIdentityKeyPair = Curve.generateKeyPair();
    final ECKeyPair pniIdentityKeyPair = Curve.generateKeyPair();

    aciSignedPreKey = Optional.of(KeysHelper.signedECPreKey(1, aciIdentityKeyPair));
    pniSignedPreKey = Optional.of(KeysHelper.signedECPreKey(2, pniIdentityKeyPair));
    aciPqLastResortPreKey = Optional.of(KeysHelper.signedKEMPreKey(3, aciIdentityKeyPair));
    pniPqLastResortPreKey = Optional.of(KeysHelper.signedKEMPreKey(4, pniIdentityKeyPair));

    when(account.getIdentityKey(IdentityType.ACI)).thenReturn(new IdentityKey(aciIdentityKeyPair.getPublicKey()));
    when(account.getIdentityKey(IdentityType.PNI)).thenReturn(new IdentityKey(pniIdentityKeyPair.getPublicKey()));

    final LinkDeviceRequest request = new LinkDeviceRequest(deviceCode.verificationCode(),
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
  void linkDeviceAtomicMissingProperty(final IdentityKey aciIdentityKey,
                                       final IdentityKey pniIdentityKey,
                                       final Optional<ECSignedPreKey> aciSignedPreKey,
                                       final Optional<ECSignedPreKey> pniSignedPreKey,
                                       final Optional<KEMSignedPreKey> aciPqLastResortPreKey,
                                       final Optional<KEMSignedPreKey> pniPqLastResortPreKey) {

    when(accountsManager.getByAccountIdentifier(AuthHelper.VALID_UUID)).thenReturn(Optional.of(AuthHelper.VALID_ACCOUNT));

    final Device existingDevice = mock(Device.class);
    when(existingDevice.getId()).thenReturn(Device.MASTER_ID);
    when(AuthHelper.VALID_ACCOUNT.getDevices()).thenReturn(List.of(existingDevice));

    VerificationCode deviceCode = resources.getJerseyTest()
        .target("/v1/devices/provisioning/code")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(VerificationCode.class);

    when(account.getIdentityKey(IdentityType.ACI)).thenReturn(aciIdentityKey);
    when(account.getIdentityKey(IdentityType.PNI)).thenReturn(pniIdentityKey);

    final LinkDeviceRequest request = new LinkDeviceRequest(deviceCode.verificationCode(),
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

    final Optional<ECSignedPreKey> aciSignedPreKey = Optional.of(KeysHelper.signedECPreKey(1, aciIdentityKeyPair));
    final Optional<ECSignedPreKey> pniSignedPreKey = Optional.of(KeysHelper.signedECPreKey(2, pniIdentityKeyPair));
    final Optional<KEMSignedPreKey> aciPqLastResortPreKey = Optional.of(KeysHelper.signedKEMPreKey(3, aciIdentityKeyPair));
    final Optional<KEMSignedPreKey> pniPqLastResortPreKey = Optional.of(KeysHelper.signedKEMPreKey(4, pniIdentityKeyPair));

    final IdentityKey aciIdentityKey = new IdentityKey(aciIdentityKeyPair.getPublicKey());
    final IdentityKey pniIdentityKey = new IdentityKey(pniIdentityKeyPair.getPublicKey());

    return Stream.of(
        Arguments.of(aciIdentityKey, pniIdentityKey, Optional.empty(), pniSignedPreKey, aciPqLastResortPreKey, pniPqLastResortPreKey),
        Arguments.of(aciIdentityKey, pniIdentityKey, aciSignedPreKey, Optional.empty(), aciPqLastResortPreKey, pniPqLastResortPreKey),
        Arguments.of(aciIdentityKey, pniIdentityKey, aciSignedPreKey, pniSignedPreKey, Optional.empty(), pniPqLastResortPreKey),
        Arguments.of(aciIdentityKey, pniIdentityKey, aciSignedPreKey, pniSignedPreKey, aciPqLastResortPreKey, Optional.empty())
    );
  }

  @ParameterizedTest
  @MethodSource
  void linkDeviceAtomicInvalidSignature(final IdentityKey aciIdentityKey,
                                        final IdentityKey pniIdentityKey,
                                        final ECSignedPreKey aciSignedPreKey,
                                        final ECSignedPreKey pniSignedPreKey,
                                        final KEMSignedPreKey aciPqLastResortPreKey,
                                        final KEMSignedPreKey pniPqLastResortPreKey) {

    when(accountsManager.getByAccountIdentifier(AuthHelper.VALID_UUID)).thenReturn(Optional.of(AuthHelper.VALID_ACCOUNT));

    final Device existingDevice = mock(Device.class);
    when(existingDevice.getId()).thenReturn(Device.MASTER_ID);
    when(AuthHelper.VALID_ACCOUNT.getDevices()).thenReturn(List.of(existingDevice));

    VerificationCode deviceCode = resources.getJerseyTest()
        .target("/v1/devices/provisioning/code")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(VerificationCode.class);

    when(account.getIdentityKey(IdentityType.ACI)).thenReturn(aciIdentityKey);
    when(account.getIdentityKey(IdentityType.PNI)).thenReturn(pniIdentityKey);

    final LinkDeviceRequest request = new LinkDeviceRequest(deviceCode.verificationCode(),
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

    final ECSignedPreKey aciSignedPreKey = KeysHelper.signedECPreKey(1, aciIdentityKeyPair);
    final ECSignedPreKey pniSignedPreKey = KeysHelper.signedECPreKey(2, pniIdentityKeyPair);
    final KEMSignedPreKey aciPqLastResortPreKey = KeysHelper.signedKEMPreKey(3, aciIdentityKeyPair);
    final KEMSignedPreKey pniPqLastResortPreKey = KeysHelper.signedKEMPreKey(4, pniIdentityKeyPair);

    final IdentityKey aciIdentityKey = new IdentityKey(aciIdentityKeyPair.getPublicKey());
    final IdentityKey pniIdentityKey = new IdentityKey(pniIdentityKeyPair.getPublicKey());

    return Stream.of(
        Arguments.of(aciIdentityKey, pniIdentityKey, ecSignedPreKeyWithBadSignature(aciSignedPreKey), pniSignedPreKey, aciPqLastResortPreKey, pniPqLastResortPreKey),
        Arguments.of(aciIdentityKey, pniIdentityKey, aciSignedPreKey, ecSignedPreKeyWithBadSignature(pniSignedPreKey), aciPqLastResortPreKey, pniPqLastResortPreKey),
        Arguments.of(aciIdentityKey, pniIdentityKey, aciSignedPreKey, pniSignedPreKey, kemSignedPreKeyWithBadSignature(aciPqLastResortPreKey), pniPqLastResortPreKey),
        Arguments.of(aciIdentityKey, pniIdentityKey, aciSignedPreKey, pniSignedPreKey, aciPqLastResortPreKey, kemSignedPreKeyWithBadSignature(pniPqLastResortPreKey))
    );
  }

  private static ECSignedPreKey ecSignedPreKeyWithBadSignature(final ECSignedPreKey signedPreKey) {
    return new ECSignedPreKey(signedPreKey.keyId(),
        signedPreKey.publicKey(),
        "incorrect-signature".getBytes(StandardCharsets.UTF_8));
  }

  private static KEMSignedPreKey kemSignedPreKeyWithBadSignature(final KEMSignedPreKey signedPreKey) {
    return new KEMSignedPreKey(signedPreKey.keyId(),
        signedPreKey.publicKey(),
        "incorrect-signature".getBytes(StandardCharsets.UTF_8));
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

    Response response = resources.getJerseyTest()
        .target("/v1/devices/" + deviceCode.verificationCode() + "-incorrect")
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
    final String verificationToken = deviceController.generateVerificationToken(AuthHelper.VALID_UUID);

    Response response = resources.getJerseyTest()
        .target("/v1/devices/" + verificationToken)
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
  void deviceDowngradePniTest() {
    DeviceCapabilities deviceCapabilities = new DeviceCapabilities(true, true,
        false, true);
    AccountAttributes accountAttributes =
        new AccountAttributes(false, 1234, null, null, true, deviceCapabilities);

    final String verificationToken = deviceController.generateVerificationToken(AuthHelper.VALID_UUID);

    Response response = resources
        .getJerseyTest()
        .target("/v1/devices/" + verificationToken)
        .request()
        .header("Authorization", AuthHelper.getProvisioningAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
        .header(HttpHeaders.USER_AGENT, "Signal-Android/5.42.8675309 Android/30")
        .put(Entity.entity(accountAttributes, MediaType.APPLICATION_JSON_TYPE));
    assertThat(response.getStatus()).isEqualTo(409);

    deviceCapabilities = new DeviceCapabilities(true, true, true, true);
    accountAttributes = new AccountAttributes(false, 1234, null, null, true, deviceCapabilities);
    response = resources
        .getJerseyTest()
        .target("/v1/devices/" + verificationToken)
        .request()
        .header("Authorization",
            AuthHelper.getProvisioningAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
        .header(HttpHeaders.USER_AGENT, "Signal-Android/5.42.8675309 Android/30")
        .put(Entity.entity(accountAttributes, MediaType.APPLICATION_JSON_TYPE));
    assertThat(response.getStatus()).isEqualTo(200);
  }

  @Test
  void putCapabilitiesSuccessTest() {
    final DeviceCapabilities deviceCapabilities = new DeviceCapabilities(true, true, true, true);
    final Response response = resources
        .getJerseyTest()
        .target("/v1/devices/capabilities")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .header(HttpHeaders.USER_AGENT, "Signal-Android/5.42.8675309 Android/30")
        .put(Entity.entity(deviceCapabilities, MediaType.APPLICATION_JSON_TYPE));
    assertThat(response.getStatus()).isEqualTo(204);
    assertThat(response.hasEntity()).isFalse();
  }

  @Test
  void putCapabilitiesFailureTest() {
    final Response response = resources
        .getJerseyTest()
        .target("/v1/devices/capabilities")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .header(HttpHeaders.USER_AGENT, "Signal-Android/5.42.8675309 Android/30")
        .put(Entity.json(""));
    assertThat(response.getStatus()).isEqualTo(422);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void deviceDowngradePaymentActivationTest(boolean paymentActivation) {
    // Update when we start returning true value of capability & restricting downgrades
    DeviceCapabilities deviceCapabilities = new DeviceCapabilities(true, true, true, paymentActivation);
    AccountAttributes accountAttributes = new AccountAttributes(false, 1234, null, null, true, deviceCapabilities);

    final String verificationToken = deviceController.generateVerificationToken(AuthHelper.VALID_UUID);

    Response response = resources
        .getJerseyTest()
        .target("/v1/devices/" + verificationToken)
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
    assertThat(response.hasEntity()).isFalse();

    verify(messagesManager, times(2)).clear(AuthHelper.VALID_UUID, deviceId);
    verify(accountsManager, times(1)).update(eq(AuthHelper.VALID_ACCOUNT), any());
    verify(AuthHelper.VALID_ACCOUNT).removeDevice(deviceId);
    verify(keysManager).delete(AuthHelper.VALID_UUID, deviceId);
  }

  @Test
  void unlinkPrimaryDevice() {
    // this is a static mock, so it might have previous invocations
    clearInvocations(AuthHelper.VALID_ACCOUNT);

    try (final Response response = resources
        .getJerseyTest()
        .target("/v1/devices/" + Device.MASTER_ID)
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .header(HttpHeaders.USER_AGENT, "Signal-Android/5.42.8675309 Android/30")
        .delete()) {

      assertThat(response.getStatus()).isEqualTo(403);

      verify(messagesManager, never()).clear(any(), anyLong());
      verify(accountsManager, never()).update(eq(AuthHelper.VALID_ACCOUNT), any());
      verify(AuthHelper.VALID_ACCOUNT, never()).removeDevice(anyLong());
      verify(keysManager, never()).delete(any(), anyLong());
    }
  }

  @Test
  void checkVerificationToken() {
    final UUID uuid = UUID.randomUUID();

    assertEquals(Optional.of(uuid),
        deviceController.checkVerificationToken(deviceController.generateVerificationToken(uuid)));
  }

  @ParameterizedTest
  @MethodSource
  void checkVerificationTokenBadToken(final String token, final Instant currentTime) {
    testClock.pin(currentTime);

    assertEquals(Optional.empty(),
        deviceController.checkVerificationToken(token));
  }

  private static Stream<Arguments> checkVerificationTokenBadToken() {
    final Instant tokenTimestamp = testClock.instant();

    return Stream.of(
        // Expired token
        Arguments.of(deviceController.generateVerificationToken(UUID.randomUUID()),
            tokenTimestamp.plus(DeviceController.TOKEN_EXPIRATION_DURATION).plusSeconds(1)),

        // Bad UUID
        Arguments.of("not-a-valid-uuid.1691096565171:0CKWF7q3E9fi4sB2or4q1A0Up2z_73EQlMAy7Dpel9c=", tokenTimestamp),

        // No UUID
        Arguments.of(".1691096565171:0CKWF7q3E9fi4sB2or4q1A0Up2z_73EQlMAy7Dpel9c=", tokenTimestamp),

        // Bad timestamp
        Arguments.of("e552603a-1492-4de6-872d-bac19a2825b4.not-a-valid-timestamp:0CKWF7q3E9fi4sB2or4q1A0Up2z_73EQlMAy7Dpel9c=", tokenTimestamp),

        // No timestamp
        Arguments.of("e552603a-1492-4de6-872d-bac19a2825b4:0CKWF7q3E9fi4sB2or4q1A0Up2z_73EQlMAy7Dpel9c=", tokenTimestamp),

        // Blank timestamp
        Arguments.of("e552603a-1492-4de6-872d-bac19a2825b4.:0CKWF7q3E9fi4sB2or4q1A0Up2z_73EQlMAy7Dpel9c=", tokenTimestamp),

        // No signature
        Arguments.of("e552603a-1492-4de6-872d-bac19a2825b4.1691096565171", tokenTimestamp),

        // Blank signature
        Arguments.of("e552603a-1492-4de6-872d-bac19a2825b4.1691096565171:", tokenTimestamp),

        // Incorrect signature
        Arguments.of("e552603a-1492-4de6-872d-bac19a2825b4.1691096565171:0CKWF7q3E9fi4sB2or4q1A0Up2z_73EQlMAy7Dpel9c=", tokenTimestamp),

        // Invalid signature
        Arguments.of("e552603a-1492-4de6-872d-bac19a2825b4.1691096565171:This is not valid base64", tokenTimestamp)
    );
  }
}
