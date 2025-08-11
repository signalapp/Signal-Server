/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.controllers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.net.HttpHeaders;
import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.mockito.ArgumentCaptor;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.entities.ApnRegistrationId;
import org.whispersystems.textsecuregcm.entities.DeviceActivationRequest;
import org.whispersystems.textsecuregcm.entities.DeviceInfo;
import org.whispersystems.textsecuregcm.entities.DeviceInfoList;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.entities.GcmRegistrationId;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.entities.LinkDeviceRequest;
import org.whispersystems.textsecuregcm.entities.LinkDeviceResponse;
import org.whispersystems.textsecuregcm.entities.RemoteAttachment;
import org.whispersystems.textsecuregcm.entities.RemoteAttachmentError;
import org.whispersystems.textsecuregcm.entities.RestoreAccountRequest;
import org.whispersystems.textsecuregcm.entities.SetPublicKeyRequest;
import org.whispersystems.textsecuregcm.entities.TransferArchiveUploadedRequest;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.mappers.DeviceLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.RateLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.ClientPublicKeysManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.DeviceCapability;
import org.whispersystems.textsecuregcm.storage.DeviceSpec;
import org.whispersystems.textsecuregcm.storage.LinkDeviceTokenAlreadyUsedException;
import org.whispersystems.textsecuregcm.storage.PersistentTimer;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.tests.util.KeysHelper;
import org.whispersystems.textsecuregcm.tests.util.MockRedisFuture;
import org.whispersystems.textsecuregcm.util.LinkDeviceToken;
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.textsecuregcm.util.TestClock;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

@ExtendWith(DropwizardExtensionsSupport.class)
class DeviceControllerTest {

  private static final AccountsManager accountsManager = mock(AccountsManager.class);
  private static final ClientPublicKeysManager clientPublicKeysManager = mock(ClientPublicKeysManager.class);
  private static final PersistentTimer persistentTimer = mock(PersistentTimer.class);
  private static final RateLimiters rateLimiters = mock(RateLimiters.class);
  private static final RateLimiter rateLimiter = mock(RateLimiter.class);
  @SuppressWarnings("unchecked")
  private static final RedisAdvancedClusterCommands<String, String> commands = mock(RedisAdvancedClusterCommands.class);
  @SuppressWarnings("unchecked")
  private static final RedisAdvancedClusterAsyncCommands<String, String> asyncCommands = mock(RedisAdvancedClusterAsyncCommands.class);
  private static final Account account = mock(Account.class);
  private static final Account maxedAccount = mock(Account.class);
  private static final Device primaryDevice = mock(Device.class);
  private static final Map<String, Integer> deviceConfiguration = new HashMap<>();
  private static final TestClock testClock = TestClock.now();

  private static final byte NEXT_DEVICE_ID = 42;

  private static final DeviceController deviceController = new DeviceController(
      accountsManager,
      clientPublicKeysManager,
      rateLimiters,
      persistentTimer,
      deviceConfiguration);

  private static final ResourceExtension resources = ResourceExtension.builder()
      .addProperty(ServerProperties.UNWRAP_COMPLETION_STAGE_IN_WRITER_ENABLE, Boolean.TRUE)
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(new AuthValueFactoryProvider.Binder<>(AuthenticatedDevice.class))
      .addProvider(new RateLimitExceededExceptionMapper())
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addProvider(new DeviceLimitExceededExceptionMapper())
      .addResource(deviceController)
      .build();

  @BeforeEach
  void setup() {
    when(rateLimiters.getAllocateDeviceLimiter()).thenReturn(rateLimiter);
    when(rateLimiters.getVerifyDeviceLimiter()).thenReturn(rateLimiter);
    when(rateLimiters.getWaitForLinkedDeviceLimiter()).thenReturn(rateLimiter);
    when(rateLimiters.getUploadTransferArchiveLimiter()).thenReturn(rateLimiter);
    when(rateLimiters.getWaitForTransferArchiveLimiter()).thenReturn(rateLimiter);

    when(primaryDevice.getId()).thenReturn(Device.PRIMARY_ID);

    when(account.getNextDeviceId()).thenReturn(NEXT_DEVICE_ID);
    when(account.getNumber()).thenReturn(AuthHelper.VALID_NUMBER);
    when(account.getUuid()).thenReturn(AuthHelper.VALID_UUID);
    when(account.getPhoneNumberIdentifier()).thenReturn(AuthHelper.VALID_PNI);
    when(account.getPrimaryDevice()).thenReturn(primaryDevice);
    when(account.getDevice(anyByte())).thenReturn(Optional.empty());
    when(account.getDevice(Device.PRIMARY_ID)).thenReturn(Optional.of(primaryDevice));
    when(account.getDevices()).thenReturn(List.of(primaryDevice));

    when(accountsManager.getByAccountIdentifier(AuthHelper.VALID_UUID)).thenReturn(Optional.of(account));
    when(accountsManager.getByAccountIdentifierAsync(AuthHelper.VALID_UUID))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

    when(accountsManager.getByE164(AuthHelper.VALID_NUMBER)).thenReturn(Optional.of(account));
    when(accountsManager.getByE164(AuthHelper.VALID_NUMBER_TWO)).thenReturn(Optional.of(maxedAccount));

    when(clientPublicKeysManager.setPublicKey(any(), anyByte(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    when(persistentTimer.start(anyString(), anyString()))
        .thenReturn(CompletableFuture.completedFuture(mock(PersistentTimer.Sample.class)));

    AccountsHelper.setupMockUpdate(accountsManager);
  }

  @AfterEach
  void teardown() {
    reset(
        accountsManager,
        rateLimiters,
        rateLimiter,
        commands,
        asyncCommands,
        account,
        maxedAccount,
        primaryDevice
    );

    testClock.unpin();
  }

  @Test
  void getDevices() {
    final byte deviceId = Device.PRIMARY_ID;
    final byte[] deviceName = "refreshed-device-name".getBytes(StandardCharsets.UTF_8);
    final long deviceCreated = System.currentTimeMillis();
    final long deviceLastSeen = deviceCreated + 1;
    final int registrationId = 2;
    final byte[] createdAtCiphertext = "timestamp ciphertext".getBytes(StandardCharsets.UTF_8);

    final Device refreshedDevice = mock(Device.class);
    when(refreshedDevice.getId()).thenReturn(deviceId);
    when(refreshedDevice.getName()).thenReturn(deviceName);
    when(refreshedDevice.getCreated()).thenReturn(deviceCreated);
    when(refreshedDevice.getLastSeen()).thenReturn(deviceLastSeen);
    when(refreshedDevice.getRegistrationId(IdentityType.ACI)).thenReturn(registrationId);
    when(refreshedDevice.getCreatedAtCiphertext()).thenReturn(createdAtCiphertext);

    final Account refreshedAccount = mock(Account.class);
    when(refreshedAccount.getDevices()).thenReturn(List.of(refreshedDevice));

    when(accountsManager.getByAccountIdentifier(AuthHelper.VALID_UUID)).thenReturn(Optional.of(refreshedAccount));

    final DeviceInfoList deviceInfoList = resources.getJerseyTest()
        .target("/v1/devices")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(DeviceInfoList.class);

    assertEquals(1, deviceInfoList.devices().size());
    assertEquals(deviceId, deviceInfoList.devices().getFirst().id());
    assertArrayEquals(deviceName, deviceInfoList.devices().getFirst().name());
    assertEquals(deviceCreated, deviceInfoList.devices().getFirst().created());
    assertEquals(deviceLastSeen, deviceInfoList.devices().getFirst().lastSeen());
    assertEquals(registrationId, deviceInfoList.devices().getFirst().registrationId());
    assertArrayEquals(createdAtCiphertext, deviceInfoList.devices().getFirst().createdAtCiphertext());
  }

  @ParameterizedTest
  @MethodSource
  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  void linkDeviceAtomic(final boolean fetchesMessages,
                        final Optional<ApnRegistrationId> apnRegistrationId,
                        final Optional<GcmRegistrationId> gcmRegistrationId,
                        final Optional<String> expectedApnsToken,
                        final Optional<String> expectedGcmToken) {

    final Device existingDevice = mock(Device.class);
    when(existingDevice.getId()).thenReturn(Device.PRIMARY_ID);
    when(account.getDevices()).thenReturn(List.of(existingDevice));

    final ECSignedPreKey aciSignedPreKey;
    final ECSignedPreKey pniSignedPreKey;
    final KEMSignedPreKey aciPqLastResortPreKey;
    final KEMSignedPreKey pniPqLastResortPreKey;

    final ECKeyPair aciIdentityKeyPair = ECKeyPair.generate();
    final ECKeyPair pniIdentityKeyPair = ECKeyPair.generate();

    aciSignedPreKey = KeysHelper.signedECPreKey(1, aciIdentityKeyPair);
    pniSignedPreKey = KeysHelper.signedECPreKey(2, pniIdentityKeyPair);
    aciPqLastResortPreKey = KeysHelper.signedKEMPreKey(3, aciIdentityKeyPair);
    pniPqLastResortPreKey = KeysHelper.signedKEMPreKey(4, pniIdentityKeyPair);

    final IdentityKey aciIdentityKey = new IdentityKey(aciIdentityKeyPair.getPublicKey());
    when(account.getIdentityKey(IdentityType.ACI)).thenReturn(aciIdentityKey);
    when(account.getIdentityKey(IdentityType.PNI)).thenReturn(new IdentityKey(pniIdentityKeyPair.getPublicKey()));

    when(accountsManager.checkDeviceLinkingToken(anyString())).thenReturn(Optional.of(AuthHelper.VALID_UUID));

    when(accountsManager.addDevice(any(), any(), any())).thenAnswer(invocation -> {
      final Account a = invocation.getArgument(0);
      final DeviceSpec deviceSpec = invocation.getArgument(1);

      return CompletableFuture.completedFuture(new Pair<>(a, deviceSpec.toDevice(NEXT_DEVICE_ID, testClock, aciIdentityKey)));
    });

    when(asyncCommands.set(any(), any(), any())).thenReturn(MockRedisFuture.completedFuture(null));

    final AccountAttributes accountAttributes = new AccountAttributes(fetchesMessages, 1234, 5678, null,
        null, true, Set.of());

    final LinkDeviceRequest request = new LinkDeviceRequest("link-device-token",
        accountAttributes,
        new DeviceActivationRequest(aciSignedPreKey, pniSignedPreKey, aciPqLastResortPreKey, pniPqLastResortPreKey, apnRegistrationId, gcmRegistrationId));

    final LinkDeviceResponse response = resources.getJerseyTest()
        .target("/v1/devices/link")
        .request()
        .header("Authorization", AuthHelper.getProvisioningAuthHeader(AuthHelper.VALID_NUMBER, "password1"))
        .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE), LinkDeviceResponse.class);

    assertThat(response.deviceId()).isEqualTo(NEXT_DEVICE_ID);

    final ArgumentCaptor<DeviceSpec> deviceSpecCaptor = ArgumentCaptor.forClass(DeviceSpec.class);
    verify(accountsManager).addDevice(eq(account), deviceSpecCaptor.capture(), any());

    final Device device = deviceSpecCaptor.getValue().toDevice(NEXT_DEVICE_ID, testClock, aciIdentityKey);

    assertEquals(fetchesMessages, device.getFetchesMessages());

    expectedApnsToken.ifPresentOrElse(expectedToken -> assertEquals(expectedToken, device.getApnId()),
        () -> assertNull(device.getApnId()));

    expectedGcmToken.ifPresentOrElse(expectedToken -> assertEquals(expectedToken, device.getGcmId()),
        () -> assertNull(device.getGcmId()));
  }

  private static Stream<Arguments> linkDeviceAtomic() {
    final String apnsToken = "apns-token";
    final String gcmToken = "gcm-token";

    return Stream.of(
        Arguments.of(true, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()),
        Arguments.of(false, Optional.of(new ApnRegistrationId(apnsToken)), Optional.empty(), Optional.of(apnsToken), Optional.empty()),
        Arguments.of(false, Optional.of(new ApnRegistrationId(apnsToken)), Optional.empty(), Optional.of(apnsToken), Optional.empty()),
        Arguments.of(false, Optional.empty(), Optional.of(new GcmRegistrationId(gcmToken)), Optional.empty(), Optional.of(gcmToken))
    );
  }

  @CartesianTest
  void deviceDowngrade(@CartesianTest.Enum final DeviceCapability capability,
      @CartesianTest.Values(booleans = {true, false}) final boolean accountHasCapability,
      @CartesianTest.Values(booleans = {true, false}) final boolean requestHasCapability) {

    when(accountsManager.getByAccountIdentifier(AuthHelper.VALID_UUID)).thenReturn(Optional.of(account));
    when(accountsManager.addDevice(any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(new Pair<>(mock(Account.class), mock(Device.class))));

    final Device primaryDevice = mock(Device.class);
    when(primaryDevice.getId()).thenReturn(Device.PRIMARY_ID);
    when(account.getDevices()).thenReturn(List.of(primaryDevice));

    final ECSignedPreKey aciSignedPreKey;
    final ECSignedPreKey pniSignedPreKey;
    final KEMSignedPreKey aciPqLastResortPreKey;
    final KEMSignedPreKey pniPqLastResortPreKey;

    final ECKeyPair aciIdentityKeyPair = ECKeyPair.generate();
    final ECKeyPair pniIdentityKeyPair = ECKeyPair.generate();

    aciSignedPreKey = KeysHelper.signedECPreKey(1, aciIdentityKeyPair);
    pniSignedPreKey = KeysHelper.signedECPreKey(2, pniIdentityKeyPair);
    aciPqLastResortPreKey = KeysHelper.signedKEMPreKey(3, aciIdentityKeyPair);
    pniPqLastResortPreKey = KeysHelper.signedKEMPreKey(4, pniIdentityKeyPair);

    when(account.getIdentityKey(IdentityType.ACI)).thenReturn(new IdentityKey(aciIdentityKeyPair.getPublicKey()));
    when(account.getIdentityKey(IdentityType.PNI)).thenReturn(new IdentityKey(pniIdentityKeyPair.getPublicKey()));
    when(account.hasCapability(capability)).thenReturn(accountHasCapability);

    when(asyncCommands.set(any(), any(), any())).thenReturn(MockRedisFuture.completedFuture(null));

    when(accountsManager.checkDeviceLinkingToken(anyString())).thenReturn(Optional.of(AuthHelper.VALID_UUID));

    final Set<DeviceCapability> requestCapabilities = EnumSet.allOf(DeviceCapability.class);

    if (!requestHasCapability) {
      requestCapabilities.remove(capability);
    }

    final LinkDeviceRequest request = new LinkDeviceRequest("link-device-token",
        new AccountAttributes(false, 1234, 5678, null, null, true, requestCapabilities),
        new DeviceActivationRequest(aciSignedPreKey, pniSignedPreKey, aciPqLastResortPreKey, pniPqLastResortPreKey, Optional.empty(), Optional.of(new GcmRegistrationId("gcm-id"))));

    final int expectedStatus =
        capability.getAccountCapabilityMode() != DeviceCapability.AccountCapabilityMode.ALWAYS_CAPABLE
            && capability.preventDowngrade() && accountHasCapability && !requestHasCapability ? 409 : 200;

    try (final Response response = resources.getJerseyTest()
        .target("/v1/devices/link")
        .request()
        .header("Authorization", AuthHelper.getProvisioningAuthHeader(AuthHelper.VALID_NUMBER, "password1"))
        .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE))) {

      assertEquals(expectedStatus, response.getStatus());
    }
  }

  @Test
  void linkDeviceAtomicBadCredentials() {
    when(accountsManager.getByAccountIdentifier(AuthHelper.VALID_UUID)).thenReturn(Optional.of(account));

    final Device primaryDevice = mock(Device.class);
    when(primaryDevice.getId()).thenReturn(Device.PRIMARY_ID);
    when(account.getDevices()).thenReturn(List.of(primaryDevice));

    final ECSignedPreKey aciSignedPreKey;
    final ECSignedPreKey pniSignedPreKey;
    final KEMSignedPreKey aciPqLastResortPreKey;
    final KEMSignedPreKey pniPqLastResortPreKey;

    final ECKeyPair aciIdentityKeyPair = ECKeyPair.generate();
    final ECKeyPair pniIdentityKeyPair = ECKeyPair.generate();

    aciSignedPreKey = KeysHelper.signedECPreKey(1, aciIdentityKeyPair);
    pniSignedPreKey = KeysHelper.signedECPreKey(2, pniIdentityKeyPair);
    aciPqLastResortPreKey = KeysHelper.signedKEMPreKey(3, aciIdentityKeyPair);
    pniPqLastResortPreKey = KeysHelper.signedKEMPreKey(4, pniIdentityKeyPair);

    when(account.getIdentityKey(IdentityType.ACI)).thenReturn(new IdentityKey(aciIdentityKeyPair.getPublicKey()));
    when(account.getIdentityKey(IdentityType.PNI)).thenReturn(new IdentityKey(pniIdentityKeyPair.getPublicKey()));

    final LinkDeviceRequest request = new LinkDeviceRequest("link-device-token",
        new AccountAttributes(false, 1234, 5678, null, null, true, null),
        new DeviceActivationRequest(aciSignedPreKey, pniSignedPreKey, aciPqLastResortPreKey, pniPqLastResortPreKey, Optional.empty(), Optional.of(new GcmRegistrationId("gcm-id"))));

    try (final Response response = resources.getJerseyTest()
        .target("/v1/devices/link")
        .request()
        .header("Authorization", "This is not a valid authorization header")
        .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE))) {

      assertEquals(Response.Status.UNAUTHORIZED.getStatusCode(), response.getStatus());
    }
  }

  @Test
  void linkDeviceAtomicReusedToken() {
    final Device existingDevice = mock(Device.class);
    when(existingDevice.getId()).thenReturn(Device.PRIMARY_ID);
    when(account.getDevices()).thenReturn(List.of(existingDevice));

    final ECSignedPreKey aciSignedPreKey;
    final ECSignedPreKey pniSignedPreKey;
    final KEMSignedPreKey aciPqLastResortPreKey;
    final KEMSignedPreKey pniPqLastResortPreKey;

    final ECKeyPair aciIdentityKeyPair = ECKeyPair.generate();
    final ECKeyPair pniIdentityKeyPair = ECKeyPair.generate();

    aciSignedPreKey = KeysHelper.signedECPreKey(1, aciIdentityKeyPair);
    pniSignedPreKey = KeysHelper.signedECPreKey(2, pniIdentityKeyPair);
    aciPqLastResortPreKey = KeysHelper.signedKEMPreKey(3, aciIdentityKeyPair);
    pniPqLastResortPreKey = KeysHelper.signedKEMPreKey(4, pniIdentityKeyPair);

    when(account.getIdentityKey(IdentityType.ACI)).thenReturn(new IdentityKey(aciIdentityKeyPair.getPublicKey()));
    when(account.getIdentityKey(IdentityType.PNI)).thenReturn(new IdentityKey(pniIdentityKeyPair.getPublicKey()));

    when(accountsManager.checkDeviceLinkingToken(anyString())).thenReturn(Optional.of(AuthHelper.VALID_UUID));

    when(accountsManager.addDevice(any(), any(), any()))
        .thenReturn(CompletableFuture.failedFuture(new LinkDeviceTokenAlreadyUsedException()));

    when(asyncCommands.set(any(), any(), any())).thenReturn(MockRedisFuture.completedFuture(null));

    final AccountAttributes accountAttributes = new AccountAttributes(true, 1234, 5678, null,
        null, true, Set.of());

    final LinkDeviceRequest request = new LinkDeviceRequest("link-device-token",
        accountAttributes,
        new DeviceActivationRequest(aciSignedPreKey, pniSignedPreKey, aciPqLastResortPreKey, pniPqLastResortPreKey, Optional.empty(), Optional.empty()));

    try (final Response response = resources.getJerseyTest()
        .target("/v1/devices/link")
        .request()
        .header("Authorization", AuthHelper.getProvisioningAuthHeader(AuthHelper.VALID_NUMBER, "password1"))
        .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE))) {

      assertEquals(403, response.getStatus());
    }
  }

  @Test
  void linkDeviceAtomicWithVerificationTokenUsed() {

    when(accountsManager.getByAccountIdentifier(AuthHelper.VALID_UUID)).thenReturn(Optional.of(account));

    final Device existingDevice = mock(Device.class);
    when(existingDevice.getId()).thenReturn(Device.PRIMARY_ID);
    when(account.getDevices()).thenReturn(List.of(existingDevice));

    final ECSignedPreKey aciSignedPreKey;
    final ECSignedPreKey pniSignedPreKey;
    final KEMSignedPreKey aciPqLastResortPreKey;
    final KEMSignedPreKey pniPqLastResortPreKey;

    final ECKeyPair aciIdentityKeyPair = ECKeyPair.generate();
    final ECKeyPair pniIdentityKeyPair = ECKeyPair.generate();

    aciSignedPreKey = KeysHelper.signedECPreKey(1, aciIdentityKeyPair);
    pniSignedPreKey = KeysHelper.signedECPreKey(2, pniIdentityKeyPair);
    aciPqLastResortPreKey = KeysHelper.signedKEMPreKey(3, aciIdentityKeyPair);
    pniPqLastResortPreKey = KeysHelper.signedKEMPreKey(4, pniIdentityKeyPair);

    when(account.getIdentityKey(IdentityType.ACI)).thenReturn(new IdentityKey(aciIdentityKeyPair.getPublicKey()));
    when(account.getIdentityKey(IdentityType.PNI)).thenReturn(new IdentityKey(pniIdentityKeyPair.getPublicKey()));

    when(commands.get(anyString())).thenReturn("");

    final LinkDeviceRequest request = new LinkDeviceRequest("link-device-token",
            new AccountAttributes(false, 1234, 5678, null, null, true, null),
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
    when(accountsManager.getByAccountIdentifier(AuthHelper.VALID_UUID)).thenReturn(Optional.of(account));
    when(accountsManager.generateLinkDeviceToken(any())).thenReturn("test");

    final Device existingDevice = mock(Device.class);
    when(existingDevice.getId()).thenReturn(Device.PRIMARY_ID);
    when(account.getDevices()).thenReturn(List.of(existingDevice));

    final LinkDeviceToken deviceCode = resources.getJerseyTest()
        .target("/v1/devices/provisioning/code")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(LinkDeviceToken.class);

    final ECSignedPreKey aciSignedPreKey;
    final ECSignedPreKey pniSignedPreKey;
    final KEMSignedPreKey aciPqLastResortPreKey;
    final KEMSignedPreKey pniPqLastResortPreKey;

    final ECKeyPair aciIdentityKeyPair = ECKeyPair.generate();
    final ECKeyPair pniIdentityKeyPair = ECKeyPair.generate();

    aciSignedPreKey = KeysHelper.signedECPreKey(1, aciIdentityKeyPair);
    pniSignedPreKey = KeysHelper.signedECPreKey(2, pniIdentityKeyPair);
    aciPqLastResortPreKey = KeysHelper.signedKEMPreKey(3, aciIdentityKeyPair);
    pniPqLastResortPreKey = KeysHelper.signedKEMPreKey(4, pniIdentityKeyPair);

    when(account.getIdentityKey(IdentityType.ACI)).thenReturn(new IdentityKey(aciIdentityKeyPair.getPublicKey()));
    when(account.getIdentityKey(IdentityType.PNI)).thenReturn(new IdentityKey(pniIdentityKeyPair.getPublicKey()));

    final LinkDeviceRequest request = new LinkDeviceRequest(deviceCode.token(),
        new AccountAttributes(fetchesMessages, 1234, 5678, null, null, true, null),
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
        Arguments.of(true, Optional.of(new ApnRegistrationId("apns-token")), Optional.of(new GcmRegistrationId("gcm-token"))),
        Arguments.of(true, Optional.empty(), Optional.of(new GcmRegistrationId("gcm-token"))),
        Arguments.of(true, Optional.of(new ApnRegistrationId("apns-token")), Optional.empty()),
        Arguments.of(false, Optional.of(new ApnRegistrationId("apns-token")), Optional.of(new GcmRegistrationId("gcm-token")))
    );
  }

  @ParameterizedTest
  @MethodSource
  void linkDeviceAtomicMissingProperty(final IdentityKey aciIdentityKey,
                                       final IdentityKey pniIdentityKey,
                                       final ECSignedPreKey aciSignedPreKey,
                                       final ECSignedPreKey pniSignedPreKey,
                                       final KEMSignedPreKey aciPqLastResortPreKey,
                                       final KEMSignedPreKey pniPqLastResortPreKey) {

    when(accountsManager.getByAccountIdentifier(AuthHelper.VALID_UUID)).thenReturn(Optional.of(account));
    when(accountsManager.generateLinkDeviceToken(any())).thenReturn("test");

    final Device existingDevice = mock(Device.class);
    when(existingDevice.getId()).thenReturn(Device.PRIMARY_ID);
    when(account.getDevices()).thenReturn(List.of(existingDevice));

    final LinkDeviceToken deviceCode = resources.getJerseyTest()
        .target("/v1/devices/provisioning/code")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(LinkDeviceToken.class);

    when(account.getIdentityKey(IdentityType.ACI)).thenReturn(aciIdentityKey);
    when(account.getIdentityKey(IdentityType.PNI)).thenReturn(pniIdentityKey);

    final LinkDeviceRequest request = new LinkDeviceRequest(deviceCode.token(),
        new AccountAttributes(true, 1234, 5678, null, null, true, null),
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
    final ECKeyPair aciIdentityKeyPair = ECKeyPair.generate();
    final ECKeyPair pniIdentityKeyPair = ECKeyPair.generate();

    final ECSignedPreKey aciSignedPreKey = KeysHelper.signedECPreKey(1, aciIdentityKeyPair);
    final ECSignedPreKey pniSignedPreKey = KeysHelper.signedECPreKey(2, pniIdentityKeyPair);
    final KEMSignedPreKey aciPqLastResortPreKey = KeysHelper.signedKEMPreKey(3, aciIdentityKeyPair);
    final KEMSignedPreKey pniPqLastResortPreKey = KeysHelper.signedKEMPreKey(4, pniIdentityKeyPair);

    final IdentityKey aciIdentityKey = new IdentityKey(aciIdentityKeyPair.getPublicKey());
    final IdentityKey pniIdentityKey = new IdentityKey(pniIdentityKeyPair.getPublicKey());

    return Stream.of(
        Arguments.of(aciIdentityKey, pniIdentityKey, null, pniSignedPreKey, aciPqLastResortPreKey, pniPqLastResortPreKey),
        Arguments.of(aciIdentityKey, pniIdentityKey, aciSignedPreKey, null, aciPqLastResortPreKey, pniPqLastResortPreKey),
        Arguments.of(aciIdentityKey, pniIdentityKey, aciSignedPreKey, pniSignedPreKey, null, pniPqLastResortPreKey),
        Arguments.of(aciIdentityKey, pniIdentityKey, aciSignedPreKey, pniSignedPreKey, aciPqLastResortPreKey, null)
    );
  }

  @Test
  void linkDeviceAtomicMissingCapabilities() {
    final ECSignedPreKey aciSignedPreKey;
    final ECSignedPreKey pniSignedPreKey;
    final KEMSignedPreKey aciPqLastResortPreKey;
    final KEMSignedPreKey pniPqLastResortPreKey;

    final ECKeyPair aciIdentityKeyPair = ECKeyPair.generate();
    final ECKeyPair pniIdentityKeyPair = ECKeyPair.generate();

    aciSignedPreKey = KeysHelper.signedECPreKey(1, aciIdentityKeyPair);
    pniSignedPreKey = KeysHelper.signedECPreKey(2, pniIdentityKeyPair);
    aciPqLastResortPreKey = KeysHelper.signedKEMPreKey(3, aciIdentityKeyPair);
    pniPqLastResortPreKey = KeysHelper.signedKEMPreKey(4, pniIdentityKeyPair);

    when(accountsManager.getByAccountIdentifier(AuthHelper.VALID_UUID)).thenReturn(Optional.of(account));

    final Device existingDevice = mock(Device.class);
    when(existingDevice.getId()).thenReturn(Device.PRIMARY_ID);
    when(account.getDevices()).thenReturn(List.of(existingDevice));

    when(account.getIdentityKey(IdentityType.ACI)).thenReturn(new IdentityKey(aciIdentityKeyPair.getPublicKey()));
    when(account.getIdentityKey(IdentityType.PNI)).thenReturn(new IdentityKey(pniIdentityKeyPair.getPublicKey()));

    when(accountsManager.checkDeviceLinkingToken(anyString())).thenReturn(Optional.of(AuthHelper.VALID_UUID));

    final LinkDeviceRequest request = new LinkDeviceRequest("link-device-token",
        new AccountAttributes(true, 1234, 5678, null, null, true, null),
        new DeviceActivationRequest(aciSignedPreKey, pniSignedPreKey, aciPqLastResortPreKey, pniPqLastResortPreKey, Optional.empty(), Optional.empty()));

    try (final Response response = resources.getJerseyTest()
        .target("/v1/devices/link")
        .request()
        .header("Authorization", AuthHelper.getProvisioningAuthHeader(AuthHelper.VALID_NUMBER, "password1"))
        .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE))) {

      assertEquals(422, response.getStatus());
    }
  }

  @ParameterizedTest
  @MethodSource
  void linkDeviceAtomicInvalidSignature(final IdentityKey aciIdentityKey,
                                        final IdentityKey pniIdentityKey,
                                        final ECSignedPreKey aciSignedPreKey,
                                        final ECSignedPreKey pniSignedPreKey,
                                        final KEMSignedPreKey aciPqLastResortPreKey,
                                        final KEMSignedPreKey pniPqLastResortPreKey) {

    when(accountsManager.getByAccountIdentifier(AuthHelper.VALID_UUID)).thenReturn(Optional.of(account));

    final Device existingDevice = mock(Device.class);
    when(existingDevice.getId()).thenReturn(Device.PRIMARY_ID);
    when(account.getDevices()).thenReturn(List.of(existingDevice));
    when(account.getIdentityKey(IdentityType.ACI)).thenReturn(aciIdentityKey);
    when(account.getIdentityKey(IdentityType.PNI)).thenReturn(pniIdentityKey);

    when(accountsManager.checkDeviceLinkingToken(anyString())).thenReturn(Optional.of(AuthHelper.VALID_UUID));

    final LinkDeviceRequest request = new LinkDeviceRequest("link-device-token",
        new AccountAttributes(true, 1234, 5678, null, null, true, null),
        new DeviceActivationRequest(aciSignedPreKey, pniSignedPreKey, aciPqLastResortPreKey, pniPqLastResortPreKey, Optional.empty(), Optional.empty()));

    try (final Response response = resources.getJerseyTest()
        .target("/v1/devices/link")
        .request()
        .header("Authorization", AuthHelper.getProvisioningAuthHeader(AuthHelper.VALID_NUMBER, "password1"))
        .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE))) {

      assertEquals(422, response.getStatus());
    }
  }

  private static Stream<Arguments> linkDeviceAtomicInvalidSignature() {
    final ECKeyPair aciIdentityKeyPair = ECKeyPair.generate();
    final ECKeyPair pniIdentityKeyPair = ECKeyPair.generate();

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

  @Test
  void linkDeviceAtomicExcessiveDeviceName() {

    when(accountsManager.getByAccountIdentifier(AuthHelper.VALID_UUID)).thenReturn(Optional.of(account));

    final Device existingDevice = mock(Device.class);
    when(existingDevice.getId()).thenReturn(Device.PRIMARY_ID);
    when(account.getDevices()).thenReturn(List.of(existingDevice));

    final ECSignedPreKey aciSignedPreKey;
    final ECSignedPreKey pniSignedPreKey;
    final KEMSignedPreKey aciPqLastResortPreKey;
    final KEMSignedPreKey pniPqLastResortPreKey;

    final ECKeyPair aciIdentityKeyPair = ECKeyPair.generate();
    final ECKeyPair pniIdentityKeyPair = ECKeyPair.generate();

    aciSignedPreKey = KeysHelper.signedECPreKey(1, aciIdentityKeyPair);
    pniSignedPreKey = KeysHelper.signedECPreKey(2, pniIdentityKeyPair);
    aciPqLastResortPreKey = KeysHelper.signedKEMPreKey(3, aciIdentityKeyPair);
    pniPqLastResortPreKey = KeysHelper.signedKEMPreKey(4, pniIdentityKeyPair);

    when(account.getIdentityKey(IdentityType.ACI)).thenReturn(new IdentityKey(aciIdentityKeyPair.getPublicKey()));
    when(account.getIdentityKey(IdentityType.PNI)).thenReturn(new IdentityKey(pniIdentityKeyPair.getPublicKey()));

    final LinkDeviceRequest request = new LinkDeviceRequest("link-device-token",
        new AccountAttributes(false, 1234, 5678, TestRandomUtil.nextBytes(512), null, true, null),
        new DeviceActivationRequest(aciSignedPreKey, pniSignedPreKey, aciPqLastResortPreKey, pniPqLastResortPreKey, Optional.empty(), Optional.of(new GcmRegistrationId("gcm-id"))));

    try (final Response response = resources.getJerseyTest()
        .target("/v1/devices/link")
        .request()
        .header("Authorization", AuthHelper.getProvisioningAuthHeader(AuthHelper.VALID_NUMBER, "password1"))
        .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE))) {

      assertEquals(422, response.getStatus());
    }
  }

  @ParameterizedTest
  @MethodSource
  void linkDeviceRegistrationId(final int registrationId, final int pniRegistrationId, final int expectedStatusCode) {
    final Device existingDevice = mock(Device.class);
    when(existingDevice.getId()).thenReturn(Device.PRIMARY_ID);
    when(account.getDevices()).thenReturn(List.of(existingDevice));

    final ECKeyPair aciIdentityKeyPair = ECKeyPair.generate();
    final ECKeyPair pniIdentityKeyPair = ECKeyPair.generate();

    final ECSignedPreKey aciSignedPreKey = KeysHelper.signedECPreKey(1, aciIdentityKeyPair);
    final ECSignedPreKey pniSignedPreKey = KeysHelper.signedECPreKey(2, pniIdentityKeyPair);
    final KEMSignedPreKey aciPqLastResortPreKey = KeysHelper.signedKEMPreKey(3, aciIdentityKeyPair);
    final KEMSignedPreKey pniPqLastResortPreKey = KeysHelper.signedKEMPreKey(4, pniIdentityKeyPair);
    final IdentityKey aciIdentityKey = new IdentityKey(aciIdentityKeyPair.getPublicKey());

    when(account.getIdentityKey(IdentityType.ACI)).thenReturn(aciIdentityKey);
    when(account.getIdentityKey(IdentityType.PNI)).thenReturn(new IdentityKey(pniIdentityKeyPair.getPublicKey()));

    when(accountsManager.addDevice(any(), any(), any())).thenAnswer(invocation -> {
      final Account a = invocation.getArgument(0);
      final DeviceSpec deviceSpec = invocation.getArgument(1);

      return CompletableFuture.completedFuture(new Pair<>(a, deviceSpec.toDevice(NEXT_DEVICE_ID, testClock, aciIdentityKey)));
    });

    when(accountsManager.checkDeviceLinkingToken(anyString())).thenReturn(Optional.of(AuthHelper.VALID_UUID));

    when(asyncCommands.set(any(), any(), any())).thenReturn(MockRedisFuture.completedFuture(null));

    final LinkDeviceRequest request = new LinkDeviceRequest("link-device-token",
        new AccountAttributes(false, registrationId, pniRegistrationId, null, null, true, Set.of()),
        new DeviceActivationRequest(aciSignedPreKey, pniSignedPreKey, aciPqLastResortPreKey, pniPqLastResortPreKey, Optional.of(new ApnRegistrationId("apn")), Optional.empty()));

    try (final Response response = resources.getJerseyTest()
        .target("/v1/devices/link")
        .request()
        .header("Authorization", AuthHelper.getProvisioningAuthHeader(AuthHelper.VALID_NUMBER, "password1"))
        .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE))) {
      assertEquals(expectedStatusCode, response.getStatus());
    }
  }

  private static Stream<Arguments> linkDeviceRegistrationId() {
    return Stream.of(
        Arguments.of(0x3FFF, 0x3FFF, 200),
        Arguments.of(0, 0x3FFF, 422),
        Arguments.of(-1, 0x3FFF, 422),
        Arguments.of(0x3FFF + 1, 0x3FFF, 422),
        Arguments.of(Integer.MAX_VALUE, 0x3FFF, 422),
        Arguments.of(0x3FFF, 0, 422),
        Arguments.of(0x3FFF, -1, 422),
        Arguments.of(0x3FFF, 0x3FFF + 1, 422),
        Arguments.of(0x3FFF, Integer.MAX_VALUE, 422)
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
  void maxDevicesTest() {
    final List<Device> devices = IntStream.range(0, DeviceController.MAX_DEVICES + 1)
        .mapToObj(i -> mock(Device.class))
        .toList();

    when(account.getDevices()).thenReturn(devices);

    Response response = resources.getJerseyTest()
        .target("/v1/devices/provisioning/code")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get();

    assertEquals(411, response.getStatus());
    verify(accountsManager, never()).addDevice(any(), any(), any());
  }

  @Test
  void putCapabilitiesSuccessTest() {
    try (final Response response = resources
        .getJerseyTest()
        .target("/v1/devices/capabilities")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .header(HttpHeaders.USER_AGENT, "Signal-Android/5.42.8675309 Android/30")
        .put(Entity.json("{\"deleteSync\": true, \"notARealDeviceCapability\": true}"))) {

      assertThat(response.getStatus()).isEqualTo(204);
      assertThat(response.hasEntity()).isFalse();
      verify(primaryDevice).setCapabilities(Set.of(DeviceCapability.DELETE_SYNC));
    }
  }

  @Test
  void putCapabilitiesFailureTest() {
    try (final Response response = resources
        .getJerseyTest()
        .target("/v1/devices/capabilities")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .header(HttpHeaders.USER_AGENT, "Signal-Android/5.42.8675309 Android/30")
        .put(Entity.json(""))) {

      assertThat(response.getStatus()).isEqualTo(422);
    }
  }

  @Test
  void removeDevice() {

    // this is a static mock, so it might have previous invocations
    clearInvocations(account);

    final byte deviceId = 2;

    when(accountsManager.removeDevice(account, deviceId))
        .thenReturn(CompletableFuture.completedFuture(account));

    try (final Response response = resources
        .getJerseyTest()
        .target("/v1/devices/" + deviceId)
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .header(HttpHeaders.USER_AGENT, "Signal-Android/5.42.8675309 Android/30")
        .delete()) {

      assertThat(response.getStatus()).isEqualTo(204);
      assertThat(response.hasEntity()).isFalse();

      verify(accountsManager).removeDevice(account, deviceId);
    }
  }

  @Test
  void unlinkPrimaryDevice() {
    // this is a static mock, so it might have previous invocations
    clearInvocations(account);

    try (final Response response = resources
        .getJerseyTest()
        .target("/v1/devices/" + Device.PRIMARY_ID)
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .header(HttpHeaders.USER_AGENT, "Signal-Android/5.42.8675309 Android/30")
        .delete()) {

      assertThat(response.getStatus()).isEqualTo(403);

      verify(accountsManager, never()).removeDevice(any(), anyByte());
    }
  }

  @Test
  void removeDeviceBySelf() {
    final byte deviceId = 2;

    when(accountsManager.removeDevice(AuthHelper.VALID_ACCOUNT_3, deviceId))
        .thenReturn(CompletableFuture.completedFuture(account));

    when(accountsManager.getByAccountIdentifier(AuthHelper.VALID_UUID_3))
        .thenReturn(Optional.of(AuthHelper.VALID_ACCOUNT_3));

    try (final Response response = resources
        .getJerseyTest()
        .target("/v1/devices/" + deviceId)
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_3, deviceId, AuthHelper.VALID_PASSWORD_3_LINKED))
        .header(HttpHeaders.USER_AGENT, "Signal-Android/5.42.8675309 Android/30")
        .delete()) {

      assertThat(response.getStatus()).isEqualTo(204);
      assertThat(response.hasEntity()).isFalse();

      verify(accountsManager).removeDevice(AuthHelper.VALID_ACCOUNT_3, deviceId);
    }
  }

  @Test
  void removeDeviceByOther() {
    final byte deviceId = 2;
    final byte otherDeviceId = 3;

    try (final Response response = resources
        .getJerseyTest()
        .target("/v1/devices/" + otherDeviceId)
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_3, deviceId, AuthHelper.VALID_PASSWORD_3_LINKED))
        .header(HttpHeaders.USER_AGENT, "Signal-Android/5.42.8675309 Android/30")
        .delete()) {

      assertThat(response.getStatus()).isEqualTo(401);

      verify(accountsManager, never()).removeDevice(any(), anyByte());
    }
  }

  @Test
  void setPublicKey() {
    final SetPublicKeyRequest request = new SetPublicKeyRequest(ECKeyPair.generate().getPublicKey());

    try (final Response response = resources.getJerseyTest()
        .target("/v1/devices/public_key")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE))) {

      assertEquals(204, response.getStatus());
    }

    verify(clientPublicKeysManager).setPublicKey(account, AuthHelper.VALID_DEVICE.getId(), request.publicKey());
  }

  @Test
  void waitForLinkedDevice() {
    final DeviceInfo deviceInfo = new DeviceInfo(Device.PRIMARY_ID,
        "Device name ciphertext".getBytes(StandardCharsets.UTF_8),
        System.currentTimeMillis(),
        System.currentTimeMillis(),
        1,
        "timestamp ciphertext".getBytes(StandardCharsets.UTF_8));

    final String tokenIdentifier = Base64.getUrlEncoder().withoutPadding().encodeToString(new byte[32]);

    when(accountsManager
        .waitForNewLinkedDevice(eq(AuthHelper.VALID_UUID), eq(primaryDevice), eq(tokenIdentifier), any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(deviceInfo)));

    when(rateLimiter.validateAsync(AuthHelper.VALID_UUID)).thenReturn(CompletableFuture.completedFuture(null));

    try (final Response response = resources.getJerseyTest()
        .target("/v1/devices/wait_for_linked_device/" + tokenIdentifier)
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get()) {

      assertEquals(200, response.getStatus());

      final DeviceInfo retrievedDeviceInfo = response.readEntity(DeviceInfo.class);
      assertEquals(deviceInfo.id(), retrievedDeviceInfo.id());
      assertArrayEquals(deviceInfo.name(), retrievedDeviceInfo.name());
      assertEquals(deviceInfo.created(), retrievedDeviceInfo.created());
      assertEquals(deviceInfo.lastSeen(), retrievedDeviceInfo.lastSeen());
      assertEquals(deviceInfo.registrationId(), retrievedDeviceInfo.registrationId());
      assertArrayEquals(deviceInfo.createdAtCiphertext(), retrievedDeviceInfo.createdAtCiphertext());
    }
  }

  @Test
  void waitForLinkedDeviceNoDeviceLinked() {
    final String tokenIdentifier = Base64.getUrlEncoder().withoutPadding().encodeToString(new byte[32]);

    when(accountsManager
        .waitForNewLinkedDevice(eq(AuthHelper.VALID_UUID), eq(primaryDevice), eq(tokenIdentifier), any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    when(rateLimiter.validateAsync(AuthHelper.VALID_UUID)).thenReturn(CompletableFuture.completedFuture(null));

    try (final Response response = resources.getJerseyTest()
        .target("/v1/devices/wait_for_linked_device/" + tokenIdentifier)
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get()) {

      assertEquals(204, response.getStatus());
    }
  }

  @Test
  void waitForLinkedDeviceBadTokenIdentifier() {
    final String tokenIdentifier = Base64.getUrlEncoder().withoutPadding().encodeToString(new byte[32]);

    when(accountsManager
        .waitForNewLinkedDevice(eq(AuthHelper.VALID_UUID), eq(primaryDevice), eq(tokenIdentifier), any()))
        .thenReturn(CompletableFuture.failedFuture(new IllegalArgumentException()));

    when(rateLimiter.validateAsync(AuthHelper.VALID_UUID)).thenReturn(CompletableFuture.completedFuture(null));

    try (final Response response = resources.getJerseyTest()
        .target("/v1/devices/wait_for_linked_device/" + tokenIdentifier)
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get()) {

      assertEquals(400, response.getStatus());
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, -1, 3601})
  void waitForLinkedDeviceBadTimeout(final int timeoutSeconds) {
    final String tokenIdentifier = Base64.getUrlEncoder().withoutPadding().encodeToString(new byte[32]);

    try (final Response response = resources.getJerseyTest()
        .target("/v1/devices/wait_for_linked_device/" + tokenIdentifier)
        .queryParam("timeout", timeoutSeconds)
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get()) {

      assertEquals(400, response.getStatus());
    }
  }

  @ParameterizedTest
  @MethodSource
  void waitForLinkedDeviceBadTokenIdentifierLength(final String tokenIdentifier) {
    try (final Response response = resources.getJerseyTest()
        .target("/v1/devices/wait_for_linked_device/" + tokenIdentifier)
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get()) {

      assertEquals(400, response.getStatus());
    }
  }

  private static List<String> waitForLinkedDeviceBadTokenIdentifierLength() {
    return List.of(RandomStringUtils.secure().nextAlphanumeric(DeviceController.MIN_TOKEN_IDENTIFIER_LENGTH - 1),
        RandomStringUtils.secure().nextAlphanumeric(DeviceController.MAX_TOKEN_IDENTIFIER_LENGTH + 1));
  }

  @Test
  void waitForLinkedDeviceRateLimited() {
    final String tokenIdentifier = Base64.getUrlEncoder().withoutPadding().encodeToString(new byte[32]);

    when(rateLimiter.validateAsync(AuthHelper.VALID_UUID))
        .thenReturn(CompletableFuture.failedFuture(new RateLimitExceededException(null)));

    try (final Response response = resources.getJerseyTest()
        .target("/v1/devices/wait_for_linked_device/" + tokenIdentifier)
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get()) {

      assertEquals(429, response.getStatus());
    }
  }

  @ParameterizedTest
  @MethodSource
  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  void recordTransferArchiveUploaded(final Optional<Instant> deviceCreated, final Optional<Integer> registrationId) {
    final byte deviceId = Device.PRIMARY_ID + 1;
    final RemoteAttachment transferArchive =
        new RemoteAttachment(3, Base64.getUrlEncoder().encodeToString("test".getBytes(StandardCharsets.UTF_8)));

    when(rateLimiter.validateAsync(AuthHelper.VALID_UUID)).thenReturn(CompletableFuture.completedFuture(null));
    when(accountsManager.recordTransferArchiveUpload(account, deviceId, deviceCreated, registrationId, transferArchive))
        .thenReturn(CompletableFuture.completedFuture(null));

    try (final Response response = resources.getJerseyTest()
        .target("/v1/devices/transfer_archive")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .put(Entity.entity(new TransferArchiveUploadedRequest(deviceId, deviceCreated.map(Instant::toEpochMilli), registrationId, transferArchive),
            MediaType.APPLICATION_JSON_TYPE))) {

      assertEquals(204, response.getStatus());

      verify(accountsManager)
          .recordTransferArchiveUpload(account, deviceId, deviceCreated, registrationId, transferArchive);
    }
  }

  private static List<Arguments> recordTransferArchiveUploaded() {
    return List.of(
        Arguments.of(Optional.empty(), Optional.of(123)),
        Arguments.of(Optional.of(Instant.now().truncatedTo(ChronoUnit.MILLIS)), Optional.empty())
    );
  }

  @Test
  void recordTransferArchiveFailed() {
    final byte deviceId = Device.PRIMARY_ID + 1;
    final Instant deviceCreated = Instant.now().truncatedTo(ChronoUnit.MILLIS);
    final RemoteAttachmentError transferFailure = new RemoteAttachmentError(RemoteAttachmentError.ErrorType.CONTINUE_WITHOUT_UPLOAD);

    when(rateLimiter.validateAsync(AuthHelper.VALID_UUID)).thenReturn(CompletableFuture.completedFuture(null));
    when(accountsManager.recordTransferArchiveUpload(account, deviceId, Optional.of(deviceCreated), Optional.empty(), transferFailure))
        .thenReturn(CompletableFuture.completedFuture(null));

    try (final Response response = resources.getJerseyTest()
        .target("/v1/devices/transfer_archive")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .put(Entity.entity(new TransferArchiveUploadedRequest(deviceId, Optional.of(deviceCreated.toEpochMilli()), Optional.empty(), transferFailure),
            MediaType.APPLICATION_JSON_TYPE))) {

      assertEquals(204, response.getStatus());

      verify(accountsManager)
          .recordTransferArchiveUpload(account, deviceId, Optional.of(deviceCreated), Optional.empty(), transferFailure);
    }
  }

  @ParameterizedTest
  @MethodSource
  void recordTransferArchiveUploadedBadRequest(final TransferArchiveUploadedRequest request) {
    when(rateLimiter.validateAsync(AuthHelper.VALID_UUID)).thenReturn(CompletableFuture.completedFuture(null));

    try (final Response response = resources.getJerseyTest()
        .target("/v1/devices/transfer_archive")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE))) {

      assertEquals(422, response.getStatus());

      verify(accountsManager, never())
          .recordTransferArchiveUpload(any(), anyByte(), any(), any(), any());
    }
  }

  @SuppressWarnings("DataFlowIssue")
  private static List<Arguments> recordTransferArchiveUploadedBadRequest() {
    final RemoteAttachment validTransferArchive =
        new RemoteAttachment(3, Base64.getUrlEncoder().encodeToString("archive".getBytes(StandardCharsets.UTF_8)));

    return List.of(
        Arguments.argumentSet("Invalid device ID", new TransferArchiveUploadedRequest((byte) -1, Optional.of(System.currentTimeMillis()), Optional.empty(), validTransferArchive)),
        Arguments.argumentSet("Invalid \"created at\" timestamp",
            new TransferArchiveUploadedRequest(Device.PRIMARY_ID, Optional.of((long) -1), Optional.empty(), validTransferArchive)),
        Arguments.argumentSet("Invalid registration ID - negative",
            new TransferArchiveUploadedRequest(Device.PRIMARY_ID, Optional.empty(), Optional.of(-1), validTransferArchive)),
        Arguments.argumentSet("Invalid registration ID - too large",
            new TransferArchiveUploadedRequest(Device.PRIMARY_ID, Optional.empty(), Optional.of(0x4000), validTransferArchive)),
        Arguments.argumentSet("Exactly one of \"created at\" timestamp and registration ID must be present - neither provided",
            new TransferArchiveUploadedRequest(Device.PRIMARY_ID, Optional.empty(), Optional.empty(), validTransferArchive)),
        Arguments.argumentSet("Exactly one of \"created at\" timestamp and registration ID must be present - both provided",
            new TransferArchiveUploadedRequest(Device.PRIMARY_ID, Optional.of(System.currentTimeMillis()), Optional.of(123), validTransferArchive)),
        Arguments.argumentSet("Missing CDN number",
            new TransferArchiveUploadedRequest(Device.PRIMARY_ID, Optional.of(System.currentTimeMillis()), Optional.empty(),
                new RemoteAttachment(null, Base64.getUrlEncoder().encodeToString("archive".getBytes(StandardCharsets.UTF_8))))),
        Arguments.argumentSet("Bad attachment key",
            new TransferArchiveUploadedRequest(Device.PRIMARY_ID, Optional.of(System.currentTimeMillis()), Optional.empty(),
                new RemoteAttachment(3, "This is not a valid base64 string")))
    );
  }

  @Test
  void recordTransferArchiveRateLimited() {
    when(rateLimiter.validateAsync(AuthHelper.VALID_UUID))
        .thenReturn(CompletableFuture.failedFuture(new RateLimitExceededException(null)));

    try (final Response response = resources.getJerseyTest()
        .target("/v1/devices/transfer_archive")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .put(Entity.entity(new TransferArchiveUploadedRequest(Device.PRIMARY_ID, Optional.of(System.currentTimeMillis()), Optional.empty(),
            new RemoteAttachment(3, Base64.getUrlEncoder().encodeToString("test".getBytes(StandardCharsets.UTF_8)))),
            MediaType.APPLICATION_JSON_TYPE))) {

      assertEquals(429, response.getStatus());

      verify(accountsManager, never())
          .recordTransferArchiveUpload(any(), anyByte(), any(), any(), any());
    }
  }

  @Test
  void waitForTransferArchive() {
    final RemoteAttachment transferArchive =
        new RemoteAttachment(3, Base64.getUrlEncoder().encodeToString("test".getBytes(StandardCharsets.UTF_8)));

    when(rateLimiter.validateAsync(anyString())).thenReturn(CompletableFuture.completedFuture(null));
    when(accountsManager.waitForTransferArchive(eq(account), eq(primaryDevice), any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(transferArchive)));

    try (final Response response = resources.getJerseyTest()
        .target("/v1/devices/transfer_archive/")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get()) {

      assertEquals(200, response.getStatus());
      assertEquals(transferArchive, response.readEntity(RemoteAttachment.class));
    }
  }

  @Test
  void waitForTransferArchiveUploadFailed() {
    final RemoteAttachment transferArchive =
        new RemoteAttachment(3, Base64.getUrlEncoder().encodeToString("test".getBytes(StandardCharsets.UTF_8)));

    when(rateLimiter.validateAsync(anyString())).thenReturn(CompletableFuture.completedFuture(null));
    when(accountsManager.waitForTransferArchive(eq(account), eq(primaryDevice), any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(transferArchive)));

    try (final Response response = resources.getJerseyTest()
        .target("/v1/devices/transfer_archive/")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get()) {

      assertEquals(200, response.getStatus());
      assertEquals(transferArchive, response.readEntity(RemoteAttachment.class));
    }
  }

  @Test
  void waitForTransferArchiveNoArchiveUploaded() {
    when(rateLimiter.validateAsync(anyString())).thenReturn(CompletableFuture.completedFuture(null));
    when(accountsManager.waitForTransferArchive(eq(account), eq(primaryDevice), any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    try (final Response response = resources.getJerseyTest()
        .target("/v1/devices/transfer_archive/")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get()) {

      assertEquals(204, response.getStatus());
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, -1, 3601})
  void waitForTransferArchiveBadTimeout(final int timeoutSeconds) {
    try (final Response response = resources.getJerseyTest()
        .target("/v1/devices/transfer_archive/")
        .queryParam("timeout", timeoutSeconds)
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get()) {

      assertEquals(400, response.getStatus());
    }
  }

  @Test
  void waitForTransferArchiveRateLimited() {
    when(rateLimiter.validateAsync(anyString()))
        .thenReturn(CompletableFuture.failedFuture(new RateLimitExceededException(null)));

    try (final Response response = resources.getJerseyTest()
        .target("/v1/devices/transfer_archive/")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get()) {

      assertEquals(429, response.getStatus());
    }
  }

  @Test
  void recordRestoreAccountRequest() {
    final String token = RandomStringUtils.secure().nextAlphanumeric(16);
    final RestoreAccountRequest restoreAccountRequest =
        new RestoreAccountRequest(RestoreAccountRequest.Method.LOCAL_BACKUP, null);

    when(accountsManager.recordRestoreAccountRequest(token, restoreAccountRequest))
        .thenReturn(CompletableFuture.completedFuture(null));

    try (final Response response = resources.getJerseyTest()
        .target("/v1/devices/restore_account/" + token)
        .request()
        .put(Entity.json(restoreAccountRequest))) {

      assertEquals(204, response.getStatus());
    }
  }

  @Test
  void recordRestoreAccountRequestBadToken() {
    final String token = RandomStringUtils.secure().nextAlphanumeric(128);
    final RestoreAccountRequest restoreAccountRequest =
        new RestoreAccountRequest(RestoreAccountRequest.Method.LOCAL_BACKUP, null);

    try (final Response response = resources.getJerseyTest()
        .target("/v1/devices/restore_account/" + token)
        .request()
        .put(Entity.json(restoreAccountRequest))) {

      assertEquals(400, response.getStatus());
    }
  }

  @Test
  void recordRestoreAccountRequestInvalidRequest() {
    final String token = RandomStringUtils.secure().nextAlphanumeric(16);
    final RestoreAccountRequest restoreAccountRequest = new RestoreAccountRequest(null, null);

    try (final Response response = resources.getJerseyTest()
        .target("/v1/devices/restore_account/" + token)
        .request()
        .put(Entity.json(restoreAccountRequest))) {

      assertEquals(422, response.getStatus());
    }
  }

  @ParameterizedTest
  @CsvSource({
      "0, true",
      "4096, true",
      "4097, false"
  })
  void recordRestoreAccountRequestBootstrapLengthLimit(int bootstrapLength, boolean valid) {
    final String token = RandomStringUtils.secure().nextAlphanumeric(16);

    final byte[] bootstrap = TestRandomUtil.nextBytes(bootstrapLength);
    final RestoreAccountRequest restoreAccountRequest = new RestoreAccountRequest(
        RestoreAccountRequest.Method.DEVICE_TRANSFER, bootstrap);

    try (final Response response = resources.getJerseyTest()
        .target("/v1/devices/restore_account/" + token)
        .request()
        .put(Entity.json(restoreAccountRequest))) {

      assertEquals(valid ? 204 : 422, response.getStatus());
    }

  }

  @Test
  void waitForDeviceTransferRequest() {
    final String token = RandomStringUtils.secure().nextAlphanumeric(16);
    final RestoreAccountRequest restoreAccountRequest =
        new RestoreAccountRequest(RestoreAccountRequest.Method.LOCAL_BACKUP, null);

    when(accountsManager.waitForRestoreAccountRequest(eq(token), any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(restoreAccountRequest)));

    try (final Response response = resources.getJerseyTest()
        .target("/v1/devices/restore_account/" + token)
        .request()
        .get()) {

      assertEquals(200, response.getStatus());
      assertEquals(restoreAccountRequest, response.readEntity(RestoreAccountRequest.class));
    }
  }

  @Test
  void waitForDeviceTransferRequestNoRequestIssued() {
    final String token = RandomStringUtils.secure().nextAlphanumeric(16);

    when(accountsManager.waitForRestoreAccountRequest(eq(token), any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    try (final Response response = resources.getJerseyTest()
        .target("/v1/devices/restore_account/" + token)
        .request()
        .get()) {

      assertEquals(204, response.getStatus());
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, -1, 3601})
  void waitForDeviceTransferRequestBadTimeout(final int timeoutSeconds) {
    final String token = RandomStringUtils.secure().nextAlphanumeric(16);

    try (final Response response = resources.getJerseyTest()
        .target("/v1/devices/restore_account/" + token)
        .queryParam("timeout", timeoutSeconds)
        .request()
        .get()) {

      assertEquals(400, response.getStatus());
    }
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {""})
  void linkDeviceMissingVerificationCode(final String verificationCode) {
    final AccountAttributes accountAttributes = new AccountAttributes(true, 1234, 5678, null,
        null, true, Set.of());

    final ECKeyPair aciIdentityKeyPair = ECKeyPair.generate();
    final ECKeyPair pniIdentityKeyPair = ECKeyPair.generate();

    final LinkDeviceRequest request = new LinkDeviceRequest(verificationCode,
        accountAttributes,
        new DeviceActivationRequest(
            KeysHelper.signedECPreKey(1, aciIdentityKeyPair),
            KeysHelper.signedECPreKey(2, pniIdentityKeyPair),
            KeysHelper.signedKEMPreKey(3, aciIdentityKeyPair),
            KeysHelper.signedKEMPreKey(4, pniIdentityKeyPair),
            Optional.empty(),
            Optional.empty()));

    try (final Response response = resources.getJerseyTest()
        .target("/v1/devices/link")
        .request()
        .header("Authorization", AuthHelper.getProvisioningAuthHeader(AuthHelper.VALID_NUMBER, "password1"))
        .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE))) {
      assertEquals(422, response.getStatus());
    }
  }
}
