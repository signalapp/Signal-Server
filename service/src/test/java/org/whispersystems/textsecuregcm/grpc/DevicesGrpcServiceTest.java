/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.whispersystems.textsecuregcm.grpc.GrpcTestUtils.assertStatusException;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.mockito.Mock;
import org.signal.chat.device.ClearPushTokenRequest;
import org.signal.chat.device.ClearPushTokenResponse;
import org.signal.chat.device.DevicesGrpc;
import org.signal.chat.device.GetDevicesRequest;
import org.signal.chat.device.GetDevicesResponse;
import org.signal.chat.device.RemoveDeviceRequest;
import org.signal.chat.device.RemoveDeviceResponse;
import org.signal.chat.device.SetCapabilitiesRequest;
import org.signal.chat.device.SetCapabilitiesResponse;
import org.signal.chat.device.SetDeviceNameRequest;
import org.signal.chat.device.SetDeviceNameResponse;
import org.signal.chat.device.SetPushTokenRequest;
import org.signal.chat.device.SetPushTokenResponse;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.DeviceCapability;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

class DevicesGrpcServiceTest extends SimpleBaseGrpcTest<DevicesGrpcService, DevicesGrpc.DevicesBlockingStub> {

  @Mock
  private AccountsManager accountsManager;

  @Mock
  private Account authenticatedAccount;

  @Override
  protected DevicesGrpcService createServiceBeforeEachTest() {
    when(authenticatedAccount.getUuid()).thenReturn(AUTHENTICATED_ACI);

    when(accountsManager.getByAccountIdentifierAsync(AUTHENTICATED_ACI))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(authenticatedAccount)));

    when(accountsManager.removeDevice(any(), anyByte()))
        .thenReturn(CompletableFuture.completedFuture(authenticatedAccount));

    when(accountsManager.updateAsync(any(), any()))
        .thenAnswer(invocation -> {
          final Account account = invocation.getArgument(0);
          final Consumer<Account> updater = invocation.getArgument(1);

          updater.accept(account);

          return CompletableFuture.completedFuture(account);
        });

    when(accountsManager.updateDeviceAsync(any(), anyByte(), any()))
        .thenAnswer(invocation -> {
          final Account account = invocation.getArgument(0);
          final Device device = account.getDevice(invocation.getArgument(1)).orElseThrow();
          final Consumer<Device> updater = invocation.getArgument(2);

          updater.accept(device);

          return CompletableFuture.completedFuture(account);
        });

    return new DevicesGrpcService(accountsManager);
  }

  @Test
  void getDevices() {
    final Instant primaryDeviceCreated = Instant.now().minus(Duration.ofDays(7)).truncatedTo(ChronoUnit.MILLIS);
    final Instant primaryDeviceLastSeen = primaryDeviceCreated.plus(Duration.ofHours(6));
    final Instant linkedDeviceCreated = Instant.now().minus(Duration.ofDays(1)).truncatedTo(ChronoUnit.MILLIS);
    final Instant linkedDeviceLastSeen = linkedDeviceCreated.plus(Duration.ofHours(7));
    final int primaryRegistrationId = 1234;
    final int linkedRegistrationId = 1235;
    final byte[] primaryCreatedAtCiphertext = "primary_timestamp_ciphertext".getBytes(StandardCharsets.UTF_8);
    final byte[] linkedCreatedAtCiphertext = "linked_timestamp_ciphertext".getBytes(StandardCharsets.UTF_8);

    final Device primaryDevice = mock(Device.class);
    when(primaryDevice.getId()).thenReturn(Device.PRIMARY_ID);
    when(primaryDevice.getCreated()).thenReturn(primaryDeviceCreated.toEpochMilli());
    when(primaryDevice.getLastSeen()).thenReturn(primaryDeviceLastSeen.toEpochMilli());
    when(primaryDevice.getRegistrationId(IdentityType.ACI)).thenReturn(primaryRegistrationId);
    when(primaryDevice.getCreatedAtCiphertext()).thenReturn(primaryCreatedAtCiphertext);

    final String linkedDeviceName = "A linked device";

    final Device linkedDevice = mock(Device.class);
    when(linkedDevice.getId()).thenReturn((byte) (Device.PRIMARY_ID + 1));
    when(linkedDevice.getCreated()).thenReturn(linkedDeviceCreated.toEpochMilli());
    when(linkedDevice.getLastSeen()).thenReturn(linkedDeviceLastSeen.toEpochMilli());
    when(linkedDevice.getName()).thenReturn(linkedDeviceName.getBytes(StandardCharsets.UTF_8));
    when(linkedDevice.getRegistrationId(IdentityType.ACI)).thenReturn(linkedRegistrationId);
    when(linkedDevice.getCreatedAtCiphertext()).thenReturn(linkedCreatedAtCiphertext);

    when(authenticatedAccount.getDevices()).thenReturn(List.of(primaryDevice, linkedDevice));

    final GetDevicesResponse expectedResponse = GetDevicesResponse.newBuilder()
        .addDevices(GetDevicesResponse.LinkedDevice.newBuilder()
            .setId(Device.PRIMARY_ID)
            .setCreated(primaryDeviceCreated.toEpochMilli())
            .setLastSeen(primaryDeviceLastSeen.toEpochMilli())
            .setRegistrationId(primaryRegistrationId)
            .setCreatedAtCiphertext(ByteString.copyFrom(primaryCreatedAtCiphertext))
            .build())
        .addDevices(GetDevicesResponse.LinkedDevice.newBuilder()
            .setId(Device.PRIMARY_ID + 1)
            .setCreated(linkedDeviceCreated.toEpochMilli())
            .setLastSeen(linkedDeviceLastSeen.toEpochMilli())
            .setName(ByteString.copyFrom(linkedDeviceName.getBytes(StandardCharsets.UTF_8)))
            .setRegistrationId(linkedRegistrationId)
            .setCreatedAtCiphertext(ByteString.copyFrom(linkedCreatedAtCiphertext))
            .build())
        .build();

    assertEquals(expectedResponse, authenticatedServiceStub().getDevices(GetDevicesRequest.newBuilder().build()));
  }

  @Test
  void removeDevice() {
    final byte deviceId = 17;

    final RemoveDeviceResponse ignored = authenticatedServiceStub().removeDevice(RemoveDeviceRequest.newBuilder()
        .setId(deviceId)
        .build());

    verify(accountsManager).removeDevice(authenticatedAccount, deviceId);
  }

  @Test
  void removeDevicePrimary() {
    assertStatusException(Status.INVALID_ARGUMENT, () -> authenticatedServiceStub().removeDevice(RemoveDeviceRequest.newBuilder()
        .setId(1)
        .build()));

    verify(accountsManager, never()).removeDevice(any(), anyByte());
  }

  @Test
  void removeDeviceNonPrimaryMatchAuthenticated() {
    final byte deviceId = Device.PRIMARY_ID + 1;

    mockAuthenticationInterceptor().setAuthenticatedDevice(AUTHENTICATED_ACI, deviceId);

    final RemoveDeviceResponse ignored = authenticatedServiceStub().removeDevice(RemoveDeviceRequest.newBuilder()
        .setId(deviceId)
        .build());

    verify(accountsManager).removeDevice(authenticatedAccount, deviceId);
  }

  @Test
  void removeDeviceNonPrimaryMismatchAuthenticated() {
    mockAuthenticationInterceptor().setAuthenticatedDevice(AUTHENTICATED_ACI, (byte) (Device.PRIMARY_ID + 1));
    assertStatusException(Status.PERMISSION_DENIED, () -> authenticatedServiceStub().removeDevice(RemoveDeviceRequest.newBuilder()
        .setId(17)
        .build()));

    verify(accountsManager, never()).removeDevice(any(), anyByte());
  }

  @ParameterizedTest
  @ValueSource(bytes = {Device.PRIMARY_ID, Device.PRIMARY_ID + 1})
  void setDeviceName(final byte deviceId) {
    mockAuthenticationInterceptor().setAuthenticatedDevice(AUTHENTICATED_ACI, deviceId);

    final Device device = mock(Device.class);
    when(authenticatedAccount.getDevice(deviceId)).thenReturn(Optional.of(device));

    final byte[] deviceName = TestRandomUtil.nextBytes(128);

    final SetDeviceNameResponse ignored = authenticatedServiceStub().setDeviceName(SetDeviceNameRequest.newBuilder()
        .setId(deviceId)
        .setName(ByteString.copyFrom(deviceName))
        .build());

    verify(device).setName(deviceName);
  }

  @Test
  void setLinkedDeviceNameFromPrimary() {
    mockAuthenticationInterceptor().setAuthenticatedDevice(AUTHENTICATED_ACI, Device.PRIMARY_ID);

    final byte deviceId = Device.PRIMARY_ID + 1;

    final Device device = mock(Device.class);
    when(authenticatedAccount.getDevice(deviceId)).thenReturn(Optional.of(device));

    final byte[] deviceName = TestRandomUtil.nextBytes(128);

    final SetDeviceNameResponse ignored = authenticatedServiceStub().setDeviceName(SetDeviceNameRequest.newBuilder()
        .setId(deviceId)
        .setName(ByteString.copyFrom(deviceName))
        .build());

    verify(device).setName(deviceName);
  }

  @Test
  void setPrimaryDeviceNameFromLinkedDevice() {
    mockAuthenticationInterceptor().setAuthenticatedDevice(AUTHENTICATED_ACI, (byte) (Device.PRIMARY_ID + 1));

    final byte deviceId = Device.PRIMARY_ID;

    final Device device = mock(Device.class);
    when(authenticatedAccount.getDevice(deviceId)).thenReturn(Optional.of(device));

    final byte[] deviceName = TestRandomUtil.nextBytes(128);

    assertStatusException(Status.PERMISSION_DENIED,
        () -> authenticatedServiceStub().setDeviceName(SetDeviceNameRequest.newBuilder()
            .setId(deviceId)
            .setName(ByteString.copyFrom(deviceName))
            .build()));

    verify(device, never()).setName(deviceName);
  }

  @Test
  void setDeviceNameNotFound() {
    mockAuthenticationInterceptor().setAuthenticatedDevice(AUTHENTICATED_ACI, Device.PRIMARY_ID);
    when(authenticatedAccount.getDevice(anyByte())).thenReturn(Optional.empty());

    final byte[] deviceName = TestRandomUtil.nextBytes(128);

    assertStatusException(Status.NOT_FOUND,
        () -> authenticatedServiceStub().setDeviceName(SetDeviceNameRequest.newBuilder()
            .setId(Device.PRIMARY_ID + 1)
            .setName(ByteString.copyFrom(deviceName))
            .build()));
  }

  @ParameterizedTest
  @MethodSource
  void setDeviceNameIllegalArgument(final SetDeviceNameRequest request) {
    when(authenticatedAccount.getDevice(AUTHENTICATED_DEVICE_ID)).thenReturn(Optional.of(mock(Device.class)));
    assertStatusException(Status.INVALID_ARGUMENT, () -> authenticatedServiceStub().setDeviceName(request));
  }

  private static Stream<Arguments> setDeviceNameIllegalArgument() {
    return Stream.of(
        // No device name
        Arguments.of(SetDeviceNameRequest.newBuilder()
            .setId(Device.PRIMARY_ID)
            .build()),

        // Excessively-long device name
        Arguments.of(SetDeviceNameRequest.newBuilder()
            .setId(Device.PRIMARY_ID)
            .setName(ByteString.copyFrom(TestRandomUtil.nextBytes(1024)))
            .build()),

        // No device ID
        Arguments.of(SetDeviceNameRequest.newBuilder()
            .setName(ByteString.copyFrom(TestRandomUtil.nextBytes(32)))
            .build()),

        // Out-of-bounds device ID
        Arguments.of(SetDeviceNameRequest.newBuilder()
            .setId(Device.MAXIMUM_DEVICE_ID + 1)
            .setName(ByteString.copyFrom(TestRandomUtil.nextBytes(32)))
            .build())
    );
  }

  @ParameterizedTest
  @MethodSource
  void setPushToken(final byte deviceId,
      final SetPushTokenRequest request,
      @Nullable final String expectedApnsToken,
      @Nullable final String expectedFcmToken) {

    mockAuthenticationInterceptor().setAuthenticatedDevice(AUTHENTICATED_ACI, deviceId);

    final Device device = mock(Device.class);
    when(authenticatedAccount.getDevice(deviceId)).thenReturn(Optional.of(device));

    final SetPushTokenResponse ignored = authenticatedServiceStub().setPushToken(request);

    verify(device).setApnId(expectedApnsToken);
    verify(device).setGcmId(expectedFcmToken);
    verify(device).setFetchesMessages(false);
  }

  private static Stream<Arguments> setPushToken() {
    final String apnsToken = "apns-token";
    final String fcmToken = "fcm-token";

    final Stream.Builder<Arguments> streamBuilder = Stream.builder();

    for (final byte deviceId : new byte[]{Device.PRIMARY_ID, Device.PRIMARY_ID + 1}) {
      streamBuilder.add(Arguments.of(deviceId,
          SetPushTokenRequest.newBuilder()
              .setApnsTokenRequest(SetPushTokenRequest.ApnsTokenRequest.newBuilder()
                  .setApnsToken(apnsToken)
                  .build())
              .build(),
          apnsToken, null));

      streamBuilder.add(Arguments.of(deviceId,
          SetPushTokenRequest.newBuilder()
              .setFcmTokenRequest(SetPushTokenRequest.FcmTokenRequest.newBuilder()
                  .setFcmToken(fcmToken)
                  .build())
              .build(),
          null, fcmToken));
    }

    return streamBuilder.build();
  }

  @ParameterizedTest
  @MethodSource
  void setPushTokenUnchanged(final SetPushTokenRequest request,
      @Nullable final String apnsToken,
      @Nullable final String fcmToken) {

    final Device device = mock(Device.class);
    when(device.getApnId()).thenReturn(apnsToken);
    when(device.getGcmId()).thenReturn(fcmToken);

    when(authenticatedAccount.getDevice(AUTHENTICATED_DEVICE_ID)).thenReturn(Optional.of(device));

    final SetPushTokenResponse ignored = authenticatedServiceStub().setPushToken(request);

    verify(accountsManager, never()).updateDevice(any(), anyByte(), any());
  }

  private static Stream<Arguments> setPushTokenUnchanged() {
    final String apnsToken = "apns-token";
    final String fcmToken = "fcm-token";

    return Stream.of(
        Arguments.of(SetPushTokenRequest.newBuilder()
                .setApnsTokenRequest(SetPushTokenRequest.ApnsTokenRequest.newBuilder()
                    .setApnsToken(apnsToken)
                    .build())
                .build(),
            apnsToken, null, false),

        Arguments.of(SetPushTokenRequest.newBuilder()
                .setFcmTokenRequest(SetPushTokenRequest.FcmTokenRequest.newBuilder()
                    .setFcmToken(fcmToken)
                    .build())
                .build(),
            null, fcmToken, false)
    );
  }

  @ParameterizedTest
  @MethodSource
  void setPushTokenIllegalArgument(final SetPushTokenRequest request) {
    final Device device = mock(Device.class);
    when(authenticatedAccount.getDevice(AUTHENTICATED_DEVICE_ID)).thenReturn(Optional.of(device));
    assertStatusException(Status.INVALID_ARGUMENT, () -> authenticatedServiceStub().setPushToken(request));
    verify(accountsManager, never()).updateDevice(any(), anyByte(), any());
  }

  private static Stream<Arguments> setPushTokenIllegalArgument() {
    return Stream.of(
        Arguments.of(SetPushTokenRequest.newBuilder().build()),

        Arguments.of(SetPushTokenRequest.newBuilder()
                .setApnsTokenRequest(SetPushTokenRequest.ApnsTokenRequest.newBuilder().build())
            .build()),

        Arguments.of(SetPushTokenRequest.newBuilder()
            .setFcmTokenRequest(SetPushTokenRequest.FcmTokenRequest.newBuilder().build())
            .build())
    );
  }

  @ParameterizedTest
  @MethodSource
  void clearPushToken(final byte deviceId,
      @Nullable final String apnsToken,
      @Nullable final String fcmToken,
      @Nullable final String expectedUserAgent) {

    mockAuthenticationInterceptor().setAuthenticatedDevice(AUTHENTICATED_ACI, deviceId);

    final Device device = mock(Device.class);
    when(device.getId()).thenReturn(deviceId);
    when(device.isPrimary()).thenReturn(deviceId == Device.PRIMARY_ID);
    when(device.getApnId()).thenReturn(apnsToken);
    when(device.getGcmId()).thenReturn(fcmToken);
    when(authenticatedAccount.getDevice(deviceId)).thenReturn(Optional.of(device));

    final ClearPushTokenResponse ignored = authenticatedServiceStub().clearPushToken(ClearPushTokenRequest.newBuilder().build());

    verify(device).setApnId(null);
    verify(device).setGcmId(null);
    verify(device).setFetchesMessages(true);

    if (expectedUserAgent != null) {
      verify(device).setUserAgent(expectedUserAgent);
    } else {
      verify(device, never()).setUserAgent(any());
    }
  }

  private static Stream<Arguments> clearPushToken() {
    return Stream.of(
        Arguments.of(Device.PRIMARY_ID, "apns-token", null, "OWI"),
        Arguments.of(Device.PRIMARY_ID, null, "fcm-token", "OWA"),
        Arguments.of(Device.PRIMARY_ID, null, null, null),
        Arguments.of((byte) (Device.PRIMARY_ID + 1), "apns-token", null, "OWP"),
        Arguments.of((byte) (Device.PRIMARY_ID + 1), null, "fcm-token", "OWA"),
        Arguments.of((byte) (Device.PRIMARY_ID + 1), null, null, null)
    );
  }

  @CartesianTest
  void setCapabilities(
      @CartesianTest.Values(bytes = {Device.PRIMARY_ID, Device.PRIMARY_ID + 1}) final byte deviceId,
      @CartesianTest.Values(booleans = {true, false}) final boolean storage,
      @CartesianTest.Values(booleans = {true, false}) final boolean transfer,
      @CartesianTest.Values(booleans = {true, false}) final boolean deleteSync,
      @CartesianTest.Values(booleans = {true, false}) final boolean attachmentBackfill,
      @CartesianTest.Values(booleans = {true, false}) final boolean spqr) {

    mockAuthenticationInterceptor().setAuthenticatedDevice(AUTHENTICATED_ACI, deviceId);

    final Device device = mock(Device.class);
    when(authenticatedAccount.getDevice(deviceId)).thenReturn(Optional.of(device));

    final SetCapabilitiesRequest.Builder requestBuilder = SetCapabilitiesRequest.newBuilder();

    if (storage) {
      requestBuilder.addCapabilities(org.signal.chat.common.DeviceCapability.DEVICE_CAPABILITY_STORAGE);
    }

    if (transfer) {
      requestBuilder.addCapabilities(org.signal.chat.common.DeviceCapability.DEVICE_CAPABILITY_TRANSFER);
    }

    if (deleteSync) {
      requestBuilder.addCapabilities(org.signal.chat.common.DeviceCapability.DEVICE_CAPABILITY_DELETE_SYNC);
    }

    if (attachmentBackfill) {
      requestBuilder.addCapabilities(org.signal.chat.common.DeviceCapability.DEVICE_CAPABILITY_ATTACHMENT_BACKFILL);
    }

    if (spqr) {
      requestBuilder.addCapabilities(org.signal.chat.common.DeviceCapability.DEVICE_CAPABILITY_SPARSE_POST_QUANTUM_RATCHET);
    }

    final SetCapabilitiesResponse ignored = authenticatedServiceStub().setCapabilities(requestBuilder.build());

    final Set<DeviceCapability> expectedCapabilities = new HashSet<>();

    if (storage) {
      expectedCapabilities.add(DeviceCapability.STORAGE);
    }

    if (transfer) {
      expectedCapabilities.add(DeviceCapability.TRANSFER);
    }

    if (deleteSync) {
      expectedCapabilities.add(DeviceCapability.DELETE_SYNC);
    }

    if (attachmentBackfill) {
      expectedCapabilities.add(DeviceCapability.ATTACHMENT_BACKFILL);
    }

    if (spqr) {
      expectedCapabilities.add(DeviceCapability.SPARSE_POST_QUANTUM_RATCHET);
    }

    verify(device).setCapabilities(expectedCapabilities);
  }
}
