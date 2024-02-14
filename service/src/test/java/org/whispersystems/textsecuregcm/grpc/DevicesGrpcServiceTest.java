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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.commons.lang3.RandomStringUtils;
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
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.KeysManager;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

class DevicesGrpcServiceTest extends SimpleBaseGrpcTest<DevicesGrpcService, DevicesGrpc.DevicesBlockingStub> {

  @Mock
  private AccountsManager accountsManager;

  @Mock
  private KeysManager keysManager;

  @Mock
  private MessagesManager messagesManager;

  @Mock
  private Account authenticatedAccount;


  @Override
  protected DevicesGrpcService createServiceBeforeEachTest() {
    when(authenticatedAccount.getUuid()).thenReturn(AUTHENTICATED_ACI);

    when(accountsManager.getByAccountIdentifierAsync(AUTHENTICATED_ACI))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(authenticatedAccount)));

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

    when(keysManager.deleteSingleUsePreKeys(any(), anyByte())).thenReturn(CompletableFuture.completedFuture(null));
    when(messagesManager.clear(any(), anyByte())).thenReturn(CompletableFuture.completedFuture(null));

    return new DevicesGrpcService(accountsManager, keysManager, messagesManager);
  }

  @Test
  void getDevices() {
    final Instant primaryDeviceCreated = Instant.now().minus(Duration.ofDays(7)).truncatedTo(ChronoUnit.MILLIS);
    final Instant primaryDeviceLastSeen = primaryDeviceCreated.plus(Duration.ofHours(6));
    final Instant linkedDeviceCreated = Instant.now().minus(Duration.ofDays(1)).truncatedTo(ChronoUnit.MILLIS);
    final Instant linkedDeviceLastSeen = linkedDeviceCreated.plus(Duration.ofHours(7));

    final Device primaryDevice = mock(Device.class);
    when(primaryDevice.getId()).thenReturn(Device.PRIMARY_ID);
    when(primaryDevice.getCreated()).thenReturn(primaryDeviceCreated.toEpochMilli());
    when(primaryDevice.getLastSeen()).thenReturn(primaryDeviceLastSeen.toEpochMilli());

    final String linkedDeviceName = "A linked device";

    final Device linkedDevice = mock(Device.class);
    when(linkedDevice.getId()).thenReturn((byte) (Device.PRIMARY_ID + 1));
    when(linkedDevice.getCreated()).thenReturn(linkedDeviceCreated.toEpochMilli());
    when(linkedDevice.getLastSeen()).thenReturn(linkedDeviceLastSeen.toEpochMilli());
    when(linkedDevice.getName()).thenReturn(linkedDeviceName.getBytes(StandardCharsets.UTF_8));

    when(authenticatedAccount.getDevices()).thenReturn(List.of(primaryDevice, linkedDevice));

    final GetDevicesResponse expectedResponse = GetDevicesResponse.newBuilder()
        .addDevices(GetDevicesResponse.LinkedDevice.newBuilder()
            .setId(Device.PRIMARY_ID)
            .setCreated(primaryDeviceCreated.toEpochMilli())
            .setLastSeen(primaryDeviceLastSeen.toEpochMilli())
            .build())
        .addDevices(GetDevicesResponse.LinkedDevice.newBuilder()
            .setId(Device.PRIMARY_ID + 1)
            .setCreated(linkedDeviceCreated.toEpochMilli())
            .setLastSeen(linkedDeviceLastSeen.toEpochMilli())
            .setName(ByteString.copyFrom(linkedDeviceName.getBytes(StandardCharsets.UTF_8)))
            .build())
        .build();

    assertEquals(expectedResponse, authenticatedServiceStub().getDevices(GetDevicesRequest.newBuilder().build()));
  }

  @Test
  void removeDevice() {
    final byte deviceId = 17;

    when(accountsManager.removeDevice(any(), anyByte()))
        .thenReturn(CompletableFuture.completedFuture(authenticatedAccount));

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
  void removeDeviceNonPrimaryAuthenticated() {
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
        .setName(ByteString.copyFrom(deviceName))
        .build());

    verify(device).setName(deviceName);
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
        Arguments.of(SetDeviceNameRequest.newBuilder().build()),

        // Excessively-long device name
        Arguments.of(SetDeviceNameRequest.newBuilder()
            .setName(ByteString.copyFrom(RandomStringUtils.randomAlphanumeric(1024).getBytes(StandardCharsets.UTF_8)))
            .build())
    );
  }

  @ParameterizedTest
  @MethodSource
  void setPushToken(final byte deviceId,
      final SetPushTokenRequest request,
      @Nullable final String expectedApnsToken,
      @Nullable final String expectedApnsVoipToken,
      @Nullable final String expectedFcmToken) {

    mockAuthenticationInterceptor().setAuthenticatedDevice(AUTHENTICATED_ACI, deviceId);

    final Device device = mock(Device.class);
    when(authenticatedAccount.getDevice(deviceId)).thenReturn(Optional.of(device));

    final SetPushTokenResponse ignored = authenticatedServiceStub().setPushToken(request);

    verify(device).setApnId(expectedApnsToken);
    verify(device).setVoipApnId(expectedApnsVoipToken);
    verify(device).setGcmId(expectedFcmToken);
    verify(device).setFetchesMessages(false);
  }

  private static Stream<Arguments> setPushToken() {
    final String apnsToken = "apns-token";
    final String apnsVoipToken = "apns-voip-token";
    final String fcmToken = "fcm-token";

    final Stream.Builder<Arguments> streamBuilder = Stream.builder();

    for (final byte deviceId : new byte[]{Device.PRIMARY_ID, Device.PRIMARY_ID + 1}) {
      streamBuilder.add(Arguments.of(deviceId,
          SetPushTokenRequest.newBuilder()
              .setApnsTokenRequest(SetPushTokenRequest.ApnsTokenRequest.newBuilder()
                  .setApnsToken(apnsToken)
                  .setApnsVoipToken(apnsVoipToken)
                  .build())
              .build(),
          apnsToken, apnsVoipToken, null));

      streamBuilder.add(Arguments.of(deviceId,
          SetPushTokenRequest.newBuilder()
              .setApnsTokenRequest(SetPushTokenRequest.ApnsTokenRequest.newBuilder()
                  .setApnsToken(apnsToken)
                  .build())
              .build(),
          apnsToken, null, null));

      streamBuilder.add(Arguments.of(deviceId,
          SetPushTokenRequest.newBuilder()
              .setFcmTokenRequest(SetPushTokenRequest.FcmTokenRequest.newBuilder()
                  .setFcmToken(fcmToken)
                  .build())
              .build(),
          null, null, fcmToken));
    }

    return streamBuilder.build();
  }

  @ParameterizedTest
  @MethodSource
  void setPushTokenUnchanged(final SetPushTokenRequest request,
      @Nullable final String apnsToken,
      @Nullable final String apnsVoipToken,
      @Nullable final String fcmToken) {

    final Device device = mock(Device.class);
    when(device.getApnId()).thenReturn(apnsToken);
    when(device.getVoipApnId()).thenReturn(apnsVoipToken);
    when(device.getGcmId()).thenReturn(fcmToken);

    when(authenticatedAccount.getDevice(AUTHENTICATED_DEVICE_ID)).thenReturn(Optional.of(device));

    final SetPushTokenResponse ignored = authenticatedServiceStub().setPushToken(request);

    verify(accountsManager, never()).updateDevice(any(), anyByte(), any());
  }

  private static Stream<Arguments> setPushTokenUnchanged() {
    final String apnsToken = "apns-token";
    final String apnsVoipToken = "apns-voip-token";
    final String fcmToken = "fcm-token";

    return Stream.of(
        Arguments.of(SetPushTokenRequest.newBuilder()
                .setApnsTokenRequest(SetPushTokenRequest.ApnsTokenRequest.newBuilder()
                    .setApnsToken(apnsToken)
                    .setApnsVoipToken(apnsVoipToken)
                    .build())
                .build(),
            apnsToken, apnsVoipToken, null, false),

        Arguments.of(SetPushTokenRequest.newBuilder()
                .setApnsTokenRequest(SetPushTokenRequest.ApnsTokenRequest.newBuilder()
                    .setApnsToken(apnsToken)
                    .build())
                .build(),
            apnsToken, null, null, false),

        Arguments.of(SetPushTokenRequest.newBuilder()
                .setFcmTokenRequest(SetPushTokenRequest.FcmTokenRequest.newBuilder()
                    .setFcmToken(fcmToken)
                    .build())
                .build(),
            null, null, fcmToken, false)
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
      @Nullable final String apnsVoipToken,
      @Nullable final String fcmToken,
      @Nullable final String expectedUserAgent) {

    mockAuthenticationInterceptor().setAuthenticatedDevice(AUTHENTICATED_ACI, deviceId);

    final Device device = mock(Device.class);
    when(device.getId()).thenReturn(deviceId);
    when(device.isPrimary()).thenReturn(deviceId == Device.PRIMARY_ID);
    when(device.getApnId()).thenReturn(apnsToken);
    when(device.getVoipApnId()).thenReturn(apnsVoipToken);
    when(device.getGcmId()).thenReturn(fcmToken);
    when(authenticatedAccount.getDevice(deviceId)).thenReturn(Optional.of(device));

    final ClearPushTokenResponse ignored = authenticatedServiceStub().clearPushToken(ClearPushTokenRequest.newBuilder().build());

    verify(device).setApnId(null);
    verify(device).setVoipApnId(null);
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
        Arguments.of(Device.PRIMARY_ID, "apns-token", null, null, "OWI"),
        Arguments.of(Device.PRIMARY_ID, "apns-token", "apns-voip-token", null, "OWI"),
        Arguments.of(Device.PRIMARY_ID, null, "apns-voip-token", null, "OWI"),
        Arguments.of(Device.PRIMARY_ID, null, null, "fcm-token", "OWA"),
        Arguments.of(Device.PRIMARY_ID, null, null, null, null),
        Arguments.of((byte) (Device.PRIMARY_ID + 1), "apns-token", null, null, "OWP"),
        Arguments.of((byte) (Device.PRIMARY_ID + 1), "apns-token", "apns-voip-token", null, "OWP"),
        Arguments.of((byte) (Device.PRIMARY_ID + 1), null, "apns-voip-token", null, "OWP"),
        Arguments.of((byte) (Device.PRIMARY_ID + 1), null, null, "fcm-token", "OWA"),
        Arguments.of((byte) (Device.PRIMARY_ID + 1), null, null, null, null)
    );
  }

  @CartesianTest
  void setCapabilities(
      @CartesianTest.Values(bytes = {Device.PRIMARY_ID, Device.PRIMARY_ID + 1}) final byte deviceId,
      @CartesianTest.Values(booleans = {true, false}) final boolean storage,
      @CartesianTest.Values(booleans = {true, false}) final boolean transfer,
      @CartesianTest.Values(booleans = {true, false}) final boolean paymentActivation) {

    mockAuthenticationInterceptor().setAuthenticatedDevice(AUTHENTICATED_ACI, deviceId);

    final Device device = mock(Device.class);
    when(authenticatedAccount.getDevice(deviceId)).thenReturn(Optional.of(device));

    final SetCapabilitiesResponse ignored = authenticatedServiceStub().setCapabilities(SetCapabilitiesRequest.newBuilder()
            .setStorage(storage)
            .setTransfer(transfer)
            .setPaymentActivation(paymentActivation)
        .build());

    final Device.DeviceCapabilities expectedCapabilities = new Device.DeviceCapabilities(
        storage,
        transfer,
        paymentActivation);

    verify(device).setCapabilities(expectedCapabilities);
  }
}
