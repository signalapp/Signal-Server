/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
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
import org.whispersystems.textsecuregcm.auth.grpc.MockAuthenticationInterceptor;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.KeysManager;
import org.whispersystems.textsecuregcm.storage.MessagesManager;

class DevicesGrpcServiceTest {

  private AccountsManager accountsManager;
  private KeysManager keysManager;
  private MessagesManager messagesManager;

  private Account authenticatedAccount;

  private MockAuthenticationInterceptor mockAuthenticationInterceptor;
  private DevicesGrpc.DevicesBlockingStub devicesStub;

  @RegisterExtension
  static final GrpcServerExtension GRPC_SERVER_EXTENSION = new GrpcServerExtension();

  private static final UUID AUTHENTICATED_ACI = UUID.randomUUID();
  private static final long AUTHENTICATED_DEVICE_ID = Device.MASTER_ID;

  @BeforeEach
  void setUp() {
    accountsManager = mock(AccountsManager.class);
    keysManager = mock(KeysManager.class);
    messagesManager = mock(MessagesManager.class);

    authenticatedAccount = mock(Account.class);
    when(authenticatedAccount.getUuid()).thenReturn(AUTHENTICATED_ACI);

    mockAuthenticationInterceptor = new MockAuthenticationInterceptor();
    mockAuthenticationInterceptor.setAuthenticatedDevice(AUTHENTICATED_ACI, AUTHENTICATED_DEVICE_ID);

    when(accountsManager.getByAccountIdentifierAsync(AUTHENTICATED_ACI))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(authenticatedAccount)));

    when(accountsManager.updateAsync(any(), any()))
        .thenAnswer(invocation -> {
          final Account account = invocation.getArgument(0);
          final Consumer<Account> updater = invocation.getArgument(1);

          updater.accept(account);

          return CompletableFuture.completedFuture(account);
        });

    when(accountsManager.updateDeviceAsync(any(), anyLong(), any()))
        .thenAnswer(invocation -> {
          final Account account = invocation.getArgument(0);
          final Device device = account.getDevice(invocation.getArgument(1)).orElseThrow();
          final Consumer<Device> updater = invocation.getArgument(2);

          updater.accept(device);

          return CompletableFuture.completedFuture(account);
        });

    when(keysManager.delete(any(), anyLong())).thenReturn(CompletableFuture.completedFuture(null));
    when(messagesManager.clear(any(), anyLong())).thenReturn(CompletableFuture.completedFuture(null));

    final DevicesGrpcService devicesGrpcService = new DevicesGrpcService(accountsManager, keysManager, messagesManager);
    devicesStub = DevicesGrpc.newBlockingStub(GRPC_SERVER_EXTENSION.getChannel());

    GRPC_SERVER_EXTENSION.getServiceRegistry()
        .addService(ServerInterceptors.intercept(devicesGrpcService, mockAuthenticationInterceptor));
  }

  @Test
  void getDevices() {
    final Instant primaryDeviceCreated = Instant.now().minus(Duration.ofDays(7)).truncatedTo(ChronoUnit.MILLIS);
    final Instant primaryDeviceLastSeen = primaryDeviceCreated.plus(Duration.ofHours(6));
    final Instant linkedDeviceCreated = Instant.now().minus(Duration.ofDays(1)).truncatedTo(ChronoUnit.MILLIS);
    final Instant linkedDeviceLastSeen = linkedDeviceCreated.plus(Duration.ofHours(7));

    final Device primaryDevice = mock(Device.class);
    when(primaryDevice.getId()).thenReturn(Device.MASTER_ID);
    when(primaryDevice.getCreated()).thenReturn(primaryDeviceCreated.toEpochMilli());
    when(primaryDevice.getLastSeen()).thenReturn(primaryDeviceLastSeen.toEpochMilli());

    final String linkedDeviceName = "A linked device";

    final Device linkedDevice = mock(Device.class);
    when(linkedDevice.getId()).thenReturn(Device.MASTER_ID + 1);
    when(linkedDevice.getCreated()).thenReturn(linkedDeviceCreated.toEpochMilli());
    when(linkedDevice.getLastSeen()).thenReturn(linkedDeviceLastSeen.toEpochMilli());
    when(linkedDevice.getName())
        .thenReturn(Base64.getEncoder().encodeToString(linkedDeviceName.getBytes(StandardCharsets.UTF_8)));

    when(authenticatedAccount.getDevices()).thenReturn(List.of(primaryDevice, linkedDevice));

    final GetDevicesResponse expectedResponse = GetDevicesResponse.newBuilder()
        .addDevices(GetDevicesResponse.LinkedDevice.newBuilder()
            .setId(Device.MASTER_ID)
            .setCreated(primaryDeviceCreated.toEpochMilli())
            .setLastSeen(primaryDeviceLastSeen.toEpochMilli())
            .build())
        .addDevices(GetDevicesResponse.LinkedDevice.newBuilder()
            .setId(Device.MASTER_ID + 1)
            .setCreated(linkedDeviceCreated.toEpochMilli())
            .setLastSeen(linkedDeviceLastSeen.toEpochMilli())
            .setName(ByteString.copyFrom(linkedDeviceName.getBytes(StandardCharsets.UTF_8)))
            .build())
        .build();

    assertEquals(expectedResponse, devicesStub.getDevices(GetDevicesRequest.newBuilder().build()));
  }

  @Test
  void removeDevice() {
    final long deviceId = 17;

    final RemoveDeviceResponse ignored = devicesStub.removeDevice(RemoveDeviceRequest.newBuilder()
        .setId(deviceId)
        .build());

    verify(messagesManager, times(2)).clear(AUTHENTICATED_ACI, deviceId);
    verify(keysManager).delete(AUTHENTICATED_ACI, deviceId);
    verify(authenticatedAccount).removeDevice(deviceId);
  }

  @Test
  void removeDevicePrimary() {
    final StatusRuntimeException exception = assertThrows(StatusRuntimeException.class,
        () -> devicesStub.removeDevice(RemoveDeviceRequest.newBuilder()
            .setId(1)
            .build()));

    assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());
  }

  @Test
  void removeDeviceNonPrimaryAuthenticated() {
    mockAuthenticationInterceptor.setAuthenticatedDevice(AUTHENTICATED_ACI, Device.MASTER_ID + 1);

    final StatusRuntimeException exception = assertThrows(StatusRuntimeException.class,
        () -> devicesStub.removeDevice(RemoveDeviceRequest.newBuilder()
            .setId(17)
            .build()));

    assertEquals(Status.Code.PERMISSION_DENIED, exception.getStatus().getCode());
  }

  @ParameterizedTest
  @ValueSource(longs = {Device.MASTER_ID, Device.MASTER_ID + 1})
  void setDeviceName(final long deviceId) {
    mockAuthenticationInterceptor.setAuthenticatedDevice(AUTHENTICATED_ACI, deviceId);

    final Device device = mock(Device.class);
    when(authenticatedAccount.getDevice(deviceId)).thenReturn(Optional.of(device));

    final byte[] deviceName = new byte[128];
    ThreadLocalRandom.current().nextBytes(deviceName);

    final SetDeviceNameResponse ignored = devicesStub.setDeviceName(SetDeviceNameRequest.newBuilder()
        .setName(ByteString.copyFrom(deviceName))
        .build());

    verify(device).setName(Base64.getEncoder().encodeToString(deviceName));
  }

  @ParameterizedTest
  @MethodSource
  void setDeviceNameIllegalArgument(final SetDeviceNameRequest request) {
    when(authenticatedAccount.getDevice(AUTHENTICATED_DEVICE_ID)).thenReturn(Optional.of(mock(Device.class)));

    final StatusRuntimeException exception =
        assertThrows(StatusRuntimeException.class, () -> devicesStub.setDeviceName(request));

    assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());
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
  void setPushToken(final long deviceId,
      final SetPushTokenRequest request,
      @Nullable final String expectedApnsToken,
      @Nullable final String expectedApnsVoipToken,
      @Nullable final String expectedFcmToken) {

    mockAuthenticationInterceptor.setAuthenticatedDevice(AUTHENTICATED_ACI, deviceId);

    final Device device = mock(Device.class);
    when(authenticatedAccount.getDevice(deviceId)).thenReturn(Optional.of(device));

    final SetPushTokenResponse ignored = devicesStub.setPushToken(request);

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

    for (final long deviceId : new long[] { Device.MASTER_ID, Device.MASTER_ID + 1 }) {
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

    final SetPushTokenResponse ignored = devicesStub.setPushToken(request);

    verify(accountsManager, never()).updateDevice(any(), anyLong(), any());
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

    final StatusRuntimeException exception =
        assertThrows(StatusRuntimeException.class, () -> devicesStub.setPushToken(request));

    assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());

    verify(accountsManager, never()).updateDevice(any(), anyLong(), any());
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
  void clearPushToken(final long deviceId,
      @Nullable final String apnsToken,
      @Nullable final String apnsVoipToken,
      @Nullable final String fcmToken,
      @Nullable final String expectedUserAgent) {

    mockAuthenticationInterceptor.setAuthenticatedDevice(AUTHENTICATED_ACI, deviceId);

    final Device device = mock(Device.class);
    when(device.getId()).thenReturn(deviceId);
    when(device.isMaster()).thenReturn(deviceId == Device.MASTER_ID);
    when(device.getApnId()).thenReturn(apnsToken);
    when(device.getVoipApnId()).thenReturn(apnsVoipToken);
    when(device.getGcmId()).thenReturn(fcmToken);
    when(authenticatedAccount.getDevice(deviceId)).thenReturn(Optional.of(device));

    final ClearPushTokenResponse ignored = devicesStub.clearPushToken(ClearPushTokenRequest.newBuilder().build());

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
        Arguments.of(Device.MASTER_ID, "apns-token", null, null, "OWI"),
        Arguments.of(Device.MASTER_ID, "apns-token", "apns-voip-token", null, "OWI"),
        Arguments.of(Device.MASTER_ID, null, "apns-voip-token", null, "OWI"),
        Arguments.of(Device.MASTER_ID, null, null, "fcm-token", "OWA"),
        Arguments.of(Device.MASTER_ID, null, null, null, null),
        Arguments.of(Device.MASTER_ID + 1, "apns-token", null, null, "OWP"),
        Arguments.of(Device.MASTER_ID + 1, "apns-token", "apns-voip-token", null, "OWP"),
        Arguments.of(Device.MASTER_ID + 1, null, "apns-voip-token", null, "OWP"),
        Arguments.of(Device.MASTER_ID + 1, null, null, "fcm-token", "OWA"),
        Arguments.of(Device.MASTER_ID + 1, null, null, null, null)
    );
  }

  @CartesianTest
  void setCapabilities(
      @CartesianTest.Values(longs = {Device.MASTER_ID, Device.MASTER_ID + 1}) final long deviceId,
      @CartesianTest.Values(booleans = {true, false}) final boolean storage,
      @CartesianTest.Values(booleans = {true, false}) final boolean transfer,
      @CartesianTest.Values(booleans = {true, false}) final boolean pni,
      @CartesianTest.Values(booleans = {true, false}) final boolean paymentActivation) {

    mockAuthenticationInterceptor.setAuthenticatedDevice(AUTHENTICATED_ACI, deviceId);

    final Device device = mock(Device.class);
    when(authenticatedAccount.getDevice(deviceId)).thenReturn(Optional.of(device));

    final SetCapabilitiesResponse ignored = devicesStub.setCapabilities(SetCapabilitiesRequest.newBuilder()
            .setStorage(storage)
            .setTransfer(transfer)
            .setPni(pni)
            .setPaymentActivation(paymentActivation)
        .build());

    final Device.DeviceCapabilities expectedCapabilities = new Device.DeviceCapabilities(
        storage,
        transfer,
        pni,
        paymentActivation);

    verify(device).setCapabilities(expectedCapabilities);
  }
}
