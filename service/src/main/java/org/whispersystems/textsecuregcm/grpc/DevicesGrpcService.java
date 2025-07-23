/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.signal.chat.device.ClearPushTokenRequest;
import org.signal.chat.device.ClearPushTokenResponse;
import org.signal.chat.device.GetDevicesRequest;
import org.signal.chat.device.GetDevicesResponse;
import org.signal.chat.device.ReactorDevicesGrpc;
import org.signal.chat.device.RemoveDeviceRequest;
import org.signal.chat.device.RemoveDeviceResponse;
import org.signal.chat.device.SetCapabilitiesRequest;
import org.signal.chat.device.SetCapabilitiesResponse;
import org.signal.chat.device.SetDeviceNameRequest;
import org.signal.chat.device.SetDeviceNameResponse;
import org.signal.chat.device.SetPushTokenRequest;
import org.signal.chat.device.SetPushTokenResponse;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticationUtil;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.DeviceCapability;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class DevicesGrpcService extends ReactorDevicesGrpc.DevicesImplBase {

  private final AccountsManager accountsManager;

  private static final int MAX_NAME_LENGTH = 256;

  public DevicesGrpcService(final AccountsManager accountsManager) {
    this.accountsManager = accountsManager;
  }

  @Override
  public Mono<GetDevicesResponse> getDevices(final GetDevicesRequest request) {
    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();

    return Mono.fromFuture(() -> accountsManager.getByAccountIdentifierAsync(authenticatedDevice.accountIdentifier()))
        .map(maybeAccount -> maybeAccount.orElseThrow(Status.UNAUTHENTICATED::asRuntimeException))
        .flatMapMany(account -> Flux.fromIterable(account.getDevices()))
        .reduce(GetDevicesResponse.newBuilder(), (builder, device) -> {
          final GetDevicesResponse.LinkedDevice.Builder linkedDeviceBuilder = GetDevicesResponse.LinkedDevice.newBuilder();

          if (device.getName() != null) {
            linkedDeviceBuilder.setName(ByteString.copyFrom(device.getName()));
          }

          return builder.addDevices(linkedDeviceBuilder
              .setId(device.getId())
              .setCreated(device.getCreated())
              .setLastSeen(device.getLastSeen())
              .setRegistrationId(device.getRegistrationId(IdentityType.ACI))
              .setCreatedAtCiphertext(ByteString.copyFrom(device.getCreatedAtCiphertext()))
              .build());
        })
        .map(GetDevicesResponse.Builder::build);
  }

  @Override
  public Mono<RemoveDeviceResponse> removeDevice(final RemoveDeviceRequest request) {
    if (request.getId() == Device.PRIMARY_ID) {
      throw Status.INVALID_ARGUMENT.withDescription("Cannot remove primary device").asRuntimeException();
    }

    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();

    if (authenticatedDevice.deviceId() != Device.PRIMARY_ID && request.getId() != authenticatedDevice.deviceId()) {
      throw Status.PERMISSION_DENIED
          .withDescription("Linked devices cannot remove devices other than themselves")
          .asRuntimeException();
    }

    final byte deviceId = DeviceIdUtil.validate(request.getId());

    return Mono.fromFuture(() -> accountsManager.getByAccountIdentifierAsync(authenticatedDevice.accountIdentifier()))
        .map(maybeAccount -> maybeAccount.orElseThrow(Status.UNAUTHENTICATED::asRuntimeException))
        .flatMap(account -> Mono.fromFuture(accountsManager.removeDevice(account, deviceId)))
        .thenReturn(RemoveDeviceResponse.newBuilder().build());
  }

  @Override
  public Mono<SetDeviceNameResponse> setDeviceName(final SetDeviceNameRequest request) {
    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();

    final byte deviceId = DeviceIdUtil.validate(request.getId());

    final boolean mayChangeName = authenticatedDevice.deviceId() == Device.PRIMARY_ID ||
        authenticatedDevice.deviceId() == deviceId;

    if (!mayChangeName) {
      throw Status.PERMISSION_DENIED
          .withDescription("Authenticated device is not authorized to change target device name")
          .asRuntimeException();
    }

    if (request.getName().isEmpty()) {
      throw Status.INVALID_ARGUMENT.withDescription("Must specify a device name").asRuntimeException();
    }

    if (request.getName().size() > MAX_NAME_LENGTH) {
      throw Status.INVALID_ARGUMENT.withDescription("Device name must be at most " + MAX_NAME_LENGTH + " bytes")
          .asRuntimeException();
    }

    return Mono.fromFuture(() -> accountsManager.getByAccountIdentifierAsync(authenticatedDevice.accountIdentifier()))
        .map(maybeAccount -> maybeAccount.orElseThrow(Status.UNAUTHENTICATED::asRuntimeException))
        .doOnNext(account -> {
          if (account.getDevice(deviceId).isEmpty()) {
            throw Status.NOT_FOUND.withDescription("No device found with given ID").asRuntimeException();
          }
        })
        .flatMap(account -> Mono.fromFuture(() -> accountsManager.updateDeviceAsync(account, deviceId,
            device -> device.setName(request.getName().toByteArray()))))
        .thenReturn(SetDeviceNameResponse.newBuilder().build());
  }

  @Override
  public Mono<SetPushTokenResponse> setPushToken(final SetPushTokenRequest request) {
    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();

    @Nullable final String apnsToken;
    @Nullable final String fcmToken;

    switch (request.getTokenRequestCase()) {

      case APNS_TOKEN_REQUEST -> {
        final SetPushTokenRequest.ApnsTokenRequest apnsTokenRequest = request.getApnsTokenRequest();

        if (StringUtils.isBlank(apnsTokenRequest.getApnsToken())) {
          throw Status.INVALID_ARGUMENT.withDescription("APNs token must not be blank").asRuntimeException();
        }

        apnsToken = StringUtils.stripToNull(apnsTokenRequest.getApnsToken());
        fcmToken = null;
      }

      case FCM_TOKEN_REQUEST -> {
        final SetPushTokenRequest.FcmTokenRequest fcmTokenRequest = request.getFcmTokenRequest();

        if (StringUtils.isBlank(fcmTokenRequest.getFcmToken())) {
          throw Status.INVALID_ARGUMENT.withDescription("FCM token must not be blank").asRuntimeException();
        }

        apnsToken = null;
        fcmToken = StringUtils.stripToNull(fcmTokenRequest.getFcmToken());
      }

      default -> throw Status.INVALID_ARGUMENT.withDescription("No tokens specified").asRuntimeException();
    }

    return Mono.fromFuture(() -> accountsManager.getByAccountIdentifierAsync(authenticatedDevice.accountIdentifier()))
        .map(maybeAccount -> maybeAccount.orElseThrow(Status.UNAUTHENTICATED::asRuntimeException))
        .flatMap(account -> {
          final Device device = account.getDevice(authenticatedDevice.deviceId())
              .orElseThrow(Status.UNAUTHENTICATED::asRuntimeException);

          final boolean tokenUnchanged =
              Objects.equals(device.getApnId(), apnsToken) &&
                  Objects.equals(device.getGcmId(), fcmToken);

          return tokenUnchanged
              ? Mono.empty()
              : Mono.fromFuture(() -> accountsManager.updateDeviceAsync(account, authenticatedDevice.deviceId(), d -> {
                d.setApnId(apnsToken);
                d.setGcmId(fcmToken);
                d.setFetchesMessages(false);
              }));
        })
        .thenReturn(SetPushTokenResponse.newBuilder().build());
  }

  @Override
  public Mono<ClearPushTokenResponse> clearPushToken(final ClearPushTokenRequest request) {
    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();

    return Mono.fromFuture(() -> accountsManager.getByAccountIdentifierAsync(authenticatedDevice.accountIdentifier()))
        .map(maybeAccount -> maybeAccount.orElseThrow(Status.UNAUTHENTICATED::asRuntimeException))
        .flatMap(account -> Mono.fromFuture(() -> accountsManager.updateDeviceAsync(account, authenticatedDevice.deviceId(), device -> {
          if (StringUtils.isNotBlank(device.getApnId())) {
            device.setUserAgent(device.isPrimary() ? "OWI" : "OWP");
          } else if (StringUtils.isNotBlank(device.getGcmId())) {
            device.setUserAgent("OWA");
          }

          device.setApnId(null);
          device.setGcmId(null);
          device.setFetchesMessages(true);
        })))
        .thenReturn(ClearPushTokenResponse.newBuilder().build());
  }

  @Override
  public Mono<SetCapabilitiesResponse> setCapabilities(final SetCapabilitiesRequest request) {
    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();

    final Set<DeviceCapability> capabilities = request.getCapabilitiesList().stream()
        .map(DeviceCapabilityUtil::fromGrpcDeviceCapability)
        .collect(Collectors.toSet());

    return Mono.fromFuture(() -> accountsManager.getByAccountIdentifierAsync(authenticatedDevice.accountIdentifier()))
        .map(maybeAccount -> maybeAccount.orElseThrow(Status.UNAUTHENTICATED::asRuntimeException))
        .flatMap(account ->
            Mono.fromFuture(() -> accountsManager.updateDeviceAsync(account, authenticatedDevice.deviceId(),
                d -> d.setCapabilities(capabilities))))
        .thenReturn(SetCapabilitiesResponse.newBuilder().build());
  }
}
