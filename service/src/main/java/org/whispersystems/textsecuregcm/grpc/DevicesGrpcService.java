/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.signal.chat.device.ClearPushTokenRequest;
import org.signal.chat.device.ClearPushTokenResponse;
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
import org.signal.chat.device.SimpleDevicesGrpc;
import org.signal.chat.errors.NotFound;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticationUtil;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.DeviceCapability;

public class DevicesGrpcService extends SimpleDevicesGrpc.DevicesImplBase {

  private final AccountsManager accountsManager;

  public DevicesGrpcService(final AccountsManager accountsManager) {
    this.accountsManager = accountsManager;
  }

  @Override
  public GetDevicesResponse getDevices(final GetDevicesRequest request) {
    final Account account = getAuthenticatedAccount();

    final GetDevicesResponse.Builder responseBuilder = GetDevicesResponse.newBuilder();

    account.getDevices().stream()
        .map(device -> {
          final GetDevicesResponse.LinkedDevice.Builder linkedDeviceBuilder =
              GetDevicesResponse.LinkedDevice.newBuilder()
                  .setId(device.getId())
                  .setLastSeen(device.getLastSeen())
                  .setRegistrationId(device.getRegistrationId(IdentityType.ACI))
                  .setCreatedAtCiphertext(ByteString.copyFrom(device.getCreatedAtCiphertext()));

          if (device.getName() != null) {
            linkedDeviceBuilder.setName(ByteString.copyFrom(device.getName()));
          }

          return linkedDeviceBuilder.build();
        })
        .forEach(responseBuilder::addDevices);

    return responseBuilder.build();
  }

  @Override
  public RemoveDeviceResponse removeDevice(final RemoveDeviceRequest request) {
    if (request.getId() == Device.PRIMARY_ID) {
      throw GrpcExceptions.invalidArguments("cannot remove primary device");
    }

    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();

    if (authenticatedDevice.deviceId() != Device.PRIMARY_ID && request.getId() != authenticatedDevice.deviceId()) {
      throw GrpcExceptions.badAuthentication("linked devices cannot remove devices other than themselves");
    }

    final byte deviceId = DeviceIdUtil.validate(request.getId());

    accountsManager.removeDevice(getAuthenticatedAccount(), deviceId).join();

    return RemoveDeviceResponse.getDefaultInstance();
  }

  @Override
  public SetDeviceNameResponse setDeviceName(final SetDeviceNameRequest request) {
    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();

    final byte deviceId = DeviceIdUtil.validate(request.getId());

    final boolean mayChangeName = authenticatedDevice.deviceId() == Device.PRIMARY_ID ||
        authenticatedDevice.deviceId() == deviceId;

    if (!mayChangeName) {
      throw GrpcExceptions.badAuthentication("linked device is not authorized to change target device name");
    }

    final Account account = getAuthenticatedAccount();

    if (account.getDevice(deviceId).isEmpty()) {
      return SetDeviceNameResponse.newBuilder().setTargetDeviceNotFound(NotFound.getDefaultInstance()).build();
    }

    accountsManager.updateDevice(account, deviceId, device -> device.setName(request.getName().toByteArray()));

    return SetDeviceNameResponse.newBuilder().setSuccess(Empty.getDefaultInstance()).build();
  }

  @Override
  public SetPushTokenResponse setPushToken(final SetPushTokenRequest request) {
    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();

    @Nullable final String apnsToken;
    @Nullable final String fcmToken;

    switch (request.getTokenRequestCase()) {

      case APNS_TOKEN_REQUEST -> {
        final SetPushTokenRequest.ApnsTokenRequest apnsTokenRequest = request.getApnsTokenRequest();
        apnsToken = StringUtils.stripToNull(apnsTokenRequest.getApnsToken());
        fcmToken = null;
      }

      case FCM_TOKEN_REQUEST -> {
        final SetPushTokenRequest.FcmTokenRequest fcmTokenRequest = request.getFcmTokenRequest();
        apnsToken = null;
        fcmToken = StringUtils.stripToNull(fcmTokenRequest.getFcmToken());
      }

      default -> throw GrpcExceptions.fieldViolation("token_request", "No tokens specified");
    }

    final Account account = getAuthenticatedAccount();

    final Device device = account.getDevice(authenticatedDevice.deviceId())
        .orElseThrow(() -> GrpcExceptions.invalidCredentials("invalid credentials"));

    if (!Objects.equals(device.getApnId(), apnsToken) || !Objects.equals(device.getGcmId(), fcmToken)) {
      accountsManager.updateDevice(account, authenticatedDevice.deviceId(), d -> {
        d.setApnId(apnsToken);
        d.setGcmId(fcmToken);
        d.setFetchesMessages(false);
      });
    }

    return SetPushTokenResponse.getDefaultInstance();
  }

  @Override
  public ClearPushTokenResponse clearPushToken(final ClearPushTokenRequest request) {
    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();
    final Account account = getAuthenticatedAccount();

    accountsManager.updateDevice(account, authenticatedDevice.deviceId(), device -> {
      if (StringUtils.isNotBlank(device.getApnId())) {
        device.setUserAgent(device.isPrimary() ? "OWI" : "OWP");
      } else if (StringUtils.isNotBlank(device.getGcmId())) {
        device.setUserAgent("OWA");
      }

      device.setApnId(null);
      device.setGcmId(null);
      device.setFetchesMessages(true);
    });

    return ClearPushTokenResponse.getDefaultInstance();
  }

  @Override
  public SetCapabilitiesResponse setCapabilities(final SetCapabilitiesRequest request) {
    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();

    final Set<DeviceCapability> capabilities = request.getCapabilitiesList().stream()
        .map(DeviceCapabilityUtil::fromGrpcDeviceCapability)
        .collect(Collectors.toSet());

    accountsManager.updateDevice(getAuthenticatedAccount(), authenticatedDevice.deviceId(),
        device -> device.setCapabilities(capabilities));

    return SetCapabilitiesResponse.getDefaultInstance();
  }

  private Account getAuthenticatedAccount() {
    return accountsManager.getByAccountIdentifier(AuthenticationUtil.requireAuthenticatedDevice().accountIdentifier())
        .orElseThrow(() -> GrpcExceptions.invalidCredentials("invalid credentials"));
  }
}
