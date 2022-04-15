/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.util;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.controllers.MismatchedDevicesException;
import org.whispersystems.textsecuregcm.controllers.StaleDevicesException;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.MessageValidation;
import org.whispersystems.textsecuregcm.util.Pair;

@ExtendWith(DropwizardExtensionsSupport.class)
class MessageValidationTest {

  static Account mockAccountWithDeviceAndRegId(Object... deviceAndRegistrationIds) {
    Account account = mock(Account.class);
    if (deviceAndRegistrationIds.length % 2 != 0) {
      throw new IllegalArgumentException("invalid number of arguments specified; must be even");
    }
    for (int i = 0; i < deviceAndRegistrationIds.length; i+=2) {
      if (!(deviceAndRegistrationIds[i] instanceof Long)) {
        throw new IllegalArgumentException("device id is not instance of long at index " + i);
      }
      if (!(deviceAndRegistrationIds[i + 1] instanceof Integer)) {
        throw new IllegalArgumentException("registration id is not instance of integer at index " + (i + 1));
      }
      Long deviceId = (Long) deviceAndRegistrationIds[i];
      Integer registrationId = (Integer) deviceAndRegistrationIds[i + 1];
      Device device = mock(Device.class);
      when(device.getRegistrationId()).thenReturn(registrationId);
      when(account.getDevice(deviceId)).thenReturn(Optional.of(device));
    }
    return account;
  }

  static Collection<Pair<Long, Integer>> deviceAndRegistrationIds(Object... deviceAndRegistrationIds) {
    final Collection<Pair<Long, Integer>> result = new HashSet<>(deviceAndRegistrationIds.length);
    if (deviceAndRegistrationIds.length % 2 != 0) {
      throw new IllegalArgumentException("invalid number of arguments specified; must be even");
    }
    for (int i = 0; i < deviceAndRegistrationIds.length; i += 2) {
      if (!(deviceAndRegistrationIds[i] instanceof Long)) {
        throw new IllegalArgumentException("device id is not instance of long at index " + i);
      }
      if (!(deviceAndRegistrationIds[i + 1] instanceof Integer)) {
        throw new IllegalArgumentException("registration id is not instance of integer at index " + (i + 1));
      }
      Long deviceId = (Long) deviceAndRegistrationIds[i];
      Integer registrationId = (Integer) deviceAndRegistrationIds[i + 1];
      result.add(new Pair<>(deviceId, registrationId));
    }
    return result;
  }

  static Stream<Arguments> validateRegistrationIdsSource() {
    return Stream.of(
        arguments(
            mockAccountWithDeviceAndRegId(1L, 0xFFFF, 2L, 0xDEAD, 3L, 0xBEEF),
            deviceAndRegistrationIds(1L, 0xFFFF, 2L, 0xDEAD, 3L, 0xBEEF),
            null),
        arguments(
            mockAccountWithDeviceAndRegId(1L, 42),
            deviceAndRegistrationIds(1L, 1492),
            Set.of(1L)),
        arguments(
            mockAccountWithDeviceAndRegId(1L, 42),
            deviceAndRegistrationIds(1L, 42),
            null),
        arguments(
            mockAccountWithDeviceAndRegId(1L, 42),
            deviceAndRegistrationIds(1L, 0),
            null),
        arguments(
            mockAccountWithDeviceAndRegId(1L, 42, 2L, 255),
            deviceAndRegistrationIds(1L, 0, 2L, 42),
            Set.of(2L)),
        arguments(
            mockAccountWithDeviceAndRegId(1L, 42, 2L, 256),
            deviceAndRegistrationIds(1L, 41, 2L, 257),
            Set.of(1L, 2L))
    );
  }

  @ParameterizedTest
  @MethodSource("validateRegistrationIdsSource")
  void testValidateRegistrationIds(
      Account account,
      Collection<Pair<Long, Integer>> deviceAndRegistrationIds,
      Set<Long> expectedStaleDeviceIds) throws Exception {
    if (expectedStaleDeviceIds != null) {
      Assertions.assertThat(assertThrows(StaleDevicesException.class, () -> {
        MessageValidation.validateRegistrationIds(account, deviceAndRegistrationIds.stream());
      }).getStaleDevices()).hasSameElementsAs(expectedStaleDeviceIds);
    } else {
      MessageValidation.validateRegistrationIds(account, deviceAndRegistrationIds.stream());
    }
  }

  static Account mockAccountWithDeviceAndEnabled(Object... deviceIdAndEnabled) {
    Account account = mock(Account.class);
    if (deviceIdAndEnabled.length % 2 != 0) {
      throw new IllegalArgumentException("invalid number of arguments specified; must be even");
    }
    final Set<Device> devices = new HashSet<>(deviceIdAndEnabled.length / 2);
    for (int i = 0; i < deviceIdAndEnabled.length; i+=2) {
      if (!(deviceIdAndEnabled[i] instanceof Long)) {
        throw new IllegalArgumentException("device id is not instance of long at index " + i);
      }
      if (!(deviceIdAndEnabled[i + 1] instanceof Boolean)) {
        throw new IllegalArgumentException("enabled is not instance of boolean at index " + (i + 1));
      }
      Long deviceId = (Long) deviceIdAndEnabled[i];
      Boolean enabled = (Boolean) deviceIdAndEnabled[i + 1];
      Device device = mock(Device.class);
      when(device.isEnabled()).thenReturn(enabled);
      when(device.getId()).thenReturn(deviceId);
      when(account.getDevice(deviceId)).thenReturn(Optional.of(device));
      devices.add(device);
    }
    when(account.getDevices()).thenReturn(devices);
    return account;
  }

  static Stream<Arguments> validateCompleteDeviceListSource() {
    return Stream.of(
        arguments(
            mockAccountWithDeviceAndEnabled(1L, true, 2L, false, 3L, true),
            Set.of(1L, 3L),
            null,
            null,
            false,
            null),
        arguments(
            mockAccountWithDeviceAndEnabled(1L, true, 2L, false, 3L, true),
            Set.of(1L, 2L, 3L),
            null,
            Set.of(2L),
            false,
            null),
        arguments(
            mockAccountWithDeviceAndEnabled(1L, true, 2L, false, 3L, true),
            Set.of(1L),
            Set.of(3L),
            null,
            false,
            null),
        arguments(
            mockAccountWithDeviceAndEnabled(1L, true, 2L, false, 3L, true),
            Set.of(1L, 2L),
            Set.of(3L),
            Set.of(2L),
            false,
            null),
        arguments(
            mockAccountWithDeviceAndEnabled(1L, true, 2L, false, 3L, true),
            Set.of(1L),
            Set.of(3L),
            Set.of(1L),
            true,
            1L
        ),
        arguments(
            mockAccountWithDeviceAndEnabled(1L, true, 2L, false, 3L, true),
            Set.of(2L),
            Set.of(3L),
            Set.of(2L),
            true,
            1L
        ),
        arguments(
            mockAccountWithDeviceAndEnabled(1L, true, 2L, false, 3L, true),
            Set.of(3L),
            null,
            null,
            true,
            1L
        )
    );
  }

  @ParameterizedTest
  @MethodSource("validateCompleteDeviceListSource")
  void testValidateCompleteDeviceList(
      Account account,
      Set<Long> deviceIds,
      Collection<Long> expectedMissingDeviceIds,
      Collection<Long> expectedExtraDeviceIds,
      boolean isSyncMessage,
      Long authenticatedDeviceId) throws Exception {
    if (expectedMissingDeviceIds != null || expectedExtraDeviceIds != null) {
      final MismatchedDevicesException mismatchedDevicesException = assertThrows(MismatchedDevicesException.class,
          () -> MessageValidation.validateCompleteDeviceList(account, deviceIds, isSyncMessage,
              Optional.ofNullable(authenticatedDeviceId)));
      if (expectedMissingDeviceIds != null) {
        Assertions.assertThat(mismatchedDevicesException.getMissingDevices())
            .hasSameElementsAs(expectedMissingDeviceIds);
      }
      if (expectedExtraDeviceIds != null) {
        Assertions.assertThat(mismatchedDevicesException.getExtraDevices()).hasSameElementsAs(expectedExtraDeviceIds);
      }
    } else {
      MessageValidation.validateCompleteDeviceList(account, deviceIds, isSyncMessage,
          Optional.ofNullable(authenticatedDeviceId));
    }
  }
}
