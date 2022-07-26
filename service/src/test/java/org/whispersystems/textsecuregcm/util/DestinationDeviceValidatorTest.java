/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.controllers.MismatchedDevicesException;
import org.whispersystems.textsecuregcm.controllers.StaleDevicesException;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;

@ExtendWith(DropwizardExtensionsSupport.class)
class DestinationDeviceValidatorTest {

  static Account mockAccountWithDeviceAndRegId(final Map<Long, Integer> registrationIdsByDeviceId) {
    final Account account = mock(Account.class);

    registrationIdsByDeviceId.forEach((deviceId, registrationId) -> {
      final Device device = mock(Device.class);
      when(device.getRegistrationId()).thenReturn(registrationId);
      when(account.getDevice(deviceId)).thenReturn(Optional.of(device));
    });

    return account;
  }

  static Stream<Arguments> validateRegistrationIdsSource() {
    return Stream.of(
        arguments(
            mockAccountWithDeviceAndRegId(Map.of(1L, 0xFFFF, 2L, 0xDEAD, 3L, 0xBEEF)),
            Map.of(1L, 0xFFFF, 2L, 0xDEAD, 3L, 0xBEEF),
            null),
        arguments(
            mockAccountWithDeviceAndRegId(Map.of(1L, 42)),
            Map.of(1L, 1492),
            Set.of(1L)),
        arguments(
            mockAccountWithDeviceAndRegId(Map.of(1L, 42)),
            Map.of(1L, 42),
            null),
        arguments(
            mockAccountWithDeviceAndRegId(Map.of(1L, 42)),
            Map.of(1L, 0),
            null),
        arguments(
            mockAccountWithDeviceAndRegId(Map.of(1L, 42, 2L, 255)),
            Map.of(1L, 0, 2L, 42),
            Set.of(2L)),
        arguments(
            mockAccountWithDeviceAndRegId(Map.of(1L, 42, 2L, 256)),
            Map.of(1L, 41, 2L, 257),
            Set.of(1L, 2L))
    );
  }

  @ParameterizedTest
  @MethodSource("validateRegistrationIdsSource")
  void testValidateRegistrationIds(
      Account account,
      Map<Long, Integer> registrationIdsByDeviceId,
      Set<Long> expectedStaleDeviceIds) throws Exception {
    if (expectedStaleDeviceIds != null) {
      Assertions.assertThat(assertThrows(StaleDevicesException.class,
          () -> DestinationDeviceValidator.validateRegistrationIds(account, registrationIdsByDeviceId, false)).getStaleDevices())
          .hasSameElementsAs(expectedStaleDeviceIds);
    } else {
      DestinationDeviceValidator.validateRegistrationIds(account, registrationIdsByDeviceId, false);
    }
  }

  static Account mockAccountWithDeviceAndEnabled(final Map<Long, Boolean> enabledStateByDeviceId) {
    final Account account = mock(Account.class);
    final List<Device> devices = new ArrayList<>();

    enabledStateByDeviceId.forEach((deviceId, enabled) -> {
      final Device device = mock(Device.class);
      when(device.isEnabled()).thenReturn(enabled);
      when(device.getId()).thenReturn(deviceId);
      when(account.getDevice(deviceId)).thenReturn(Optional.of(device));

      devices.add(device);
    });

    when(account.getDevices()).thenReturn(devices);

    return account;
  }

  static Stream<Arguments> validateCompleteDeviceListSource() {
    return Stream.of(
        arguments(
            mockAccountWithDeviceAndEnabled(Map.of(1L, true, 2L, false, 3L, true)),
            Set.of(1L, 3L),
            null,
            null,
            Collections.emptySet()),
        arguments(
            mockAccountWithDeviceAndEnabled(Map.of(1L, true, 2L, false, 3L, true)),
            Set.of(1L, 2L, 3L),
            null,
            Set.of(2L),
            Collections.emptySet()),
        arguments(
            mockAccountWithDeviceAndEnabled(Map.of(1L, true, 2L, false, 3L, true)),
            Set.of(1L),
            Set.of(3L),
            null,
            Collections.emptySet()),
        arguments(
            mockAccountWithDeviceAndEnabled(Map.of(1L, true, 2L, false, 3L, true)),
            Set.of(1L, 2L),
            Set.of(3L),
            Set.of(2L),
            Collections.emptySet()),
        arguments(
            mockAccountWithDeviceAndEnabled(Map.of(1L, true, 2L, false, 3L, true)),
            Set.of(1L),
            Set.of(3L),
            Set.of(1L),
            Set.of(1L)
        ),
        arguments(
            mockAccountWithDeviceAndEnabled(Map.of(1L, true, 2L, false, 3L, true)),
            Set.of(2L),
            Set.of(3L),
            Set.of(2L),
            Set.of(1L)
        ),
        arguments(
            mockAccountWithDeviceAndEnabled(Map.of(1L, true, 2L, false, 3L, true)),
            Set.of(3L),
            null,
            null,
            Set.of(1L)
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
      Set<Long> excludedDeviceIds) throws Exception {

    if (expectedMissingDeviceIds != null || expectedExtraDeviceIds != null) {
      final MismatchedDevicesException mismatchedDevicesException = assertThrows(MismatchedDevicesException.class,
          () -> DestinationDeviceValidator.validateCompleteDeviceList(account, deviceIds, excludedDeviceIds));
      if (expectedMissingDeviceIds != null) {
        Assertions.assertThat(mismatchedDevicesException.getMissingDevices())
            .hasSameElementsAs(expectedMissingDeviceIds);
      }
      if (expectedExtraDeviceIds != null) {
        Assertions.assertThat(mismatchedDevicesException.getExtraDevices()).hasSameElementsAs(expectedExtraDeviceIds);
      }
    } else {
      DestinationDeviceValidator.validateCompleteDeviceList(account, deviceIds, excludedDeviceIds);
    }
  }

  @Test
  void testValidatePniRegistrationIds() {
    final Device device = mock(Device.class);
    when(device.getId()).thenReturn(Device.MASTER_ID);

    final Account account = mock(Account.class);
    when(account.getDevices()).thenReturn(List.of(device));
    when(account.getDevice(Device.MASTER_ID)).thenReturn(Optional.of(device));

    final int aciRegistrationId = 17;
    final int pniRegistrationId = 89;
    final int incorrectRegistrationId = aciRegistrationId + pniRegistrationId;

    when(device.getRegistrationId()).thenReturn(aciRegistrationId);
    when(device.getPhoneNumberIdentityRegistrationId()).thenReturn(OptionalInt.of(pniRegistrationId));

    assertDoesNotThrow(() -> DestinationDeviceValidator.validateRegistrationIds(account, Map.of(Device.MASTER_ID, aciRegistrationId), false));
    assertDoesNotThrow(() -> DestinationDeviceValidator.validateRegistrationIds(account, Map.of(Device.MASTER_ID, pniRegistrationId), true));
    assertThrows(StaleDevicesException.class, () -> DestinationDeviceValidator.validateRegistrationIds(account, Map.of(Device.MASTER_ID, aciRegistrationId), true));
    assertThrows(StaleDevicesException.class, () -> DestinationDeviceValidator.validateRegistrationIds(account, Map.of(Device.MASTER_ID, pniRegistrationId), false));

    when(device.getPhoneNumberIdentityRegistrationId()).thenReturn(OptionalInt.empty());

    assertDoesNotThrow(() -> DestinationDeviceValidator.validateRegistrationIds(account, Map.of(Device.MASTER_ID, aciRegistrationId), false));
    assertDoesNotThrow(() -> DestinationDeviceValidator.validateRegistrationIds(account, Map.of(Device.MASTER_ID, aciRegistrationId), true));
    assertThrows(StaleDevicesException.class, () -> DestinationDeviceValidator.validateRegistrationIds(account, Map.of(Device.MASTER_ID, incorrectRegistrationId), true));
    assertThrows(StaleDevicesException.class, () -> DestinationDeviceValidator.validateRegistrationIds(account, Map.of(Device.MASTER_ID, incorrectRegistrationId), false));
  }
}
