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

  static Account mockAccountWithDeviceAndRegId(final Map<Byte, Integer> registrationIdsByDeviceId) {
    final Account account = mock(Account.class);

    registrationIdsByDeviceId.forEach((deviceId, registrationId) -> {
      final Device device = mock(Device.class);
      when(device.getRegistrationId()).thenReturn(registrationId);
      when(account.getDevice(deviceId)).thenReturn(Optional.of(device));
    });

    return account;
  }

  static Stream<Arguments> validateRegistrationIdsSource() {
    final byte id1 = 1;
    final byte id2 = 2;
    final byte id3 = 3;
    return Stream.of(
        arguments(
            mockAccountWithDeviceAndRegId(Map.of(id1, 0xFFFF, id2, 0xDEAD, id3, 0xBEEF)),
            Map.of(id1, 0xFFFF, id2, 0xDEAD, id3, 0xBEEF),
            null),
        arguments(
            mockAccountWithDeviceAndRegId(Map.of(id1, 42)),
            Map.of(id1, 1492),
            Set.of(id1)),
        arguments(
            mockAccountWithDeviceAndRegId(Map.of(id1, 42)),
            Map.of(id1, 42),
            null),
        arguments(
            mockAccountWithDeviceAndRegId(Map.of(id1, 42)),
            Map.of(id1, 0),
            null),
        arguments(
            mockAccountWithDeviceAndRegId(Map.of(id1, 42, id2, 255)),
            Map.of(id1, 0, id2, 42),
            Set.of(id2)),
        arguments(
            mockAccountWithDeviceAndRegId(Map.of(id1, 42, id2, 256)),
            Map.of(id1, 41, id2, 257),
            Set.of(id1, id2))
    );
  }

  @ParameterizedTest
  @MethodSource("validateRegistrationIdsSource")
  void testValidateRegistrationIds(
      Account account,
      Map<Byte, Integer> registrationIdsByDeviceId,
      Set<Byte> expectedStaleDeviceIds) throws Exception {
    if (expectedStaleDeviceIds != null) {
      Assertions.assertThat(assertThrows(StaleDevicesException.class,
              () -> DestinationDeviceValidator.validateRegistrationIds(
                  account,
                  registrationIdsByDeviceId.entrySet(),
                  Map.Entry::getKey,
                  Map.Entry::getValue,
                  false))
              .getStaleDevices())
          .hasSameElementsAs(expectedStaleDeviceIds);
    } else {
      DestinationDeviceValidator.validateRegistrationIds(account, registrationIdsByDeviceId.entrySet(),
          Map.Entry::getKey, Map.Entry::getValue, false);
    }
  }

  static Account mockAccountWithDeviceAndEnabled(final Map<Byte, Boolean> enabledStateByDeviceId) {
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
    final byte id1 = 1;
    final byte id2 = 2;
    final byte id3 = 3;
    return Stream.of(
        arguments(
            mockAccountWithDeviceAndEnabled(Map.of(id1, true, id2, false, id3, true)),
            Set.of(id1, id3),
            null,
            null,
            Collections.emptySet()),
        arguments(
            mockAccountWithDeviceAndEnabled(Map.of(id1, true, id2, false, id3, true)),
            Set.of(id1, id2, id3),
            null,
            Set.of(id2),
            Collections.emptySet()),
        arguments(
            mockAccountWithDeviceAndEnabled(Map.of(id1, true, id2, false, id3, true)),
            Set.of(id1),
            Set.of(id3),
            null,
            Collections.emptySet()),
        arguments(
            mockAccountWithDeviceAndEnabled(Map.of(id1, true, id2, false, id3, true)),
            Set.of(id1, id2),
            Set.of(id3),
            Set.of(id2),
            Collections.emptySet()),
        arguments(
            mockAccountWithDeviceAndEnabled(Map.of(id1, true, id2, false, id3, true)),
            Set.of(id1),
            Set.of(id3),
            Set.of(id1),
            Set.of(id1)
        ),
        arguments(
            mockAccountWithDeviceAndEnabled(Map.of(id1, true, id2, false, id3, true)),
            Set.of(id2),
            Set.of(id3),
            Set.of(id2),
            Set.of(id1)
        ),
        arguments(
            mockAccountWithDeviceAndEnabled(Map.of(id1, true, id2, false, id3, true)),
            Set.of(id3),
            null,
            null,
            Set.of(id1)
        )
    );
  }

  @ParameterizedTest
  @MethodSource("validateCompleteDeviceListSource")
  void testValidateCompleteDeviceList(
      Account account,
      Set<Byte> deviceIds,
      Collection<Byte> expectedMissingDeviceIds,
      Collection<Byte> expectedExtraDeviceIds,
      Set<Byte> excludedDeviceIds) throws Exception {

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
  void testDuplicateDeviceIds() {
    final Account account = mockAccountWithDeviceAndRegId(Map.of(Device.PRIMARY_ID, 17));
    try {
      DestinationDeviceValidator.validateRegistrationIds(account,
          Stream.of(new Pair<>(Device.PRIMARY_ID, 16), new Pair<>(Device.PRIMARY_ID, 17)), false);
      Assertions.fail("duplicate devices should throw StaleDevicesException");
    } catch (StaleDevicesException e) {
      Assertions.assertThat(e.getStaleDevices()).hasSameElementsAs(Collections.singletonList(Device.PRIMARY_ID));
    }
  }

  @Test
  void testValidatePniRegistrationIds() {
    final Device device = mock(Device.class);
    when(device.getId()).thenReturn(Device.PRIMARY_ID);

    final Account account = mock(Account.class);
    when(account.getDevices()).thenReturn(List.of(device));
    when(account.getDevice(Device.PRIMARY_ID)).thenReturn(Optional.of(device));

    final int aciRegistrationId = 17;
    final int pniRegistrationId = 89;
    final int incorrectRegistrationId = aciRegistrationId + pniRegistrationId;

    when(device.getRegistrationId()).thenReturn(aciRegistrationId);
    when(device.getPhoneNumberIdentityRegistrationId()).thenReturn(OptionalInt.of(pniRegistrationId));

    assertDoesNotThrow(
        () -> DestinationDeviceValidator.validateRegistrationIds(account,
            Stream.of(new Pair<>(Device.PRIMARY_ID, aciRegistrationId)), false));
    assertDoesNotThrow(
        () -> DestinationDeviceValidator.validateRegistrationIds(account,
            Stream.of(new Pair<>(Device.PRIMARY_ID, pniRegistrationId)),
            true));
    assertThrows(StaleDevicesException.class,
        () -> DestinationDeviceValidator.validateRegistrationIds(account,
            Stream.of(new Pair<>(Device.PRIMARY_ID, aciRegistrationId)),
            true));
    assertThrows(StaleDevicesException.class,
        () -> DestinationDeviceValidator.validateRegistrationIds(account,
            Stream.of(new Pair<>(Device.PRIMARY_ID, pniRegistrationId)),
            false));

    when(device.getPhoneNumberIdentityRegistrationId()).thenReturn(OptionalInt.empty());

    assertDoesNotThrow(
        () -> DestinationDeviceValidator.validateRegistrationIds(account,
            Stream.of(new Pair<>(Device.PRIMARY_ID, aciRegistrationId)),
            false));
    assertDoesNotThrow(
        () -> DestinationDeviceValidator.validateRegistrationIds(account,
            Stream.of(new Pair<>(Device.PRIMARY_ID, aciRegistrationId)),
            true));
    assertThrows(StaleDevicesException.class, () -> DestinationDeviceValidator.validateRegistrationIds(account,
        Stream.of(new Pair<>(Device.PRIMARY_ID, incorrectRegistrationId)), true));
    assertThrows(StaleDevicesException.class, () -> DestinationDeviceValidator.validateRegistrationIds(account,
        Stream.of(new Pair<>(Device.PRIMARY_ID, incorrectRegistrationId)), false));
  }
}
