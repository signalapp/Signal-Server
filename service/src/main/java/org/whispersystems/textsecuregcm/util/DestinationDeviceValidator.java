/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.whispersystems.textsecuregcm.controllers.MismatchedDevicesException;
import org.whispersystems.textsecuregcm.controllers.StaleDevicesException;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;

public class DestinationDeviceValidator {

  /**
   * Validates that the given device ID/registration ID pairs exactly match the corresponding device ID/registration ID
   * pairs in the given destination account. This method does <em>not</em> validate that all devices associated with the
   * destination account are present in the given device ID/registration ID pairs.
   *
   * @param account the destination account against which to check the given device ID/registration ID pairs
   * @param registrationIdsByDeviceId a map of device IDs to registration IDs
   * @param usePhoneNumberIdentity if {@code true}, compare provided registration IDs against device registration IDs
   *                               associated with the account's PNI (if available); compare against the ACI-associated
   *                               registration ID otherwise
   *
   * @throws StaleDevicesException if the device ID/registration ID pairs contained an entry for which the destination
   *                               account does not have a corresponding device or if the registration IDs do not match
   */
  public static void validateRegistrationIds(final Account account,
      final Map<Long, Integer> registrationIdsByDeviceId,
      final boolean usePhoneNumberIdentity) throws StaleDevicesException {

    final List<Long> staleDevices = new ArrayList<>();

    registrationIdsByDeviceId.forEach((deviceId, registrationId) -> {
      if (registrationId > 0) {
        final boolean registrationIdMatches =
            account.getDevice(deviceId).map(device -> registrationId == (usePhoneNumberIdentity ?
                    device.getPhoneNumberIdentityRegistrationId().orElse(device.getRegistrationId()) :
                    device.getRegistrationId()))
                .orElse(false);

        if (!registrationIdMatches) {
          staleDevices.add(deviceId);
        }
      }
    });

    if (!staleDevices.isEmpty()) {
      throw new StaleDevicesException(staleDevices);
    }
  }

  /**
   * Validates that the given set of device IDs from a set of messages matches the set of device IDs associated with the
   * given destination account in preparation for sending those messages to the destination account. In general, the set
   * of device IDs must exactly match the set of active devices associated with the destination account. When sending a
   * "sync," message, though, the authenticated account is sending messages from one of their devices to all other
   * devices; in that case, callers must pass the ID of the sending device in the set of {@code excludedDeviceIds}.
   *
   * @param account the destination account against which to check the given set of device IDs
   * @param messageDeviceIds the set of device IDs to check against the destination account
   * @param excludedDeviceIds a set of device IDs that may be associated with the destination account, but must not be
   *                          present in the given set of device IDs (i.e. the device that is sending a sync message)
   *
   * @throws MismatchedDevicesException if the given set of device IDs contains entries not currently associated with
   *                                    the destination account or is missing entries associated with the destination
   *                                    account
   */
  public static void validateCompleteDeviceList(final Account account,
      final Set<Long> messageDeviceIds,
      final Set<Long> excludedDeviceIds) throws MismatchedDevicesException {

    final Set<Long> accountDeviceIds = account.getDevices().stream()
        .filter(Device::isEnabled)
        .map(Device::getId)
        .filter(deviceId -> !excludedDeviceIds.contains(deviceId))
        .collect(Collectors.toSet());

    final Set<Long> missingDeviceIds = new HashSet<>(accountDeviceIds);
    missingDeviceIds.removeAll(messageDeviceIds);

    final Set<Long> extraDeviceIds = new HashSet<>(messageDeviceIds);
    extraDeviceIds.removeAll(accountDeviceIds);

    if (!missingDeviceIds.isEmpty() || !extraDeviceIds.isEmpty()) {
      throw new MismatchedDevicesException(new ArrayList<>(missingDeviceIds), new ArrayList<>(extraDeviceIds));
    }
  }
}
