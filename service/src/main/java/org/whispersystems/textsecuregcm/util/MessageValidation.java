/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.util;

import org.whispersystems.textsecuregcm.controllers.MismatchedDevicesException;
import org.whispersystems.textsecuregcm.controllers.StaleDevicesException;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MessageValidation {
  public static <T> void validateRegistrationIds(Account account, List<T> messages, Function<T, Long> getDeviceId, Function<T, Integer> getRegistrationId)
      throws StaleDevicesException {
    final Stream<Pair<Long, Integer>> deviceIdAndRegistrationIdStream = messages
        .stream()
        .map(message -> new Pair<>(getDeviceId.apply(message), getRegistrationId.apply(message)));
    validateRegistrationIds(account, deviceIdAndRegistrationIdStream);
  }

  public static void validateRegistrationIds(Account account, Stream<Pair<Long, Integer>> deviceIdAndRegistrationIdStream)
      throws StaleDevicesException {
    final List<Long> staleDevices = deviceIdAndRegistrationIdStream
        .filter(deviceIdAndRegistrationId -> deviceIdAndRegistrationId.second() > 0)
        .filter(deviceIdAndRegistrationId -> {
          Optional<Device> device = account.getDevice(deviceIdAndRegistrationId.first());
          return device.isPresent() && deviceIdAndRegistrationId.second() != device.get().getRegistrationId();
        })
        .map(Pair::first)
        .collect(Collectors.toList());

    if (!staleDevices.isEmpty()) {
      throw new StaleDevicesException(staleDevices);
    }
  }

  public static <T> void validateCompleteDeviceList(Account account, Collection<T> messages, Function<T, Long> getDeviceId, boolean isSyncMessage,
      Optional<Long> authenticatedDeviceId)
      throws MismatchedDevicesException {
    Set<Long> messageDeviceIds = messages.stream().map(getDeviceId)
        .collect(Collectors.toSet());
    validateCompleteDeviceList(account, messageDeviceIds, isSyncMessage, authenticatedDeviceId);
  }

  public static void validateCompleteDeviceList(Account account, Set<Long> messageDeviceIds, boolean isSyncMessage,
      Optional<Long> authenticatedDeviceId)
      throws MismatchedDevicesException {
    Set<Long> accountDeviceIds = new HashSet<>();

    List<Long> missingDeviceIds = new LinkedList<>();
    List<Long> extraDeviceIds = new LinkedList<>();

    for (Device device : account.getDevices()) {
      if (device.isEnabled() &&
          !(isSyncMessage && device.getId() == authenticatedDeviceId.get())) {
        accountDeviceIds.add(device.getId());

        if (!messageDeviceIds.contains(device.getId())) {
          missingDeviceIds.add(device.getId());
        }
      }
    }

    for (Long deviceId : messageDeviceIds) {
      if (!accountDeviceIds.contains(deviceId)) {
        extraDeviceIds.add(deviceId);
      }
    }

    if (!missingDeviceIds.isEmpty() || !extraDeviceIds.isEmpty()) {
      throw new MismatchedDevicesException(missingDeviceIds, extraDeviceIds);
    }
  }

}
