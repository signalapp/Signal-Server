/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import com.google.common.annotations.VisibleForTesting;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.StringUtils;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import reactor.core.publisher.Mono;

/**
 * Checks if a device may benefit from receiving a push notification
 */
public class IdleWakeupEligibilityChecker {

  @VisibleForTesting
  static final Duration MIN_SHORT_IDLE_DURATION = Duration.ofDays(3);

  @VisibleForTesting
  static final Duration MAX_SHORT_IDLE_DURATION = Duration.ofDays(30);

  @VisibleForTesting
  static final Duration MIN_LONG_IDLE_DURATION = Duration.ofDays(60);

  @VisibleForTesting
  static final Duration MAX_LONG_IDLE_DURATION = Duration.ofDays(75);

  private final MessagesManager messagesManager;
  private final Clock clock;

  public IdleWakeupEligibilityChecker(final Clock clock, final MessagesManager messagesManager) {
    this.messagesManager = messagesManager;
    this.clock = clock;
  }

  /**
   * Determine whether the device may benefit from a push notification.
   *
   * @param account The account to check
   * @param device  The device to check
   * @return true if the device may benefit from a push notification, otherwise false
   * @implNote There are two populations that may benefit from a wakeup:
   * <ol>
   * <li> Devices that have only been idle for a little while, but have messages that they don't seem to be retrieving
   * <li> Devices that have been idle for a long time, but don't have any messages
   * </ol>
   * We think the first group sometimes just needs a little nudge to wake up and get their messages, and the latter
   * group generally WOULD get their messages if they had any. We want to notify the first group to prompt them to
   * actually get their messages and the latter group to prevent them from getting deleted due to inactivity (since they
   * are otherwise healthy installations that just aren't getting much traffic).
   */
  public CompletableFuture<Boolean> isDeviceEligible(final Account account, final Device device) {

    if (!hasPushToken(device)) {
      return CompletableFuture.completedFuture(false);
    }

    if (isShortIdle(device, clock)) {
      return messagesManager.mayHaveUrgentPersistedMessages(account.getIdentifier(IdentityType.ACI), device);
    } else if (isLongIdle(device, clock)) {
      return messagesManager.mayHavePersistedMessages(account.getIdentifier(IdentityType.ACI), device)
          .thenApply(mayHavePersistedMessages -> !mayHavePersistedMessages);
    } else {
      return CompletableFuture.completedFuture(false);
    }
  }

  @VisibleForTesting
  static boolean isShortIdle(final Device device, final Clock clock) {
    final Duration idleDuration = Duration.between(Instant.ofEpochMilli(device.getLastSeen()), clock.instant());

    return idleDuration.compareTo(MIN_SHORT_IDLE_DURATION) >= 0 && idleDuration.compareTo(MAX_SHORT_IDLE_DURATION) < 0;
  }

  @VisibleForTesting
  static boolean isLongIdle(final Device device, final Clock clock) {
    final Duration idleDuration = Duration.between(Instant.ofEpochMilli(device.getLastSeen()), clock.instant());

    return idleDuration.compareTo(MIN_LONG_IDLE_DURATION) >= 0 && idleDuration.compareTo(MAX_LONG_IDLE_DURATION) < 0;
  }

  @VisibleForTesting
  static boolean hasPushToken(final Device device) {
    return !StringUtils.isAllBlank(device.getApnId(), device.getGcmId());
  }

}
