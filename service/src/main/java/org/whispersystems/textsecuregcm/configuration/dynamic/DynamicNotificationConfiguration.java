/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration.dynamic;

public record DynamicNotificationConfiguration(boolean sendRemainingMessagesNotificationImmediately) {

  public static final DynamicNotificationConfiguration DEFAULT =
      new DynamicNotificationConfiguration(false);
}
