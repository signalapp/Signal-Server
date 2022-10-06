/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.registration;

/**
 * A message transport is a medium via which verification codes can be delivered to a destination phone.
 */
public enum MessageTransport {
  SMS,
  VOICE
}
