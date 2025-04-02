/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.spam;

public enum MessageType {
  INDIVIDUAL_IDENTIFIED_SENDER,
  SYNC,
  INDIVIDUAL_SEALED_SENDER,
  MULTI_RECIPIENT_SEALED_SENDER,
  INDIVIDUAL_STORY,
  MULTI_RECIPIENT_STORY,
}
