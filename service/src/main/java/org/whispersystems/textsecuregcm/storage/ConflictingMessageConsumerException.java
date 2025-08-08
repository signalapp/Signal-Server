/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import org.whispersystems.textsecuregcm.util.NoStackTraceException;

/// Indicates that more than one consumer is trying to read a specific message queue at the same time.
public class ConflictingMessageConsumerException extends NoStackTraceException {
}
