/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.spam;

import java.util.Optional;

/**
 * The result of a spam check. May contain a response to relay to the caller if a message was identified as potential
 * spam or a spam reporting token to include in the delivered message.
 *
 * @param response a transport-appropriate response to return to the sender if the message was identified as potential
 *                 spam, or empty if processing should continue as normal
 * @param token a spam-reporting token to include in the outbound message, or empty if no token applies to the message
 *
 * @param <T> the type of response for messages identified as potential spam
 */
public record SpamCheckResult<T>(Optional<T> response, Optional<byte[]> token) {
}
