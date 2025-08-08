/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import org.whispersystems.textsecuregcm.entities.MessageProtos;

/// A message stream publishes an ordered stream of Signal messages from a destination device's queue and provides a
/// mechanism for consumers to acknowledge receipt of delivered messages.
public interface MessageStream {

  /// Publishes a non-terminating stream of [MessageStreamEntry.Envelope] entities and at most one
  /// [MessageStreamEntry.QueueEmpty].
  ///
  /// @return a non-terminating stream of message stream entries
  Flow.Publisher<MessageStreamEntry> getMessages();

  /// Acknowledges receipt of the given message. Implementations may delete the message immediately or defer deletion for
  /// inclusion in a more efficient batch deletion.
  ///
  /// @param message the message to acknowledge
  ///
  /// @return a future that completes when the message stream has processed the acknowledgement
  CompletableFuture<Void> acknowledgeMessage(MessageProtos.Envelope message);
}
