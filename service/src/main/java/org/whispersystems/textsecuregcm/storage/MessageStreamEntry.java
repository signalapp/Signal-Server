/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import org.whispersystems.textsecuregcm.entities.MessageProtos;

/// A `MessageStreamEntr` is an entity that can be emitted by the publisher returned by [MessageStream#getMessages()].
/// Message stream entries either produce an individual message (see [Envelope]) or that the initial contents of a
/// message queue have been drained (see [QueueEmpty]).
public sealed interface MessageStreamEntry permits MessageStreamEntry.Envelope, MessageStreamEntry.QueueEmpty {

  /// A message stream entry that carries a single message.
  ///
  /// @param message the message emitted by the publisher
  record Envelope(MessageProtos.Envelope message) implements MessageStreamEntry {
  }

  /// A message stream entry that indicates that the initial contents of a message queue have been emitted by the
  /// publisher; any [Envelope] entries after a `QueueEmpty` entry arrived after caller started reading
  /// messages from the queue.
  record QueueEmpty() implements MessageStreamEntry {
  }
}
