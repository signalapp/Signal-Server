package org.whispersystems.textsecuregcm.storage.foundationdb;

import com.apple.foundationdb.tuple.Versionstamp;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.storage.MessageStreamEntry;
import org.whispersystems.textsecuregcm.util.UUIDUtil;

sealed interface FoundationDbMessageStreamEntry permits FoundationDbMessageStreamEntry.Message,
    FoundationDbMessageStreamEntry.QueueEmpty {

  record Message(Versionstamp versionstamp, MessageProtos.Envelope partialEnvelope) implements FoundationDbMessageStreamEntry {}

  record QueueEmpty() implements FoundationDbMessageStreamEntry {}

  default MessageStreamEntry toMessageStreamEntry(final MessageGuidCodec messageGuidCodec) {
    return switch (this) {
      case Message(final Versionstamp versionstamp, final MessageProtos.Envelope partialEnvelope): {
        yield new MessageStreamEntry.Envelope(partialEnvelope.toBuilder()
            .setServerGuidBinary(UUIDUtil.toByteString(messageGuidCodec.encodeMessageGuid(versionstamp)))
            .build());
      }
      case QueueEmpty():
        yield new MessageStreamEntry.QueueEmpty();
    };
  }
}
