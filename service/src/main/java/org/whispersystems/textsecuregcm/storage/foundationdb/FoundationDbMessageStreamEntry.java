package org.whispersystems.textsecuregcm.storage.foundationdb;

import com.apple.foundationdb.tuple.Versionstamp;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.UncheckedIOException;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.storage.MessageStreamEntry;
import org.whispersystems.textsecuregcm.util.UUIDUtil;

sealed interface FoundationDbMessageStreamEntry permits FoundationDbMessageStreamEntry.Message,
    FoundationDbMessageStreamEntry.QueueEmpty {

  record Message(Versionstamp versionstamp, byte[] payload) implements FoundationDbMessageStreamEntry {}

  record QueueEmpty() implements FoundationDbMessageStreamEntry {}

  default MessageStreamEntry toMessageStreamEntry(final MessageGuidCodec messageGuidCodec) {
    return switch (this) {
      case Message(final Versionstamp versionstamp, final byte[] payload): {
        try {
          yield new MessageStreamEntry.Envelope(MessageProtos.Envelope.parseFrom(payload)
              .toBuilder()
              .setServerGuidBinary(UUIDUtil.toByteString(messageGuidCodec.encodeMessageGuid(versionstamp)))
              .build());
        } catch (final InvalidProtocolBufferException e) {
          throw new UncheckedIOException(e);
        }
      }
      case QueueEmpty():
        yield new MessageStreamEntry.QueueEmpty();
    };
  }
}
