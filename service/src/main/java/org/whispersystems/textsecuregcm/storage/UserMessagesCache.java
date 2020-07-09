package org.whispersystems.textsecuregcm.storage;

import com.google.common.annotations.VisibleForTesting;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntity;
import org.whispersystems.textsecuregcm.push.PushSender;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface UserMessagesCache {
    @VisibleForTesting
    static OutgoingMessageEntity constructEntityFromEnvelope(long id, MessageProtos.Envelope envelope) {
      return new OutgoingMessageEntity(id, true,
                                       envelope.hasServerGuid() ? UUID.fromString(envelope.getServerGuid()) : null,
                                       envelope.getType().getNumber(),
                                       envelope.getRelay(),
                                       envelope.getTimestamp(),
                                       envelope.getSource(),
                                       envelope.hasSourceUuid() ? UUID.fromString(envelope.getSourceUuid()) : null,
                                       envelope.getSourceDevice(),
                                       envelope.hasLegacyMessage() ? envelope.getLegacyMessage().toByteArray() : null,
                                       envelope.hasContent() ? envelope.getContent().toByteArray() : null,
                                       envelope.hasServerTimestamp() ? envelope.getServerTimestamp() : 0);
    }

    long insert(UUID guid, String destination, long destinationDevice, MessageProtos.Envelope message);

    Optional<OutgoingMessageEntity> remove(String destination, long destinationDevice, long id);

    Optional<OutgoingMessageEntity> remove(String destination, long destinationDevice, String sender, long timestamp);

    Optional<OutgoingMessageEntity> remove(String destination, long destinationDevice, UUID guid);

    List<OutgoingMessageEntity> get(String destination, long destinationDevice, int limit);

    void clear(String destination);

    void clear(String destination, long deviceId);
}
