/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nullable;

public record OutgoingMessageEntity(UUID guid, int type, long timestamp, @Nullable UUID sourceUuid, int sourceDevice,
                                    UUID destinationUuid, @Nullable UUID updatedPni, byte[] content,
                                    long serverTimestamp, boolean urgent, boolean story) {

  public MessageProtos.Envelope toEnvelope() {
    final MessageProtos.Envelope.Builder builder = MessageProtos.Envelope.newBuilder()
        .setType(MessageProtos.Envelope.Type.forNumber(type()))
        .setTimestamp(timestamp())
        .setServerTimestamp(serverTimestamp())
        .setDestinationUuid(destinationUuid().toString())
        .setServerGuid(guid().toString())
        .setStory(story)
        .setUrgent(urgent);

    if (sourceUuid() != null) {
      builder.setSourceUuid(sourceUuid().toString());
      builder.setSourceDevice(sourceDevice());
    }

    if (content() != null) {
      builder.setContent(ByteString.copyFrom(content()));
    }

    if (updatedPni() != null) {
      builder.setUpdatedPni(updatedPni().toString());
    }

    return builder.build();
  }

  public static OutgoingMessageEntity fromEnvelope(final MessageProtos.Envelope envelope) {
    return new OutgoingMessageEntity(
        UUID.fromString(envelope.getServerGuid()),
        envelope.getType().getNumber(),
        envelope.getTimestamp(),
        envelope.hasSourceUuid() ? UUID.fromString(envelope.getSourceUuid()) : null,
        envelope.getSourceDevice(),
        envelope.hasDestinationUuid() ? UUID.fromString(envelope.getDestinationUuid()) : null,
        envelope.hasUpdatedPni() ? UUID.fromString(envelope.getUpdatedPni()) : null,
        envelope.getContent().toByteArray(),
        envelope.getServerTimestamp(),
        envelope.getUrgent(),
        envelope.getStory());
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final OutgoingMessageEntity that = (OutgoingMessageEntity) o;
    return guid.equals(that.guid) &&
        type == that.type &&
        timestamp == that.timestamp &&
        Objects.equals(sourceUuid, that.sourceUuid) &&
        sourceDevice == that.sourceDevice &&
        destinationUuid.equals(that.destinationUuid) &&
        Objects.equals(updatedPni, that.updatedPni) &&
        Arrays.equals(content, that.content) &&
        serverTimestamp == that.serverTimestamp &&
        urgent == that.urgent &&
        story == that.story;
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(guid, type, timestamp, sourceUuid, sourceDevice, destinationUuid, updatedPni,
        serverTimestamp, urgent, story);
    result = 31 * result + Arrays.hashCode(content);
    return result;
  }
}
