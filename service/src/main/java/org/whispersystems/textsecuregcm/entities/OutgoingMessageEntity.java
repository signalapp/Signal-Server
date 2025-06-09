/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.util.ServiceIdentifierAdapter;

public record OutgoingMessageEntity(UUID guid,
                                    int type,
                                    long timestamp,

                                    @JsonSerialize(using = ServiceIdentifierAdapter.ServiceIdentifierSerializer.class)
                                    @JsonDeserialize(using = ServiceIdentifierAdapter.ServiceIdentifierDeserializer.class)
                                    @Nullable
                                    ServiceIdentifier sourceUuid,

                                    int sourceDevice,

                                    @JsonSerialize(using = ServiceIdentifierAdapter.ServiceIdentifierSerializer.class)
                                    @JsonDeserialize(using = ServiceIdentifierAdapter.ServiceIdentifierDeserializer.class)
                                    ServiceIdentifier destinationUuid,

                                    @Nullable UUID updatedPni,
                                    byte[] content,
                                    long serverTimestamp,
                                    boolean urgent,
                                    boolean story,
                                    @Nullable byte[] reportSpamToken) {

  @VisibleForTesting
  MessageProtos.Envelope toEnvelope() {
    final MessageProtos.Envelope.Builder builder = MessageProtos.Envelope.newBuilder()
        .setType(MessageProtos.Envelope.Type.forNumber(type()))
        .setClientTimestamp(timestamp())
        .setServerTimestamp(serverTimestamp())
        .setDestinationServiceId(destinationUuid().toServiceIdentifierString())
        .setServerGuid(guid().toString())
        .setStory(story)
        .setUrgent(urgent);

    if (sourceUuid() != null) {
      builder.setSourceServiceId(sourceUuid().toServiceIdentifierString());
      builder.setSourceDevice(sourceDevice());
    }

    if (content() != null) {
      builder.setContent(ByteString.copyFrom(content()));
    }

    if (updatedPni() != null) {
      builder.setUpdatedPni(updatedPni().toString());
    }

    if (reportSpamToken != null) {
      builder.setReportSpamToken(ByteString.copyFrom(reportSpamToken));
    }

    return builder.build();
  }

  public static OutgoingMessageEntity fromEnvelope(final MessageProtos.Envelope envelope) {
    ByteString token = envelope.getReportSpamToken();
    return new OutgoingMessageEntity(
        UUID.fromString(envelope.getServerGuid()),
        envelope.getType().getNumber(),
        envelope.getClientTimestamp(),
        envelope.hasSourceServiceId() ? ServiceIdentifier.valueOf(envelope.getSourceServiceId()) : null,
        envelope.getSourceDevice(),
        envelope.hasDestinationServiceId() ? ServiceIdentifier.valueOf(envelope.getDestinationServiceId()) : null,
        envelope.hasUpdatedPni() ? UUID.fromString(envelope.getUpdatedPni()) : null,
        envelope.getContent().toByteArray(),
        envelope.getServerTimestamp(),
        envelope.getUrgent(),
        envelope.getStory(),
        token.isEmpty() ? null : token.toByteArray());
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
        story == that.story &&
        Arrays.equals(reportSpamToken, that.reportSpamToken);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(
        guid, type, timestamp, sourceUuid, sourceDevice, destinationUuid, updatedPni, serverTimestamp, urgent, story);
    result = 31 * result + Arrays.hashCode(content);
    result = 71 * result + Arrays.hashCode(reportSpamToken);
    return result;
  }
}
