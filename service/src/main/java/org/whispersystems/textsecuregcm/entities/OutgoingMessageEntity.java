/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

public record OutgoingMessageEntity(UUID guid, int type, long timestamp, String source, UUID sourceUuid,
                                    int sourceDevice, UUID destinationUuid, UUID updatedPni, byte[] content,
                                    long serverTimestamp) {

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final OutgoingMessageEntity that = (OutgoingMessageEntity) o;
    return type == that.type && timestamp == that.timestamp && sourceDevice == that.sourceDevice
        && serverTimestamp == that.serverTimestamp && guid.equals(that.guid) && Objects.equals(source, that.source)
        && Objects.equals(sourceUuid, that.sourceUuid) && destinationUuid.equals(that.destinationUuid)
        && Objects.equals(updatedPni, that.updatedPni) && Arrays.equals(content, that.content);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(guid, type, timestamp, source, sourceUuid, sourceDevice, destinationUuid, updatedPni,
        serverTimestamp);
    result = 31 * result + Arrays.hashCode(content);
    return result;
  }
}
