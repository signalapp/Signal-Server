/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.protobuf.ByteString;
import katie.FullTreeHead;
import katie.TreeHead;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ProtobufAdapterTest {
  private record FullTreeHeadTestRecord(@JsonSerialize(using = FullTreeHeadProtobufAdapter.Serializer.class)
                                        @JsonDeserialize(using = FullTreeHeadProtobufAdapter.Deserializer.class)
                                        FullTreeHead fullTreeHead) {
  }

  @Test
  void serializeDeserialize() throws JsonProcessingException {
    final TreeHead treeHead = TreeHead.newBuilder()
        .setTreeSize(10)
        .setTimestamp(12345)
        .setSignature(ByteString.copyFrom(TestRandomUtil.nextBytes(16)))
        .build();

    final FullTreeHead fullTreeHead = FullTreeHead.newBuilder()
        .setTreeHead(treeHead)
        .addAllConsistency(List.of(ByteString.copyFrom(TestRandomUtil.nextBytes(20))))
        .build();

    final FullTreeHeadTestRecord expectedTestRecord = new FullTreeHeadTestRecord(fullTreeHead);

    // Serialize to JSON
    final String json = SystemMapper.jsonMapper().writeValueAsString(expectedTestRecord);

    // Deserialize back to record
    assertEquals(expectedTestRecord, SystemMapper.jsonMapper().readValue(json, FullTreeHeadTestRecord.class));
  }

  @Test
  void deserializeFailure() {
    assertThrows(JsonParseException.class,
        () -> SystemMapper.jsonMapper().readValue("this is not valid json", FullTreeHeadTestRecord.class));
  }
}
