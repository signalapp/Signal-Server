/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import katie.MonitorProof;

public class MonitorProofProtobufAdapter {

  public static class Serializer extends ProtobufAdapter.Serializer<MonitorProof> {}

  public static class Deserializer extends ProtobufAdapter.Deserializer<MonitorProof> {

    public Deserializer() {
      super(MonitorProof::newBuilder);
    }
  }
}
