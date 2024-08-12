/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import katie.FullTreeHead;

public class FullTreeHeadProtobufAdapter {

  public static class Serializer extends ProtobufAdapter.Serializer<FullTreeHead> {}

  public static class Deserializer extends ProtobufAdapter.Deserializer<FullTreeHead> {

    public Deserializer() {
      super(FullTreeHead::newBuilder);
    }
  }
}
