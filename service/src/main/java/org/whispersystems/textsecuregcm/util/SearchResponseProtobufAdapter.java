/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import katie.SearchResponse;

public class SearchResponseProtobufAdapter {

  public static class Serializer extends ProtobufAdapter.Serializer<SearchResponse> {}

  public static class Deserializer extends ProtobufAdapter.Deserializer<SearchResponse> {

    public Deserializer() {
      super(SearchResponse::newBuilder);
    }
  }
}
