/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import java.time.Instant;

public class InstantAdapter {

  public static class EpochSecondSerializer extends JsonSerializer<Instant> {

    @Override
    public void serialize(final Instant value, final JsonGenerator gen, final SerializerProvider serializers)
        throws IOException {

      gen.writeNumber(value.getEpochSecond());
    }
  }

}
