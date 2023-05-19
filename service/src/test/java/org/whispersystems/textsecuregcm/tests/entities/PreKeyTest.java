/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.entities;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.whispersystems.textsecuregcm.tests.util.JsonHelpers.asJson;
import static org.whispersystems.textsecuregcm.tests.util.JsonHelpers.jsonFixture;

import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.entities.PreKey;

import java.util.Base64;

class PreKeyTest {

  private static final byte[] PUBLIC_KEY = Base64.getDecoder().decode("BQ+NbroQtVKyFaCSfqzSw8Wy72Ff22RSa5ERKTv5DIk2");

  @Test
  void serializeToJSONV2() throws Exception {
    PreKey preKey = new PreKey(1234, PUBLIC_KEY);

    assertThat("PreKeyV2 Serialization works",
               asJson(preKey),
               is(equalTo(jsonFixture("fixtures/prekey_v2.json"))));
  }

}
