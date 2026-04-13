/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Base64;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

class AccountAttributesTest {

  private static final ObjectMapper mapper = SystemMapper.jsonMapper();

  @Test
  void testSerializationDeserializationRoundTrip() throws Exception {

    final String originalJson = testJson();

    final AccountAttributes attributes = mapper.readValue(originalJson, AccountAttributes.class);

    assertEquals(originalJson, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(attributes));
  }

  private static String testJson() {

    return String.format("""
        {
          "fetchesMessages" : false,
          "registrationId" : 123,
          "pniRegistrationId" : 456,
          "name" : "%s",
          "capabilities" : {
            "storage" : true
          },
          "registrationLock" : null,
          "unidentifiedAccessKey" : "%s",
          "unrestrictedUnidentifiedAccess" : false,
          "discoverableByPhoneNumber" : true,
          "recoveryPassword" : "%s"
        }
        """,
        Base64.getEncoder().withoutPadding().encodeToString(TestRandomUtil.nextBytes(128)), // name
        Base64.getEncoder().encodeToString(TestRandomUtil.nextBytes(16)), // unidentifiedAccessKey
        Base64.getEncoder().encodeToString(TestRandomUtil.nextBytes(32)) // recoveryPassword
    ).trim();
  }
}
