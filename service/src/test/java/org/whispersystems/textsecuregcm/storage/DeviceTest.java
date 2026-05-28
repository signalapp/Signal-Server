/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.dropwizard.jersey.validation.Validators;
import jakarta.validation.ConstraintViolation;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.whispersystems.textsecuregcm.push.WebPushSubscription;
import org.whispersystems.textsecuregcm.util.SystemMapper;

class DeviceTest {

  @ParameterizedTest
  @CsvSource({
      "true, P1D, false",
      "true, P30D, false",
      "true, P31D, false",
      "true, P180D, false",
      "true, P181D, true",
      "false, P1D, false",
      "false, P45D, false",
      "false, P46D, true",
      "false, P180D, true",
  })
  public void testIsExpired(final boolean primary, final Duration timeSinceLastSeen, final boolean expectExpired) {

    final long lastSeen = Instant.now()
        .minus(timeSinceLastSeen)
        // buffer for test runtime
        .plusSeconds(1)
        .toEpochMilli();

    final Device device = new Device();
    device.setId(primary ? Device.PRIMARY_ID : Device.PRIMARY_ID + 1);
    device.setCreated(lastSeen);
    device.setLastSeen(lastSeen);

    assertEquals(expectExpired, device.isExpired());
  }

  @Test
  void deserializeCapabilities() throws JsonProcessingException {
    {
      final Device device = SystemMapper.jsonMapper().readValue("""
          {
            "capabilities": null
          }
          """, Device.class);

      assertNotNull(device.getCapabilities(),
          "Device deserialization should populate null capabilities with an empty set");
    }

    {
      final Device device = SystemMapper.jsonMapper().readValue("{}", Device.class);

      assertNotNull(device.getCapabilities(),
          "Device deserialization should populate null capabilities with an empty set");
    }
  }

  @Test
  void deserializeWebPushSub() throws JsonProcessingException, URISyntaxException {
    {
      final String endpoint = "https://domain.tld/random1";
      final String b64Auth = "BTBZMqHH6r4Tts7J_aSIgg";
      final byte[] auth = Base64.getUrlDecoder().decode(b64Auth);
      final Device device = SystemMapper.jsonMapper().readValue(String.format("""
          {
            "webPush": {
              "endpoint": "%s",
              "auth": "%s",
              "publicKey": "BCVxsr7N_eNgVRqvHtD0zTZsEc6-VV-JvLexhqUzORcxaOzi6-AYWXvTBHm4bjyPjs7Vd8pZGH6SRpkNtoIAiw4"
            }
          }
          """, endpoint, b64Auth), Device.class);

      WebPushSubscription webpush = device.getWebPush();
      final Set<ConstraintViolation<Device>> constraintViolations = Validators.newValidator().validate(device);
      assertTrue(constraintViolations.isEmpty(),
          "Device should pass validation");
      assertNotNull(webpush,
          "Device deserialization should populate webPush");
      assertEquals(new URI(endpoint), webpush.endpoint(),
          "Endpoint populated from device deserialization doesn't match expectation");
      assertArrayEquals(auth, webpush.userAuth(),
          "Auth populated from device deserialization doesn't match expectation");
    }
  }

  @ParameterizedTest
  @CsvSource({
      // Invalid endpoints
      "\"http://domain.tld/random1\", \"BTBZMqHH6r4Tts7J_aSIgg\", \"BCVxsr7N_eNgVRqvHtD0zTZsEc6-VV-JvLexhqUzORcxaOzi6-AYWXvTBHm4bjyPjs7Vd8pZGH6SRpkNtoIAiw4\"",
      "\"https:///random1\", \"BTBZMqHH6r4Tts7J_aSIgg\", \"BCVxsr7N_eNgVRqvHtD0zTZsEc6-VV-JvLexhqUzORcxaOzi6-AYWXvTBHm4bjyPjs7Vd8pZGH6SRpkNtoIAiw4\"",
      "\"unix:///domain.tld/random1\", \"BTBZMqHH6r4Tts7J_aSIgg\", \"BCVxsr7N_eNgVRqvHtD0zTZsEc6-VV-JvLexhqUzORcxaOzi6-AYWXvTBHm4bjyPjs7Vd8pZGH6SRpkNtoIAiw4\"",
      "null, \"BTBZMqHH6r4Tts7J_aSIgg\", \"BCVxsr7N_eNgVRqvHtD0zTZsEc6-VV-JvLexhqUzORcxaOzi6-AYWXvTBHm4bjyPjs7Vd8pZGH6SRpkNtoIAiw4\"",
      "0, \"BTBZMqHH6r4Tts7J_aSIgg\", \"BCVxsr7N_eNgVRqvHtD0zTZsEc6-VV-JvLexhqUzORcxaOzi6-AYWXvTBHm4bjyPjs7Vd8pZGH6SRpkNtoIAiw4\"",
      "[], \"BTBZMqHH6r4Tts7J_aSIgg\", \"BCVxsr7N_eNgVRqvHtD0zTZsEc6-VV-JvLexhqUzORcxaOzi6-AYWXvTBHm4bjyPjs7Vd8pZGH6SRpkNtoIAiw4\"",
      "[\"https://domain.tld.random1\"], \"BTBZMqHH6r4Tts7J_aSIgg\", \"BCVxsr7N_eNgVRqvHtD0zTZsEc6-VV-JvLexhqUzORcxaOzi6-AYWXvTBHm4bjyPjs7Vd8pZGH6SRpkNtoIAiw4\"",
      // Invalid auth
      "\"https://domain.tld/random1\", \"BTBZMqHH6r4Tts7J/aSIgg\", \"BCVxsr7N_eNgVRqvHtD0zTZsEc6-VV-JvLexhqUzORcxaOzi6-AYWXvTBHm4bjyPjs7Vd8pZGH6SRpkNtoIAiw4\"",
      "\"https://domain.tld/random1\", \"BTBZMqHH6r4\", \"BCVxsr7N_eNgVRqvHtD0zTZsEc6-VV-JvLexhqUzORcxaOzi6-AYWXvTBHm4bjyPjs7Vd8pZGH6SRpkNtoIAiw4\"",
      "\"https://domain.tld/random1\", \"BTBZMqHH6r4Tts7J_aSIgkFBQUE\", \"BCVxsr7N_eNgVRqvHtD0zTZsEc6-VV-JvLexhqUzORcxaOzi6-AYWXvTBHm4bjyPjs7Vd8pZGH6SRpkNtoIAiw4\"",
      // Invalid publicKey
      "\"https://domain.tld/random1\", \"BTBZMqHH6r4Tts7J_aSIgg\", \"BCVxsr7N_eNgVRqvHtD0zTZsEc6-VV-JvLexhqUzORcxaOzi6-AYWXvTBHm4bjyPjs4\"",
      "\"https://domain.tld/random1\", \"BTBZMqHH6r4Tts7J_aSIgg\", \"BCVxsr7N_eNgVRqvHtD0zTZsEc6-VV-JvLexhqUzORcxaOzi6-AYWXvTBHm4bjyPjs7Vd8pZGH6SRpkNtoIAiw5BQUFB\"",
      "\"https://domain.tld/random1\", \"BTBZMqHH6r4Tts7J_aSIgg\", \"BAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\"",
      "\"https://domain.tld/random1\", \"BTBZMqHH6r4Tts7J_aSIgg\", \"MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEJXGyvs3942BVGq8e0PTNNmwRzr5VX4m8t7GGpTM5FzFo7OLr4BhZe9MEebhuPI-OztV3ylkYfpJGmQ22ggCLDg\"",
  })
  void deserializeInvalidWebPushSub(String endpoint, String b64Auth, String publicKey) {
    {
      try {
        final Device device = SystemMapper.jsonMapper().readValue(String.format("""
          {
            "webPush": {
              "endpoint": %s,
              "auth": %s,
              "publicKey": %s
            }
          }
          """, endpoint, b64Auth, publicKey), Device.class);
          final Set<ConstraintViolation<Device>> constraintViolations = Validators.newValidator().validate(device);
          assertFalse(constraintViolations.isEmpty(),
              "Invalid device should not pass validation");
      } catch (JsonProcessingException e) {
        return;
      }
    }
  }
}
