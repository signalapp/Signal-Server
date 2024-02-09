/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.calls.routing;

import org.junit.jupiter.api.Test;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class CallDnsRecordsManagerTest {

  @Test
  public void testParseDnsRecords() throws IOException {
    var input = """
        {
          "aByRegion": {
            "datacenter-1": [
              "127.0.0.1"
            ],
            "datacenter-2": [
              "127.0.0.2",
              "127.0.0.3"
            ],
            "datacenter-3": [
              "127.0.0.4",
              "127.0.0.5"
            ],
            "datacenter-4": [
              "127.0.0.6",
              "127.0.0.7"
            ]
          },
          "aaaaByRegion": {
            "datacenter-1": [
              "2600:1111:2222:3333:0:20:0:0",
              "2600:1111:2222:3333:0:21:0:0",
              "2600:1111:2222:3333:0:22:0:0"
            ],
            "datacenter-2": [
              "2600:1111:2222:3333:0:23:0:0",
              "2600:1111:2222:3333:0:24:0:0"
            ],
            "datacenter-3": [
              "2600:1111:2222:3333:0:25:0:0",
              "2600:1111:2222:3333:0:26:0:0"
            ],
            "datacenter-4": [
              "2600:1111:2222:3333:0:27:0:0"
            ]
          }
        }
    """;

    var actual = CallDnsRecordsManager.parseRecords(new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)));
    var expected = new CallDnsRecords(
        Map.of(
            "datacenter-1", Stream.of("127.0.0.1").map(this::getAddressByName).toList(),
            "datacenter-2",  Stream.of("127.0.0.2", "127.0.0.3").map(this::getAddressByName).toList(),
            "datacenter-3",  Stream.of("127.0.0.4", "127.0.0.5").map(this::getAddressByName).toList(),
            "datacenter-4",  Stream.of("127.0.0.6", "127.0.0.7").map(this::getAddressByName).toList()
        ),
        Map.of(
            "datacenter-1", Stream.of(
                "2600:1111:2222:3333:0:20:0:0",
                "2600:1111:2222:3333:0:21:0:0",
                "2600:1111:2222:3333:0:22:0:0"
            ).map(this::getAddressByName).toList(),
            "datacenter-2", Stream.of(
                "2600:1111:2222:3333:0:23:0:0",
                "2600:1111:2222:3333:0:24:0:0")
                .map(this::getAddressByName).toList(),
            "datacenter-3", Stream.of(
                    "2600:1111:2222:3333:0:25:0:0",
                    "2600:1111:2222:3333:0:26:0:0")
                .map(this::getAddressByName).toList(),
            "datacenter-4", Stream.of(
                    "2600:1111:2222:3333:0:27:0:0"
                ).map(this::getAddressByName).toList()
            )
    );

    assertThat(actual).isEqualTo(expected);
  }

  InetAddress getAddressByName(String ip) {
    try {
      return InetAddress.getByName(ip) ;
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }
}
