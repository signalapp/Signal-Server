/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.configuration.MonitoredS3ObjectConfiguration;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Inet4Address;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class AsnManagerTest {

  @Test
  void getAsn() throws IOException {
    final MonitoredS3ObjectConfiguration configuration = new MonitoredS3ObjectConfiguration();
    configuration.setS3Region("ap-northeast-3");

    final AsnManager asnManager = new AsnManager(mock(ScheduledExecutorService.class), configuration);

    assertEquals(Optional.empty(), asnManager.getAsn("10.0.0.1"));

    try (final InputStream tableInputStream = getClass().getResourceAsStream("ip2asn-test.tsv")) {
      asnManager.handleAsnTableChanged(tableInputStream);
    }

    assertEquals(Optional.of(7922L), asnManager.getAsn("50.79.54.1"));
    assertEquals(Optional.of(7552L), asnManager.getAsn("27.79.32.1"));
    assertEquals(Optional.empty(), asnManager.getAsn("32.79.117.1"));
    assertEquals(Optional.empty(), asnManager.getAsn("10.0.0.1"));
  }
}
