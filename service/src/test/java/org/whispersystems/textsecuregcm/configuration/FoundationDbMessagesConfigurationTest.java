/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class FoundationDbMessagesConfigurationTest {

  @Test
  void isEveryEpochClusterConfigured() {
    assertTrue(new FoundationDbMessagesConfiguration(
        Map.of("messages-0", new FoundationDbClusterConfiguration("test-url", null)),
        Map.of(0, List.of("messages-0")),
        0
    ).isEveryEpochClusterConfigured());

    assertFalse(new FoundationDbMessagesConfiguration(
        Map.of("messages-0", new FoundationDbClusterConfiguration("test-url", null)),
        Map.of(0, List.of("messages-0", "unconfigured-cluster")),
        0
    ).isEveryEpochClusterConfigured());
  }

  @Test
  void isEveryEpochFreeOfDuplicates() {
    assertTrue(new FoundationDbMessagesConfiguration(
        Map.of("messages-0", new FoundationDbClusterConfiguration("test-url", null)),
        Map.of(0, List.of("messages-0")),
        0
    ).isEveryEpochFreeOfDuplicates());

    assertFalse(new FoundationDbMessagesConfiguration(
        Map.of("messages-0", new FoundationDbClusterConfiguration("test-url", null)),
        Map.of(0, List.of("messages-0", "messages-0")),
        0
    ).isEveryEpochFreeOfDuplicates());
  }

  @Test
  void isActiveEpochConfigured() {
    assertTrue(new FoundationDbMessagesConfiguration(
        Map.of("messages-0", new FoundationDbClusterConfiguration("test-url", null)),
        Map.of(0, List.of("messages-0")),
        0
    ).isActiveEpochConfigured());

    assertFalse(new FoundationDbMessagesConfiguration(
        Map.of("messages-0", new FoundationDbClusterConfiguration("test-url", null)),
        Map.of(0, List.of("messages-0")),
        1
    ).isActiveEpochConfigured());
  }
}
