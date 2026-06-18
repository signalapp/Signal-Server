/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretBytes;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class FoundationDbMessagesConfigurationTest {

  @Test
  void isEveryEpochClusterConfigured() {
    assertTrue(new FoundationDbMessagesConfiguration(
        Map.of("messages-0", new FoundationDbClusterConfiguration("test-url")),
        Map.of(0, List.of("messages-0")),
        0,
        Map.of(0, new SecretBytes(TestRandomUtil.nextBytes(16))),
        0,
        FoundationDbMessagesConfiguration.DEFAULT_MAX_WATCHES_PER_CLIENT
    ).isEveryEpochClusterConfigured());

    assertFalse(new FoundationDbMessagesConfiguration(
        Map.of("messages-0", new FoundationDbClusterConfiguration("test-url")),
        Map.of(0, List.of("messages-0", "unconfigured-cluster")),
        0,
        Map.of(0, new SecretBytes(TestRandomUtil.nextBytes(16))),
        0,
        FoundationDbMessagesConfiguration.DEFAULT_MAX_WATCHES_PER_CLIENT
    ).isEveryEpochClusterConfigured());
  }

  @Test
  void isEveryEpochFreeOfDuplicates() {
    assertTrue(new FoundationDbMessagesConfiguration(
        Map.of("messages-0", new FoundationDbClusterConfiguration("test-url")),
        Map.of(0, List.of("messages-0")),
        0,
        Map.of(0, new SecretBytes(TestRandomUtil.nextBytes(16))),
        0,
        FoundationDbMessagesConfiguration.DEFAULT_MAX_WATCHES_PER_CLIENT
    ).isEveryEpochFreeOfDuplicates());

    assertFalse(new FoundationDbMessagesConfiguration(
        Map.of("messages-0", new FoundationDbClusterConfiguration("test-url")),
        Map.of(0, List.of("messages-0", "messages-0")),
        0,
        Map.of(0, new SecretBytes(TestRandomUtil.nextBytes(16))),
        0,
        FoundationDbMessagesConfiguration.DEFAULT_MAX_WATCHES_PER_CLIENT
    ).isEveryEpochFreeOfDuplicates());
  }

  @Test
  void isActiveEpochConfigured() {
    assertTrue(new FoundationDbMessagesConfiguration(
        Map.of("messages-0", new FoundationDbClusterConfiguration("test-url")),
        Map.of(0, List.of("messages-0")),
        0,
        Map.of(0, new SecretBytes(TestRandomUtil.nextBytes(16))),
        0,
        FoundationDbMessagesConfiguration.DEFAULT_MAX_WATCHES_PER_CLIENT
    ).isActiveEpochConfigured());

    assertFalse(new FoundationDbMessagesConfiguration(
        Map.of("messages-0", new FoundationDbClusterConfiguration("test-url")),
        Map.of(0, List.of("messages-0")),
        1,
        Map.of(0, new SecretBytes(TestRandomUtil.nextBytes(16))),
        0,
        FoundationDbMessagesConfiguration.DEFAULT_MAX_WATCHES_PER_CLIENT
    ).isActiveEpochConfigured());
  }

  @Test
  void isCurrentVersionstampCipherKeyConfigured() {
    assertTrue(new FoundationDbMessagesConfiguration(
        Map.of("messages-0", new FoundationDbClusterConfiguration("test-url")),
        Map.of(0, List.of("messages-0")),
        0,
        Map.of(0, new SecretBytes(TestRandomUtil.nextBytes(16))),
        0,
        FoundationDbMessagesConfiguration.DEFAULT_MAX_WATCHES_PER_CLIENT
    ).isCurrentVersionstampCipherKeyConfigured());

    assertFalse(new FoundationDbMessagesConfiguration(
        Map.of("messages-0", new FoundationDbClusterConfiguration("test-url")),
        Map.of(0, List.of("messages-0")),
        0,
        Map.of(0, new SecretBytes(TestRandomUtil.nextBytes(16))),
        1,
        FoundationDbMessagesConfiguration.DEFAULT_MAX_WATCHES_PER_CLIENT
    ).isCurrentVersionstampCipherKeyConfigured());
  }
}
