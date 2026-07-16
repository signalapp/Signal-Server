/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretBytes;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

class FoundationDbMessagesConfigurationTest {

  @Test
  void isEveryEpochClusterConfigured() {
    assertTrue(new FoundationDbMessagesConfiguration(
        Map.of("messages-0", new FoundationDbClusterConfiguration("test-url")),
        Map.of(0, List.of("messages-0")),
        0,
        Map.of(0, new SecretBytes(TestRandomUtil.nextBytes(16))),
        0,
        FoundationDbMessagesConfiguration.DEFAULT_MAX_WATCHES_PER_CLIENT,
        FoundationDbMessagesConfiguration.DEFAULT_TRANSACTION_TIMEOUT,
        FoundationDbMessagesConfiguration.DEFAULT_TRANSACTION_RETRY_LIMIT,
        FoundationDbMessagesConfiguration.DEFAULT_TRANSACTION_TIMEOUT.multipliedBy(2),
        FoundationDbMessagesConfiguration.DEFAULT_TRANSACTION_RETRY_LIMIT  * 2
    ).isEveryEpochClusterConfigured());

    assertFalse(new FoundationDbMessagesConfiguration(
        Map.of("messages-0", new FoundationDbClusterConfiguration("test-url")),
        Map.of(0, List.of("messages-0", "unconfigured-cluster")),
        0,
        Map.of(0, new SecretBytes(TestRandomUtil.nextBytes(16))),
        0,
        FoundationDbMessagesConfiguration.DEFAULT_MAX_WATCHES_PER_CLIENT,
        FoundationDbMessagesConfiguration.DEFAULT_TRANSACTION_TIMEOUT,
        FoundationDbMessagesConfiguration.DEFAULT_TRANSACTION_RETRY_LIMIT,
        FoundationDbMessagesConfiguration.DEFAULT_TRANSACTION_TIMEOUT.multipliedBy(2),
        FoundationDbMessagesConfiguration.DEFAULT_TRANSACTION_RETRY_LIMIT  * 2
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
        FoundationDbMessagesConfiguration.DEFAULT_MAX_WATCHES_PER_CLIENT,
        FoundationDbMessagesConfiguration.DEFAULT_TRANSACTION_TIMEOUT,
        FoundationDbMessagesConfiguration.DEFAULT_TRANSACTION_RETRY_LIMIT,
        FoundationDbMessagesConfiguration.DEFAULT_TRANSACTION_TIMEOUT.multipliedBy(2),
        FoundationDbMessagesConfiguration.DEFAULT_TRANSACTION_RETRY_LIMIT  * 2
    ).isEveryEpochFreeOfDuplicates());

    assertFalse(new FoundationDbMessagesConfiguration(
        Map.of("messages-0", new FoundationDbClusterConfiguration("test-url")),
        Map.of(0, List.of("messages-0", "messages-0")),
        0,
        Map.of(0, new SecretBytes(TestRandomUtil.nextBytes(16))),
        0,
        FoundationDbMessagesConfiguration.DEFAULT_MAX_WATCHES_PER_CLIENT,
        FoundationDbMessagesConfiguration.DEFAULT_TRANSACTION_TIMEOUT,
        FoundationDbMessagesConfiguration.DEFAULT_TRANSACTION_RETRY_LIMIT,
        FoundationDbMessagesConfiguration.DEFAULT_TRANSACTION_TIMEOUT.multipliedBy(2),
        FoundationDbMessagesConfiguration.DEFAULT_TRANSACTION_RETRY_LIMIT  * 2
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
        FoundationDbMessagesConfiguration.DEFAULT_MAX_WATCHES_PER_CLIENT,
        FoundationDbMessagesConfiguration.DEFAULT_TRANSACTION_TIMEOUT,
        FoundationDbMessagesConfiguration.DEFAULT_TRANSACTION_RETRY_LIMIT,
        FoundationDbMessagesConfiguration.DEFAULT_TRANSACTION_TIMEOUT.multipliedBy(2),
        FoundationDbMessagesConfiguration.DEFAULT_TRANSACTION_RETRY_LIMIT  * 2
    ).isActiveEpochConfigured());

    assertFalse(new FoundationDbMessagesConfiguration(
        Map.of("messages-0", new FoundationDbClusterConfiguration("test-url")),
        Map.of(0, List.of("messages-0")),
        1,
        Map.of(0, new SecretBytes(TestRandomUtil.nextBytes(16))),
        0,
        FoundationDbMessagesConfiguration.DEFAULT_MAX_WATCHES_PER_CLIENT,
        FoundationDbMessagesConfiguration.DEFAULT_TRANSACTION_TIMEOUT,
        FoundationDbMessagesConfiguration.DEFAULT_TRANSACTION_RETRY_LIMIT,
        FoundationDbMessagesConfiguration.DEFAULT_TRANSACTION_TIMEOUT.multipliedBy(2),
        FoundationDbMessagesConfiguration.DEFAULT_TRANSACTION_RETRY_LIMIT  * 2
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
        FoundationDbMessagesConfiguration.DEFAULT_MAX_WATCHES_PER_CLIENT,
        FoundationDbMessagesConfiguration.DEFAULT_TRANSACTION_TIMEOUT,
        FoundationDbMessagesConfiguration.DEFAULT_TRANSACTION_RETRY_LIMIT,
        FoundationDbMessagesConfiguration.DEFAULT_TRANSACTION_TIMEOUT.multipliedBy(2),
        FoundationDbMessagesConfiguration.DEFAULT_TRANSACTION_RETRY_LIMIT  * 2
    ).isCurrentVersionstampCipherKeyConfigured());

    assertFalse(new FoundationDbMessagesConfiguration(
        Map.of("messages-0", new FoundationDbClusterConfiguration("test-url")),
        Map.of(0, List.of("messages-0")),
        0,
        Map.of(0, new SecretBytes(TestRandomUtil.nextBytes(16))),
        1,
        FoundationDbMessagesConfiguration.DEFAULT_MAX_WATCHES_PER_CLIENT,
        FoundationDbMessagesConfiguration.DEFAULT_TRANSACTION_TIMEOUT,
        FoundationDbMessagesConfiguration.DEFAULT_TRANSACTION_RETRY_LIMIT,
        FoundationDbMessagesConfiguration.DEFAULT_TRANSACTION_TIMEOUT.multipliedBy(2),
        FoundationDbMessagesConfiguration.DEFAULT_TRANSACTION_RETRY_LIMIT  * 2
    ).isCurrentVersionstampCipherKeyConfigured());
  }
}
