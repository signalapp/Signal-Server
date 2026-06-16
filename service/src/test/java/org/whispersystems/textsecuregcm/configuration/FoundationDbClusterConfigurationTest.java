/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class FoundationDbClusterConfigurationTest {

  @ParameterizedTest
  @MethodSource
  void isSingleClusterFileSourceSpecified(final FoundationDbClusterConfiguration clusterConfiguration,
      final boolean expectSingleClusterFileSourceSpecified) {

    assertEquals(expectSingleClusterFileSourceSpecified, clusterConfiguration.isSingleClusterFileSourceSpecified());
  }

  private static List<Arguments> isSingleClusterFileSourceSpecified() {
    return List.of(
        Arguments.argumentSet("Cluster file URL only",
            new FoundationDbClusterConfiguration("test-url", null), true),

        Arguments.argumentSet("Cluster file contents only",
            new FoundationDbClusterConfiguration(null, "test-contents"), true),

        Arguments.argumentSet("Both",
            new FoundationDbClusterConfiguration("test-url", "test-contents"), false),

        Arguments.argumentSet("Neither",
            new FoundationDbClusterConfiguration(null, null), false)
    );
  }
}
