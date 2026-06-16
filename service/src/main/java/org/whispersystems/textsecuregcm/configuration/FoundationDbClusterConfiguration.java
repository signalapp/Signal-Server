/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import jakarta.validation.constraints.AssertTrue;
import org.apache.commons.lang3.StringUtils;
import javax.annotation.Nullable;

public record FoundationDbClusterConfiguration(@Nullable String clusterFileUrl,
                                               @Nullable String clusterFileContents) {

  @AssertTrue
  public boolean isSingleClusterFileSourceSpecified() {
    return StringUtils.isBlank(clusterFileUrl) ^ StringUtils.isBlank(clusterFileContents);
  }
}
