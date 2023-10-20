/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.spam;

import java.util.Optional;

import org.glassfish.jersey.server.ContainerRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A PushChallengeConfig may be provided by an upstream request filter. If request contains a
 * property for PROPERTY_NAME it can be forwarded to a downstream filter to indicate whether
 * push-token challenges can be used in place of captchas when evaluating whether a request should
 * be allowed to continue.
 */
public class PushChallengeConfig {
  private static final Logger logger = LoggerFactory.getLogger(PushChallengeConfig.class);

  public static final String PROPERTY_NAME = "pushChallengePermitted";

  /**
   * A score threshold in the range [0, 1.0]
   */
  private final boolean pushPermitted;

  /**
   * Extract an optional score threshold parameter provided by an upstream request filter
   */
  public PushChallengeConfig(final ContainerRequest containerRequest) {
    this.pushPermitted = Optional
        .ofNullable(containerRequest.getProperty(PROPERTY_NAME))
        .filter(obj -> obj instanceof Boolean)
        .map(obj -> (Boolean) obj)
        .orElse(true);          // not a typo! true is the default
  }

  public boolean pushPermitted() {
    return pushPermitted;
  }
}
