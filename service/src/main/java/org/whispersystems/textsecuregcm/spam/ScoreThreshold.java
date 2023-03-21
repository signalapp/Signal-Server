/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.spam;

import org.glassfish.jersey.server.ContainerRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Optional;

/**
 * A ScoreThreshold may be provided by an upstream request filter. If request contains a property for
 * SCORE_THRESHOLD_PROPERTY_NAME it can be forwarded to a downstream filter to indicate it can use
 * a more or less strict score threshold when evaluating whether a request should be allowed to continue.
 */
public class ScoreThreshold {
  private static final Logger logger = LoggerFactory.getLogger(ScoreThreshold.class);

  public static final String PROPERTY_NAME = "scoreThreshold";

  /**
   * A score threshold in the range [0, 1.0]
   */
  private final Optional<Float> scoreThreshold;

  /**
   * Extract an optional score threshold parameter provided by an upstream request filter
   */
  public ScoreThreshold(final ContainerRequest containerRequest) {
    this.scoreThreshold = Optional
        .ofNullable(containerRequest.getProperty(PROPERTY_NAME))
        .flatMap(obj -> {
          if (obj instanceof Float f) {
            return Optional.of(f);
          }
          logger.warn("invalid format for filter provided score threshold {}", obj);
          return Optional.empty();
        });
  }

  public Optional<Float> getScoreThreshold() {
    return this.scoreThreshold;
  }
}
