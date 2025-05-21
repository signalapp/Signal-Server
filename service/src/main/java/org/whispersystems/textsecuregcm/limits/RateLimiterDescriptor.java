/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.limits;

/**
 * Represents an information that defines a rate limiter.
 */
public interface RateLimiterDescriptor {
  /**
   * Implementing classes will likely be Enums, so name is chosen not to clash with {@link Enum#name()}.
   * @return id of this rate limiter to be used in `yml` config files and as a part of the bucket key.
   */
  String id();

  /**
   * @return an instance of {@link RateLimiterConfig} to be used by default,
   *         i.e. if there is no override in the application dynamic configuration.
   */
  RateLimiterConfig defaultConfig();
}
