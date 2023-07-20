/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusException;
import java.time.Duration;
import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;

public class RateLimitUtil {

  public static final Metadata.Key<Duration> RETRY_AFTER_DURATION_KEY =
      Metadata.Key.of("retry-after", new Metadata.AsciiMarshaller<>() {
        @Override
        public String toAsciiString(final Duration value) {
          return value.toString();
        }

        @Override
        public Duration parseAsciiString(final String serialized) {
          return Duration.parse(serialized);
        }
      });

  public static Throwable mapRateLimitExceededException(final Throwable throwable) {
    if (throwable instanceof RateLimitExceededException rateLimitExceededException) {
      @Nullable final Metadata trailers = rateLimitExceededException.getRetryDuration()
          .map(duration -> {
            final Metadata metadata = new Metadata();
            metadata.put(RETRY_AFTER_DURATION_KEY, duration);

            return metadata;
          }).orElse(null);

      return new StatusException(Status.RESOURCE_EXHAUSTED, trailers);
    }

    return throwable;
  }
}
