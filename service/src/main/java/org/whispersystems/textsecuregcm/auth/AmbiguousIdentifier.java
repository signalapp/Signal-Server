/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import javax.annotation.Nullable;
import java.util.UUID;

import static com.codahale.metrics.MetricRegistry.name;

public class AmbiguousIdentifier {

  private final UUID   uuid;
  private final String number;

  private static final String REQUEST_COUNTER_NAME = name(AmbiguousIdentifier.class, "request");

  public AmbiguousIdentifier(String target) {
    if (target.startsWith("+")) {
      this.uuid   = null;
      this.number = target;
    } else {
      this.uuid   = UUID.fromString(target);
      this.number = null;
    }
  }

  public UUID getUuid() {
    return uuid;
  }

  public String getNumber() {
    return number;
  }

  public boolean hasUuid() {
    return uuid != null;
  }

  public boolean hasNumber() {
    return number != null;
  }

  @Override
  public String toString() {
    return hasUuid() ? uuid.toString() : number;
  }

  public void incrementRequestCounter(final String context, @Nullable final String userAgent) {
    Metrics.counter(REQUEST_COUNTER_NAME, Tags.of(
        Tag.of("type", hasUuid() ? "uuid" : "e164"),
        Tag.of("context", context),
        UserAgentTagUtil.getPlatformTag(userAgent))).increment();
  }
}
