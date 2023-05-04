/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class MutableClock extends Clock {

  private final AtomicReference<Clock> delegate;


  public MutableClock(final long timeMillis) {
    this(fixedTimeMillis(timeMillis));
  }

  public MutableClock(final Clock clock) {
    this.delegate = new AtomicReference<>(clock);
  }

  public MutableClock() {
    this(Clock.systemUTC());
  }

  public MutableClock setTimeInstant(final Instant instant) {
    delegate.set(Clock.fixed(instant, ZoneId.of("Etc/UTC")));
    return this;
  }

  public MutableClock setTimeMillis(final long timeMillis) {
    delegate.set(fixedTimeMillis(timeMillis));
    return this;
  }

  public MutableClock incrementMillis(final long incrementMillis) {
    return increment(incrementMillis, TimeUnit.MILLISECONDS);
  }

  public MutableClock incrementSeconds(final long incrementSeconds) {
    return increment(incrementSeconds, TimeUnit.SECONDS);
  }

  public MutableClock increment(final long increment, final TimeUnit timeUnit) {
    final long current = delegate.get().instant().toEpochMilli();
    delegate.set(fixedTimeMillis(current + timeUnit.toMillis(increment)));
    return this;
  }

  @Override
  public ZoneId getZone() {
    return delegate.get().getZone();
  }

  @Override
  public Clock withZone(final ZoneId zone) {
    return delegate.get().withZone(zone);
  }

  @Override
  public Instant instant() {
    return delegate.get().instant();
  }

  private static Clock fixedTimeMillis(final long timeMillis) {
    return Clock.fixed(Instant.ofEpochMilli(timeMillis), ZoneId.of("Etc/UTC"));
  }
}
