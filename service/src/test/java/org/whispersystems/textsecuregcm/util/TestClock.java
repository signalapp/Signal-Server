package org.whispersystems.textsecuregcm.util;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Optional;

/**
 * Clock class specialized for testing.
 *
 * This clock can be pinned to a particular instant or can provide the "normal" time.
 *
 * Unlike normal clocks it can be dynamically pinned and unpinned to help with testing.
 * It should not be used in production.
 */
public class TestClock extends java.time.Clock {

  private Optional<Instant> pinnedInstant;
  private final ZoneId zoneId;

  private TestClock(Optional<Instant> maybePinned, ZoneId id) {
    this.pinnedInstant = maybePinned;
    this.zoneId = id;
  }

  /**
   * Instantiate a test clock that returns the "real" time.
   *
   * The clock can later be pinned to an instant if desired.
   *
   * @return
   */
  public static TestClock now() {
    return new TestClock(Optional.empty(), ZoneId.of("UTC"));
  }

  /**
   * Instantiate a test clock pinned to a particular instant.
   *
   * The clock can later be pinned to a different instant or unpinned if desired.
   *
   * Unlike the fixed constructor no time zone is required (it defaults to UTC).
   *
   * @param instant
   * @return test clock pinned to the given instant.
   */
  public static TestClock pinned(Instant instant) {
    return new TestClock(Optional.of(instant), ZoneId.of("UTC"));
  }

  /**
   * Pin this test clock to the given instance.
   *
   * This modifies the existing clock in-place.
   *
   * @param instant
   */
  public void pin(Instant instant) {
    this.pinnedInstant = Optional.of(instant);
  }

  /**
   * Unpin this test clock so it will being returning the "real" time.
   *
   * This modifies the existing clock in-place.
   */
  public void unpin() {
    this.pinnedInstant = Optional.empty();
  }


  public TestClock withZone(ZoneId id) {
    return new TestClock(pinnedInstant, id);
  }

  public ZoneId getZone() {
    return zoneId;
  }

  public Instant instant() {
    return pinnedInstant.orElseGet(Instant::now);
  }

  public long millis() {
    return instant().toEpochMilli();
  }

}
