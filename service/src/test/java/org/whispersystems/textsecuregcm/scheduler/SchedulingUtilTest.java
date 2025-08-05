package org.whispersystems.textsecuregcm.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.i18n.phonenumbers.PhoneNumberUtil;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import com.google.i18n.phonenumbers.Phonenumber;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.util.TestClock;

class SchedulingUtilTest {

  @Test
  void getNextRecommendedNotificationTime() {
    {
      final Account account = mock(Account.class);

      // The account has a phone number that can be resolved to a region with known timezones
      when(account.getNumber()).thenReturn(PhoneNumberUtil.getInstance().format(
          PhoneNumberUtil.getInstance().getExampleNumber("DE"), PhoneNumberUtil.PhoneNumberFormat.E164));

      final ZoneId berlinZoneId = ZoneId.of("Europe/Berlin");
      final ZonedDateTime beforeNotificationTime = ZonedDateTime.now(berlinZoneId).with(LocalTime.of(13, 0));

      assertEquals(
          beforeNotificationTime.with(LocalTime.of(14, 0)).toInstant(),
          SchedulingUtil.getNextRecommendedNotificationTime(account, LocalTime.of(14, 0),
              Clock.fixed(beforeNotificationTime.toInstant(), berlinZoneId)));

      final ZonedDateTime afterNotificationTime = ZonedDateTime.now(berlinZoneId).with(LocalTime.of(15, 0));

      assertEquals(
          afterNotificationTime.with(LocalTime.of(14, 0)).plusDays(1).toInstant(),
          SchedulingUtil.getNextRecommendedNotificationTime(account, LocalTime.of(14, 0),
              Clock.fixed(afterNotificationTime.toInstant(), berlinZoneId)));
    }

    {
      final Account account = mock(Account.class);

      // The account does not have a phone number that can be connected to a region/time zone
      when(account.getNumber()).thenReturn("Not a parseable number");

      final ZonedDateTime beforeNotificationTime = ZonedDateTime.now(ZoneId.systemDefault()).with(LocalTime.of(13, 59));
      final LocalTime preferredNotificationTime = LocalTime.of(14, 0);

      assertEquals(
          beforeNotificationTime.with(preferredNotificationTime).toInstant(),
          SchedulingUtil.getNextRecommendedNotificationTime(account, preferredNotificationTime,
              Clock.fixed(beforeNotificationTime.toInstant(), ZoneId.systemDefault())));

      final ZonedDateTime afterNotificationTime = ZonedDateTime.now(ZoneId.systemDefault()).with(LocalTime.of(14, 1));

      assertEquals(
          afterNotificationTime.with(preferredNotificationTime).plusDays(1).toInstant(),
          SchedulingUtil.getNextRecommendedNotificationTime(account, preferredNotificationTime,
              Clock.fixed(afterNotificationTime.toInstant(), ZoneId.systemDefault())));
    }
  }

  @Test
  void getNextRecommendedNotificationTimeDaylightSavings() {
    final Account account = mock(Account.class);

    // The account has a phone number that can be resolved to a region with known timezones
    when(account.getNumber()).thenReturn(PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("DE"), PhoneNumberUtil.PhoneNumberFormat.E164));

    final LocalDateTime afterNotificationTime = LocalDateTime.of(2025, 3, 29, 15, 0);
    final ZoneId berlinZoneId = ZoneId.of("Europe/Berlin");
    final ZoneOffset berlineZoneOffset = berlinZoneId.getRules().getOffset(afterNotificationTime);

    // Daylight Savings Time started on 2025-03-30 at 2:00AM in Germany.
    // Instantiating a ZonedDateTime with a zone ID factors in daylight savings when we adjust the time.
    final ZonedDateTime afterNotificationTimeWithZoneId = ZonedDateTime.of(afterNotificationTime, berlinZoneId);
    
    assertEquals(
        afterNotificationTimeWithZoneId.with(LocalTime.of(14, 0)).plusDays(1).toInstant(),
        SchedulingUtil.getNextRecommendedNotificationTime(account, LocalTime.of(14, 0),
            Clock.fixed(afterNotificationTime.toInstant(berlineZoneOffset), berlinZoneId)));
  }

  @Test
  void zoneIdSelectionSingleOffset() {
    final Account account = mock(Account.class);
    final Phonenumber.PhoneNumber phoneNumber = PhoneNumberUtil.getInstance().getExampleNumber("DE");

    when(account.getNumber())
        .thenReturn(PhoneNumberUtil.getInstance().format(phoneNumber , PhoneNumberUtil.PhoneNumberFormat.E164));

    final Instant now = Instant.now();

    assertEquals(
        ZoneId.of("Europe/Berlin").getRules().getOffset(now),
        SchedulingUtil.getZoneId(account, TestClock.pinned(now)).orElseThrow().getRules().getOffset(now));
  }

  @Test
  void zoneIdSelectionMultipleOffsets() {
    final Account account = mock(Account.class);

    // A US VOIP number spans multiple time zones, we should pick a 'middle' one
    final Phonenumber.PhoneNumber phoneNumber =
        PhoneNumberUtil.getInstance().getExampleNumberForType("US", PhoneNumberUtil.PhoneNumberType.VOIP);
    when(account.getNumber())
        .thenReturn(PhoneNumberUtil.getInstance().format(phoneNumber, PhoneNumberUtil.PhoneNumberFormat.E164));

    final Instant now = Instant.now();

    assertEquals(
        ZoneId.of("America/Chicago").getRules().getOffset(now),
        SchedulingUtil.getZoneId(account, TestClock.pinned(now)).orElseThrow().getRules().getOffset(now));
  }
}
