package org.whispersystems.textsecuregcm.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.i18n.phonenumbers.PhoneNumberUtil;
import java.time.Clock;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;

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
      final Device primaryDevice = mock(Device.class);

      // The account does not have a phone number that can be connected to a region/time zone
      when(account.getNumber()).thenReturn("Not a parseable number");
      when(account.getPrimaryDevice()).thenReturn(primaryDevice);
      when(primaryDevice.getCreated())
          .thenReturn(ZonedDateTime.of(2024, 7, 10, 9, 53, 12, 0, ZoneId.systemDefault()).toInstant().toEpochMilli());

      final ZonedDateTime beforeNotificationTime = ZonedDateTime.now(ZoneId.systemDefault()).with(LocalTime.of(9, 0));

      assertEquals(
          beforeNotificationTime.with(LocalTime.of(9, 53, 12)).toInstant(),
          SchedulingUtil.getNextRecommendedNotificationTime(account, LocalTime.of(14, 0),
              Clock.fixed(beforeNotificationTime.toInstant(), ZoneId.systemDefault())));

      final ZonedDateTime afterNotificationTime = ZonedDateTime.now(ZoneId.systemDefault()).with(LocalTime.of(10, 0));

      assertEquals(
          afterNotificationTime.with(LocalTime.of(9, 53, 12)).plusDays(1).toInstant(),
          SchedulingUtil.getNextRecommendedNotificationTime(account, LocalTime.of(14, 0),
              Clock.fixed(afterNotificationTime.toInstant(), ZoneId.systemDefault())));
    }
  }
}
