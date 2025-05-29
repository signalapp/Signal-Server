package org.whispersystems.textsecuregcm.scheduler;

import com.google.common.annotations.VisibleForTesting;
import com.google.i18n.phonenumbers.NumberParseException;
import com.google.i18n.phonenumbers.PhoneNumberToTimeZonesMapper;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.Phonenumber;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.zone.ZoneRules;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.whispersystems.textsecuregcm.storage.Account;

public class SchedulingUtil {

  /**
   * Gets a present or future time at which to send a notification to a device associated with the given account. This
   * is mainly intended to facilitate scheduling notifications such that they arrive during a recipient's waking hours.
   * <p/>
   * This method will attempt to use a timezone derived from the account's phone number to choose an appropriate time
   * to send a notification. If a timezone cannot be derived from the account's phone number, then this method will use
   * the account's creation time as a hint. As an example, if the account was created at 07:17 in the server's local
   * time, we can assume that the account holder was awake at that time of day, and it's likely a safe time to send a
   * notification in the absence of other hints.
   *
   * @param account the account that will receive the notification
   * @param preferredTime the preferred local time (e.g. "noon") at which to deliver the notification
   * @param clock a source of the current time
   *
   * @return the next time in the present or future at which to send a notification for the target account
   */
  public static Instant getNextRecommendedNotificationTime(final Account account,
      final LocalTime preferredTime,
      final Clock clock) {

    final ZonedDateTime candidateNotificationTime = getZoneId(account, clock)
        .map(zoneId -> ZonedDateTime.now(clock.withZone(zoneId)).with(preferredTime))
        .orElseGet(() -> {
          // We couldn't find a reasonable timezone for the account for some reason, so make an educated guess at a
          // reasonable time to send a notification based on the account's creation time.
          final Instant accountCreation = Instant.ofEpochMilli(account.getPrimaryDevice().getCreated());
          final LocalTime accountCreationLocalTime = LocalTime.ofInstant(accountCreation, ZoneId.systemDefault());

          return ZonedDateTime.now(ZoneId.systemDefault()).with(accountCreationLocalTime);
        });

    if (candidateNotificationTime.toInstant().isBefore(clock.instant())) {
      // We've missed our opportunity today, so go for the same time tomorrow
      return candidateNotificationTime.plusDays(1).toInstant();
    } else {
      // The best time to send a notification hasn't happened yet today
      return candidateNotificationTime.toInstant();
    }
  }

  @VisibleForTesting
  static Optional<ZoneId> getZoneId(final Account account, final Clock clock) {
    try {
      final Phonenumber.PhoneNumber phoneNumber = PhoneNumberUtil.getInstance().parse(account.getNumber(), null);

      final List<String> timeZonesForNumber =
          PhoneNumberToTimeZonesMapper.getInstance().getTimeZonesForNumber(phoneNumber);

      if (timeZonesForNumber.equals(List.of(PhoneNumberToTimeZonesMapper.getUnknownTimeZone()))) {
        return Optional.empty();
      }

      final Instant now = clock.instant();

      // Consider each unique ZoneRules and pick an arbitrary representative ZoneId for it
      final Map<ZoneRules, ZoneId> byOffset = timeZonesForNumber
          .stream()
          .map(id -> {
            try {
              return ZoneId.of(id);
            } catch (final Exception e) {
              return null;
            }
          })
          .filter(Objects::nonNull)
          .collect(Collectors.toMap(
              ZoneId::getRules,
              Function.identity(),
              (v1, v2) -> v2));

      // Sort the ZoneRules by the offsets they produce
      final List<ZoneRules> zoneRulesSortedByOffset = byOffset.keySet()
          .stream()
          .sorted(Comparator.comparing(z -> z.getOffset(now)))
          .toList();

      // Select the "middle" ZoneRule and return one of the ZoneIds that have that ZoneRule
      return Optional.of(byOffset.get(zoneRulesSortedByOffset.get(zoneRulesSortedByOffset.size() / 2)));
    } catch (final NumberParseException e) {
      return Optional.empty();
    }
  }
}
