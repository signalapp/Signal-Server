/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.util;

import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.Phonenumber;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.support.ParameterDeclarations;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.auth.SaltedTokenHash;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountBadge;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.Util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AccountsTestHelper {

  public static class AccountsDataReportArgumentProvider implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(final ParameterDeclarations parameters,
        final ExtensionContext context) {

      final String exampleNumber1 = toE164(PhoneNumberUtil.getInstance().getExampleNumber("ES"));
      final String account2PhoneNumber = toE164(PhoneNumberUtil.getInstance().getExampleNumber("AU"));
      final String account3PhoneNumber = toE164(PhoneNumberUtil.getInstance().getExampleNumber("IN"));

      final Instant account1Device1Created = Instant.ofEpochSecond(1669323142); // 2022-11-24T20:52:22Z
      final Instant account1Device2Created = Instant.ofEpochSecond(1679155122); // 2023-03-18T15:58:42Z
      final Instant account1Device1LastSeen = Instant.ofEpochMilli(Util.todayInMillis());
      final Instant account1Device2LastSeen = Instant.ofEpochSecond(1678838400); // 2023-03-15T00:00:00Z

      final Instant account2Device1Created = Instant.ofEpochSecond(1659123001); // 2022-07-29T19:30:01Z
      final Instant account2Device1LastSeen = Instant.ofEpochMilli(Util.todayInMillis());
      final Instant badgeAExpiration = Instant.now().plus(Duration.ofDays(21)).truncatedTo(ChronoUnit.SECONDS);

      final Instant account3Device1Created = Instant.ofEpochSecond(1639923487); // 2021-12-19T14:18:07Z
      final Instant account3Device1LastSeen = Instant.ofEpochMilli(Util.todayInMillis());
      final Instant badgeBExpiration = Instant.now().plus(Duration.ofDays(21)).truncatedTo(ChronoUnit.SECONDS);
      final Instant badgeCExpiration = Instant.now().plus(Duration.ofDays(24)).truncatedTo(ChronoUnit.SECONDS);

      return Stream.of(
          Arguments.of(
              buildTestAccountForDataReport(UUID.randomUUID(), exampleNumber1,
                  true, true,
                  Collections.emptyList(),
                  List.of(new DeviceData(Device.PRIMARY_ID, account1Device1LastSeen, account1Device1Created, null),
                      new DeviceData((byte) 2, account1Device2LastSeen, account1Device2Created, "OWP"))),
              String.format("""
                      # Account
                      Phone number: %s
                      Allow sealed sender from anyone: true
                      Find account by phone number: true
                      Badges: None
                      
                      # Devices
                      - ID: 1
                        Created: 2022-11-24T20:52:22Z
                        Last seen: %s
                        User-agent: null
                      - ID: 2
                        Created: 2023-03-18T15:58:42Z
                        Last seen: 2023-03-15T00:00:00Z
                        User-agent: OWP
                      """,
                  exampleNumber1,
                  account1Device1LastSeen)
          ),
          Arguments.of(
              buildTestAccountForDataReport(UUID.randomUUID(), account2PhoneNumber,
                  false, true,
                  List.of(new AccountBadge("badge_a", badgeAExpiration, true)),
                  List.of(new DeviceData(Device.PRIMARY_ID, account2Device1LastSeen, account2Device1Created, "OWI"))),
              String.format("""
                      # Account
                      Phone number: %s
                      Allow sealed sender from anyone: false
                      Find account by phone number: true
                      Badges:
                      - ID: badge_a
                        Expiration: %s
                        Visible: true
                      
                      # Devices
                      - ID: 1
                        Created: 2022-07-29T19:30:01Z
                        Last seen: %s
                        User-agent: OWI
                      """, account2PhoneNumber,
                  badgeAExpiration,
                  account2Device1LastSeen)
          ),
          Arguments.of(
              buildTestAccountForDataReport(UUID.randomUUID(), account3PhoneNumber,
                  true, false,
                  List.of(
                      new AccountBadge("badge_b", badgeBExpiration, true),
                      new AccountBadge("badge_c", badgeCExpiration, false)),
                  List.of(new DeviceData(Device.PRIMARY_ID, account3Device1LastSeen, account3Device1Created, "OWA"))),
              String.format("""
                      # Account
                      Phone number: %s
                      Allow sealed sender from anyone: true
                      Find account by phone number: false
                      Badges:
                      - ID: badge_b
                        Expiration: %s
                        Visible: true
                      - ID: badge_c
                        Expiration: %s
                        Visible: false
                      
                      # Devices
                      - ID: 1
                        Created: 2021-12-19T14:18:07Z
                        Last seen: %s
                        User-agent: OWA
                      """, account3PhoneNumber,
                  badgeBExpiration,
                  badgeCExpiration,
                  account3Device1LastSeen)
          )
      );
    }

    public static Account buildTestAccountForDataReport(final UUID aci, final String number,
        final boolean unrestrictedUnidentifiedAccess, final boolean discoverableByPhoneNumber,
        final List<AccountBadge> badges, final List<DeviceData> devices) {

      final ECKeyPair aciIdentityKeyPair = ECKeyPair.generate();
      final ECKeyPair pniIdentityKeyPair = ECKeyPair.generate();

      final Account account = new Account();
      account.setUuid(aci);
      account.setNumber(number, UUID.randomUUID());
      account.setUnrestrictedUnidentifiedAccess(unrestrictedUnidentifiedAccess);
      account.setDiscoverableByPhoneNumber(discoverableByPhoneNumber);
      account.setBadges(Clock.systemUTC(), new ArrayList<>(badges));
      account.setIdentityKey(new IdentityKey(aciIdentityKeyPair.getPublicKey()));
      account.setPhoneNumberIdentityKey(new IdentityKey(pniIdentityKeyPair.getPublicKey()));

      assert !devices.isEmpty();

      final SaltedTokenHash passwordTokenHash = SaltedTokenHash.generateFor("password");

      devices.forEach(deviceData -> {
        final Device device = new Device();
        device.setId(deviceData.id());
        device.setAuthTokenHash(passwordTokenHash);
        device.setFetchesMessages(true);
        device.setLastSeen(deviceData.lastSeen().toEpochMilli());
        device.setCreated(deviceData.created().toEpochMilli());
        device.setUserAgent(deviceData.userAgent());
        account.addDevice(device);
      });

      return account;
    }

    private static String toE164(final Phonenumber.PhoneNumber phoneNumber) {
      return PhoneNumberUtil.getInstance().format(phoneNumber, PhoneNumberUtil.PhoneNumberFormat.E164);
    }

    public record DeviceData(byte id, Instant lastSeen, Instant created, @Nullable String userAgent) {
    }
  }

  public static void verifyAccountDataReportText(final String actualText, final String expectedTextAfterHeader) {
    final int headerEnd = actualText.indexOf("# Account");
    assertEquals(expectedTextAfterHeader, actualText.substring(headerEnd));

    final String actualHeader = actualText.substring(0, headerEnd);
    assertTrue(actualHeader.matches(
        "Report ID: [a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}\nReport timestamp: \\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z\n\n"));

  }
}
