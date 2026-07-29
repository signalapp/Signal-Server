/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import com.google.i18n.phonenumbers.PhoneNumberUtil;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.asn.AsnInfo;
import org.whispersystems.textsecuregcm.asn.AsnInfoProvider;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicPaymentsConfiguration;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.storage.VersionedProfile;
import org.whispersystems.textsecuregcm.storage.VersionedProfileV1;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ProfileHelperTest {

  private enum PhoneNumberType {
    ALLOWED,
    FORBIDDEN,
    NONE
  }

  private enum IpAddressType {
    ALLOWED,
    FORBIDDEN,
    UNRECOGNIZED
  }

  private enum ExistingPaymentAddressType {
    CURRENT_PROFILE,
    LEGACY_PROFILE,
    NONE
  }

  @ParameterizedTest
  @MethodSource
  void isPaymentAddressUpdateForbidden(final PhoneNumberType phoneNumberType,
      final IpAddressType ipAddressType,
      final ExistingPaymentAddressType existingPaymentAddressType,
      final boolean expectForbidden) {

    final DynamicConfiguration dynamicConfiguration = mock(DynamicConfiguration.class);
    when(dynamicConfiguration.getPaymentsConfiguration())
        .thenReturn(new DynamicPaymentsConfiguration(List.of("+1"), List.of("US")));

    @SuppressWarnings("unchecked") final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager =
        mock(DynamicConfigurationManager.class);

    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);

    final Account account = mock(Account.class);

    switch (phoneNumberType) {
      case ALLOWED -> when(account.getNumberOptional())
          .thenReturn(Optional.of(PhoneNumberUtil.getInstance().format(
              PhoneNumberUtil.getInstance().getExampleNumber("DE"), PhoneNumberUtil.PhoneNumberFormat.E164)));

      case FORBIDDEN -> when(account.getNumberOptional())
          .thenReturn(Optional.of(PhoneNumberUtil.getInstance().format(
              PhoneNumberUtil.getInstance().getExampleNumber("US"), PhoneNumberUtil.PhoneNumberFormat.E164)));

      case NONE -> when(account.getNumberOptional()).thenReturn(Optional.empty());
    }

    final AsnInfoProvider asnInfoProvider = mock(AsnInfoProvider.class);

    switch (ipAddressType) {
      case ALLOWED -> when(asnInfoProvider.lookup(anyString())).thenReturn(Optional.of(new AsnInfo(123, "DE")));
      case FORBIDDEN -> when(asnInfoProvider.lookup(anyString())).thenReturn(Optional.of(new AsnInfo(123, "US")));
      case UNRECOGNIZED -> when(asnInfoProvider.lookup(anyString())).thenReturn(Optional.empty());
    }

    final Optional<VersionedProfile> maybeProfile = switch (existingPaymentAddressType) {
      case CURRENT_PROFILE -> Optional.of(new VersionedProfile(TestRandomUtil.nextBytes(16),
          TestRandomUtil.nextBytes(16),
          TestRandomUtil.nextBytes(16),
          TestRandomUtil.nextBytes(16)));
      case LEGACY_PROFILE, NONE -> Optional.empty();
    };

    final Optional<VersionedProfileV1> maybeLegacyProfile = switch (existingPaymentAddressType) {
      case LEGACY_PROFILE -> Optional.of(new VersionedProfileV1("version",
          TestRandomUtil.nextBytes(16),
          "avatar",
          TestRandomUtil.nextBytes(16),
          TestRandomUtil.nextBytes(16),
          TestRandomUtil.nextBytes(16),
          TestRandomUtil.nextBytes(16),
          TestRandomUtil.nextBytes(16)));
      case CURRENT_PROFILE, NONE -> Optional.empty();
    };

    assertEquals(expectForbidden, ProfileHelper.isPaymentAddressUpdateForbidden(account,
        maybeProfile,
        maybeLegacyProfile,
        "127.0.0.1",
        asnInfoProvider,
        dynamicConfigurationManager));
  }

  private static List<Arguments> isPaymentAddressUpdateForbidden() {
    return List.of(
        Arguments.argumentSet("Permitted phone number",
            PhoneNumberType.ALLOWED, IpAddressType.ALLOWED, ExistingPaymentAddressType.NONE, false),

        Arguments.argumentSet("Forbidden phone number",
            PhoneNumberType.FORBIDDEN, IpAddressType.ALLOWED, ExistingPaymentAddressType.NONE, true),

        Arguments.argumentSet("Forbidden phone number, has existing address",
            PhoneNumberType.FORBIDDEN, IpAddressType.ALLOWED, ExistingPaymentAddressType.CURRENT_PROFILE, false),

        Arguments.argumentSet("Forbidden phone number, has existing address in legacy profile",
            PhoneNumberType.FORBIDDEN, IpAddressType.ALLOWED, ExistingPaymentAddressType.LEGACY_PROFILE, false),

        Arguments.argumentSet("No phone number, permitted ASN region",
            PhoneNumberType.NONE, IpAddressType.ALLOWED, ExistingPaymentAddressType.NONE, false),

        Arguments.argumentSet("No phone number, unrecognized ASN region",
            PhoneNumberType.NONE, IpAddressType.UNRECOGNIZED, ExistingPaymentAddressType.NONE, false),

        Arguments.argumentSet("No phone number, forbidden ASN region",
            PhoneNumberType.NONE, IpAddressType.FORBIDDEN, ExistingPaymentAddressType.NONE, true),

        Arguments.argumentSet("No phone number, forbidden ASN region, has existing address",
            PhoneNumberType.NONE, IpAddressType.FORBIDDEN, ExistingPaymentAddressType.CURRENT_PROFILE, false),

        Arguments.argumentSet("No phone number, forbidden ASN region, has existing address in legacy profile",
            PhoneNumberType.NONE, IpAddressType.FORBIDDEN, ExistingPaymentAddressType.LEGACY_PROFILE, false)
    );
  }
}
