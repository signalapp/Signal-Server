/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.telephony.hlrlookup;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.Phonenumber;
import org.apache.commons.lang3.StringUtils;
import javax.annotation.Nullable;
import java.time.Duration;

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
record TelephoneNumberRequest(String telephoneNumber,
                              @Nullable Integer cacheDaysGlobal,
                              @Nullable Integer cacheDaysPrivate,
                              @JsonProperty("save_to_cache") String saveToGlobalCache) {

  static TelephoneNumberRequest forPhoneNumber(final Phonenumber.PhoneNumber phoneNumber, final Duration maxCachedAge) {

    return new TelephoneNumberRequest(
        StringUtils.stripStart(PhoneNumberUtil.getInstance().format(phoneNumber, PhoneNumberUtil.PhoneNumberFormat.E164), "+"),
        (int) maxCachedAge.toDays(),
        (int) maxCachedAge.toDays(),
        "NO");
  }
}
