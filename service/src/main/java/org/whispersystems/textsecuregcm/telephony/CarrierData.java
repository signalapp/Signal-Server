/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.telephony;

import java.util.Optional;

/// Line type and home network information for a specific phone number.
///
/// @param carrierName the name of the network operator for the specified phone number
/// @param lineType the line type for the specified phone number
/// @param mcc the mobile country code (MCC) of the phone number's home network if known; may be empty if the phone
/// number is not a mobile number
/// @param mnc the mobile network code (MNC) of the phone number's home network if known; may be empty if the phone
/// number is not a mobile number
public record CarrierData(String carrierName, LineType lineType, Optional<String> mcc, Optional<String> mnc) {

  public enum LineType {
    MOBILE,
    LANDLINE,
    FIXED_VOIP,
    NON_FIXED_VOIP,
    OTHER,
    UNKNOWN
  }
}
