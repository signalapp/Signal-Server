/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.telephony;

import com.google.i18n.phonenumbers.Phonenumber;
import java.io.IOException;
import java.time.Duration;
import java.util.Optional;

/// A carrier data provider returns line type and home network information about a specific phone number. Carrier data
/// providers may cache results and return cached results if they are newer than a given maximum age.
public interface CarrierDataProvider {

  /// Retrieves carrier data for a given phone number.
  ///
  /// @param phoneNumber the phone number for which to retrieve line type and home network information
  /// @param maxCachedAge the maximum age of a cached response to return; providers must attempt to fetch fresh data if
  /// cached data is older than the given maximum age, and may choose to fetch fresh data under any circumstances
  ///
  /// @return line type and home network information for the given phone number if available or empty if this provider
  /// could not find information for the given phone number
  ///
  /// @throws IOException if the provider could not be reached due to a network problem of any kind
  /// @throws CarrierDataException if the request failed and should not be retried without modification
  Optional<CarrierData> lookupCarrierData(Phonenumber.PhoneNumber phoneNumber, Duration maxCachedAge)
      throws IOException, CarrierDataException;
}
