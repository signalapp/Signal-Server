/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import com.google.i18n.phonenumbers.NumberParseException;
import java.util.List;

/// An alternate phone number form generator produces a list of equivalent phone numbers to the given phone number. This
/// is useful in cases where a numbering authority has changed the numbering format for a region or in cases where
/// multiple formats of a number may be valid in different circumstances. Numbers are considered equivalent if a
/// call/message sent to each number will generally arrive at the same device.
public interface AlternatePhoneNumberFormGenerator {

  AlternatePhoneNumberFormGenerator IDENTITY = List::of;

  /// Returns a list of equivalent phone numbers to the given phone number.
  ///
  /// @apiNote This method is intended to support number format transitions in cases where we do not already have
  /// multiple accounts registered with different forms of the same number. As a result, this method does not cover all
  /// possible cases of equivalent formats, but instead focuses on the cases where we can and choose to prevent multiple
  /// accounts from using different formats of the same number.
  ///
  /// @param number the e164-formatted phone number for which to find equivalent forms
  ///
  /// @return a list of phone numbers equivalent to the given phone number, including the given number. The given number
  /// will always be the first element of the list.
  ///
  /// @throws NumberParseException if `number` could not be parsed as a phone number for any reason
  List<String> getAlternateForms(String number) throws NumberParseException;
}
