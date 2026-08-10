/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Base64;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class AccountChangeValidatorTest {

  private static final String ORIGINAL_NUMBER = PhoneNumberUtil.getInstance().format(
      PhoneNumberUtil.getInstance().getExampleNumber("US"), PhoneNumberUtil.PhoneNumberFormat.E164);

  private static final String CHANGED_NUMBER = PhoneNumberUtil.getInstance().format(
      PhoneNumberUtil.getInstance().getExampleNumber("FR"), PhoneNumberUtil.PhoneNumberFormat.E164);

  private static final UUID ORIGINAL_PNI = UUID.randomUUID();
  private static final UUID CHANGED_PNI = UUID.randomUUID();

  private static final String BASE_64_URL_ORIGINAL_USERNAME = "9p6Tip7BFefFOJzv4kv4GyXEYsBVfk_WbjNejdlOvQE";
  private static final String BASE_64_URL_CHANGED_USERNAME = "NLUom-CHwtemcdvOTTXdmXmzRIV7F05leS8lwkVK_vc";
  private static final byte[] ORIGINAL_USERNAME_HASH = Base64.getUrlDecoder().decode(BASE_64_URL_ORIGINAL_USERNAME);
  private static final byte[] CHANGED_USERNAME_HASH = Base64.getUrlDecoder().decode(BASE_64_URL_CHANGED_USERNAME);

  @ParameterizedTest
  @MethodSource
  void validateChange(final Account originalAccount,
      final Account updatedAccount,
      final AccountChangeValidator changeValidator,
      final boolean expectChangeAllowed) {

    final Executable applyChange = () -> changeValidator.validateChange(originalAccount, updatedAccount);

    if (expectChangeAllowed) {
      assertDoesNotThrow(applyChange);
    } else {
      assertThrows(AssertionError.class, applyChange);
    }
  }

  private static Stream<Arguments> validateChange() {
    final Account originalAccount = mock(Account.class);
    when(originalAccount.getNumberOptional()).thenReturn(Optional.of(ORIGINAL_NUMBER));
    when(originalAccount.getPhoneNumberIdentifierOptional()).thenReturn(Optional.of(ORIGINAL_PNI));
    when(originalAccount.getUsernameHash()).thenReturn(Optional.of(ORIGINAL_USERNAME_HASH));

    final Account unchangedAccount = mock(Account.class);
    when(unchangedAccount.getNumberOptional()).thenReturn(Optional.of(ORIGINAL_NUMBER));
    when(unchangedAccount.getPhoneNumberIdentifierOptional()).thenReturn(Optional.of(ORIGINAL_PNI));
    when(unchangedAccount.getUsernameHash()).thenReturn(Optional.of(ORIGINAL_USERNAME_HASH));

    final Account changedNumberAccount = mock(Account.class);
    when(changedNumberAccount.getNumberOptional()).thenReturn(Optional.of(CHANGED_NUMBER));
    when(changedNumberAccount.getPhoneNumberIdentifierOptional()).thenReturn(Optional.of(CHANGED_PNI));
    when(changedNumberAccount.getUsernameHash()).thenReturn(Optional.of(ORIGINAL_USERNAME_HASH));

    final Account changedUsernameAccount = mock(Account.class);
    when(changedUsernameAccount.getNumberOptional()).thenReturn(Optional.of(ORIGINAL_NUMBER));
    when(changedUsernameAccount.getPhoneNumberIdentifierOptional()).thenReturn(Optional.of(ORIGINAL_PNI));
    when(changedUsernameAccount.getUsernameHash()).thenReturn(Optional.of(CHANGED_USERNAME_HASH));

    final Account numberlessAccount = mock(Account.class);
    when(numberlessAccount.getNumberOptional()).thenReturn(Optional.empty());
    when(numberlessAccount.getPhoneNumberIdentifierOptional()).thenReturn(Optional.empty());
    when(numberlessAccount.getUsernameHash()).thenReturn(Optional.of(ORIGINAL_USERNAME_HASH));

    return Stream.of(
        Arguments.of(originalAccount, unchangedAccount, AccountChangeValidator.GENERAL_CHANGE_VALIDATOR, true),
        Arguments.of(originalAccount, unchangedAccount, AccountChangeValidator.NUMBER_CHANGE_VALIDATOR, true),
        Arguments.of(originalAccount, unchangedAccount, AccountChangeValidator.USERNAME_CHANGE_VALIDATOR, true),

        Arguments.of(originalAccount, changedNumberAccount, AccountChangeValidator.GENERAL_CHANGE_VALIDATOR, false),
        Arguments.of(originalAccount, changedNumberAccount, AccountChangeValidator.NUMBER_CHANGE_VALIDATOR, true),
        Arguments.of(originalAccount, changedNumberAccount, AccountChangeValidator.USERNAME_CHANGE_VALIDATOR, false),

        Arguments.of(originalAccount, changedUsernameAccount, AccountChangeValidator.GENERAL_CHANGE_VALIDATOR, false),
        Arguments.of(originalAccount, changedUsernameAccount, AccountChangeValidator.NUMBER_CHANGE_VALIDATOR, false),
        Arguments.of(originalAccount, changedUsernameAccount, AccountChangeValidator.USERNAME_CHANGE_VALIDATOR, true),

        Arguments.of(originalAccount, numberlessAccount, AccountChangeValidator.GENERAL_CHANGE_VALIDATOR, false),
        Arguments.of(originalAccount, numberlessAccount, AccountChangeValidator.NUMBER_CHANGE_VALIDATOR, true),
        Arguments.of(originalAccount, numberlessAccount, AccountChangeValidator.USERNAME_CHANGE_VALIDATOR, false),

        Arguments.of(numberlessAccount, originalAccount, AccountChangeValidator.GENERAL_CHANGE_VALIDATOR, false),
        Arguments.of(numberlessAccount, originalAccount, AccountChangeValidator.NUMBER_CHANGE_VALIDATOR, true),
        Arguments.of(numberlessAccount, originalAccount, AccountChangeValidator.USERNAME_CHANGE_VALIDATOR, false)
    );
  }
}
