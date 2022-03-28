/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class AccountChangeValidatorTest {

  private static final String ORIGINAL_NUMBER = "+18005551234";
  private static final String CHANGED_NUMBER = "+18005559876";

  private static final UUID ORIGINAL_PNI = UUID.randomUUID();
  private static final UUID CHANGED_PNI = UUID.randomUUID();

  private static final String ORIGINAL_USERNAME = "bruce_wayne";
  private static final String CHANGED_USERNAME = "batman";

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
    when(originalAccount.getNumber()).thenReturn(ORIGINAL_NUMBER);
    when(originalAccount.getPhoneNumberIdentifier()).thenReturn(ORIGINAL_PNI);
    when(originalAccount.getUsername()).thenReturn(Optional.of(ORIGINAL_USERNAME));

    final Account unchangedAccount = mock(Account.class);
    when(unchangedAccount.getNumber()).thenReturn(ORIGINAL_NUMBER);
    when(unchangedAccount.getPhoneNumberIdentifier()).thenReturn(ORIGINAL_PNI);
    when(unchangedAccount.getUsername()).thenReturn(Optional.of(ORIGINAL_USERNAME));

    final Account changedNumberAccount = mock(Account.class);
    when(changedNumberAccount.getNumber()).thenReturn(CHANGED_NUMBER);
    when(changedNumberAccount.getPhoneNumberIdentifier()).thenReturn(CHANGED_PNI);
    when(changedNumberAccount.getUsername()).thenReturn(Optional.of(ORIGINAL_USERNAME));

    final Account changedUsernameAccount = mock(Account.class);
    when(changedUsernameAccount.getNumber()).thenReturn(ORIGINAL_NUMBER);
    when(changedUsernameAccount.getPhoneNumberIdentifier()).thenReturn(ORIGINAL_PNI);
    when(changedUsernameAccount.getUsername()).thenReturn(Optional.of(CHANGED_USERNAME));

    return Stream.of(
        Arguments.of(originalAccount, unchangedAccount, AccountChangeValidator.GENERAL_CHANGE_VALIDATOR, true),
        Arguments.of(originalAccount, unchangedAccount, AccountChangeValidator.NUMBER_CHANGE_VALIDATOR, true),
        Arguments.of(originalAccount, unchangedAccount, AccountChangeValidator.USERNAME_CHANGE_VALIDATOR, true),

        Arguments.of(originalAccount, changedNumberAccount, AccountChangeValidator.GENERAL_CHANGE_VALIDATOR, false),
        Arguments.of(originalAccount, changedNumberAccount, AccountChangeValidator.NUMBER_CHANGE_VALIDATOR, true),
        Arguments.of(originalAccount, changedNumberAccount, AccountChangeValidator.USERNAME_CHANGE_VALIDATOR, false),

        Arguments.of(originalAccount, changedUsernameAccount, AccountChangeValidator.GENERAL_CHANGE_VALIDATOR, false),
        Arguments.of(originalAccount, changedUsernameAccount, AccountChangeValidator.NUMBER_CHANGE_VALIDATOR, false),
        Arguments.of(originalAccount, changedUsernameAccount, AccountChangeValidator.USERNAME_CHANGE_VALIDATOR, true)
    );
  }
}
