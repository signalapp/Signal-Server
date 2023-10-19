/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.SecureRandom;
import java.util.Optional;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.storage.Account;

class UnidentifiedAccessUtilTest {

  @ParameterizedTest
  @MethodSource
  void checkUnidentifiedAccess(@Nullable final byte[] targetUak,
      final boolean unrestrictedUnidentifiedAccess,
      final byte[] presentedUak,
      final boolean expectAccessAllowed) {

    final Account account = mock(Account.class);
    when(account.getUnidentifiedAccessKey()).thenReturn(Optional.ofNullable(targetUak));
    when(account.isUnrestrictedUnidentifiedAccess()).thenReturn(unrestrictedUnidentifiedAccess);

    assertEquals(expectAccessAllowed, UnidentifiedAccessUtil.checkUnidentifiedAccess(account, presentedUak));
  }

  private static Stream<Arguments> checkUnidentifiedAccess() {
    final byte[] uak = new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH];
    new SecureRandom().nextBytes(uak);

    final byte[] incorrectUak = new byte[uak.length + 1];

    return Stream.of(
        Arguments.of(null, false, uak, false),
        Arguments.of(null, true, uak, true),
        Arguments.of(uak, false, incorrectUak, false),
        Arguments.of(uak, false, uak, true),
        Arguments.of(uak, true, incorrectUak, true),
        Arguments.of(uak, true, uak, true)
    );
  }
}
