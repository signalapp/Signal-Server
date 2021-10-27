/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

class AssignPhoneNumberIdentifierCrawlerListenerTest {

  @Test
  void onCrawlChunk() {
    final UUID accountIdentifierWithPni = UUID.randomUUID();
    final UUID accountIdentifierWithoutPni = UUID.randomUUID();

    final String numberWithPni = "+18005551111";
    final String numberWithoutPni = "+18005552222";

    final Account accountWithPni = mock(Account.class);
    when(accountWithPni.getUuid()).thenReturn(accountIdentifierWithPni);
    when(accountWithPni.getNumber()).thenReturn(numberWithPni);
    when(accountWithPni.getPhoneNumberIdentifier()).thenReturn(Optional.of(UUID.randomUUID()));

    final Account accountWithoutPni = mock(Account.class);
    when(accountWithoutPni.getUuid()).thenReturn(accountIdentifierWithoutPni);
    when(accountWithoutPni.getNumber()).thenReturn(numberWithoutPni);
    when(accountWithoutPni.getPhoneNumberIdentifier()).thenReturn(Optional.empty());

    final AccountsManager accountsManager = mock(AccountsManager.class);
    when(accountsManager.getByAccountIdentifier(accountIdentifierWithPni)).thenReturn(Optional.of(accountWithPni));
    when(accountsManager.getByAccountIdentifier(accountIdentifierWithoutPni)).thenReturn(Optional.of(accountWithoutPni));

    when(accountsManager.update(any(), any())).thenAnswer((Answer<Account>) invocation -> {
      final Account account = invocation.getArgument(0, Account.class);
      final Consumer<Account> updater = invocation.getArgument(1, Consumer.class);

      updater.accept(account);

      return account;
    });

    final PhoneNumberIdentifiers phoneNumberIdentifiers = mock(PhoneNumberIdentifiers.class);
    when(phoneNumberIdentifiers.getPhoneNumberIdentifier(anyString())).thenAnswer(
        (Answer<UUID>) invocation -> UUID.randomUUID());

    final AssignPhoneNumberIdentifierCrawlerListener crawler =
        new AssignPhoneNumberIdentifierCrawlerListener(accountsManager, phoneNumberIdentifiers);

    crawler.onCrawlChunk(Optional.empty(), List.of(accountWithPni, accountWithoutPni));

    verify(accountsManager).update(eq(accountWithoutPni), any());
    verify(accountWithoutPni).setNumber(eq(numberWithoutPni), any());

    verify(accountsManager, never()).update(eq(accountWithPni), any());
    verify(accountWithPni, never()).setNumber(any(), any());
  }
}
