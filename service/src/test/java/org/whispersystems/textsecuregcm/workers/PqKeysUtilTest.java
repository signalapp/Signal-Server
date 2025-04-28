/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.KeysManager;
import reactor.core.publisher.Flux;

class PqKeysUtilTest {

  private KeysManager keysManager;
  private PqKeysUtil pqKeysUtil;

  @BeforeEach
  void setUp() {
    keysManager = mock(KeysManager.class);
    pqKeysUtil = new PqKeysUtil(keysManager, 16, 3);
  }

  @Test
  void getAccountsWithoutPqKeys() {
    final UUID aciWithPqKeys = UUID.randomUUID();

    final Account accountWithPqKeys = mock(Account.class);
    when(accountWithPqKeys.getIdentifier(IdentityType.ACI)).thenReturn(aciWithPqKeys);

    final Account accountWithoutPqKeys = mock(Account.class);
    when(accountWithoutPqKeys.getIdentifier(IdentityType.ACI)).thenReturn(UUID.randomUUID());

    when(keysManager.getLastResort(any(), anyByte())).thenReturn(CompletableFuture.completedFuture(Optional.empty()));
    when(keysManager.getLastResort(aciWithPqKeys, Device.PRIMARY_ID))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(mock(KEMSignedPreKey.class))));

    assertEquals(List.of(accountWithoutPqKeys),
        Flux.just(accountWithPqKeys, accountWithoutPqKeys)
            .transform(pqKeysUtil::getAccountsWithoutPqKeys)
            .collectList()
            .block());
  }
}
