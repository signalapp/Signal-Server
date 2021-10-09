/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.controllers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialGenerator;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.controllers.DirectoryV2Controller;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.Pair;

class DirectoryControllerV2Test {

  @Test
  void testAuthToken() {
    final ExternalServiceCredentialGenerator credentialGenerator = new ExternalServiceCredentialGenerator(
        new byte[]{0x1}, new byte[0], false, false,
        Clock.fixed(Instant.ofEpochSecond(1633738643L), ZoneId.of("Etc/UTC")));

    final DirectoryV2Controller controller = new DirectoryV2Controller(credentialGenerator);

    final Account account = mock(Account.class);
    final UUID uuid = UUID.fromString("11111111-1111-1111-1111-111111111111");
    when(account.getUuid()).thenReturn(uuid);
    when(account.getNumber()).thenReturn("+15055551111");

    final ExternalServiceCredentials credentials = (ExternalServiceCredentials) controller.getAuthToken(
        new AuthenticatedAccount(() -> new Pair<>(account, mock(Device.class)))).getEntity();

    assertEquals(credentials.getUsername(), "EREREREREREREREREREREQAAAABZvPKn");
    assertEquals(credentials.getPassword(), "1633738643:ff03669c64f3f938a279");
    assertEquals(32, credentials.getUsername().length());
    assertEquals(31, credentials.getPassword().length());
  }

}
