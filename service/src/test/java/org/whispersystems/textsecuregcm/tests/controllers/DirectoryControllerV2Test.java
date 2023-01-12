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
        new byte[]{0x1}, new byte[]{0x2}, true, false,
        Clock.fixed(Instant.ofEpochSecond(1633738643L), ZoneId.of("Etc/UTC")));

    final DirectoryV2Controller controller = new DirectoryV2Controller(credentialGenerator);

    final Account account = mock(Account.class);
    final UUID uuid = UUID.fromString("11111111-1111-1111-1111-111111111111");
    when(account.getUuid()).thenReturn(uuid);

    final ExternalServiceCredentials credentials = (ExternalServiceCredentials) controller.getAuthToken(
        new AuthenticatedAccount(() -> new Pair<>(account, mock(Device.class)))).getEntity();

    assertEquals(credentials.username(), "d369bc712e2e0dd36258");
    assertEquals(credentials.password(), "1633738643:4433b0fab41f25f79dd4");
  }

}
