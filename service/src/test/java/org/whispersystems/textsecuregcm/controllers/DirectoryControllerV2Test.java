/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.whispersystems.textsecuregcm.util.MockUtils.secretBytesOf;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.configuration.DirectoryV2ClientConfiguration;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;

class DirectoryControllerV2Test {

  @Test
  void testAuthToken() {
    final ExternalServiceCredentialsGenerator credentialsGenerator = DirectoryV2Controller.credentialsGenerator(
        new DirectoryV2ClientConfiguration(secretBytesOf(0x01), secretBytesOf(0x02)),
        Clock.fixed(Instant.ofEpochSecond(1633738643L), ZoneId.of("Etc/UTC"))
    );

    final DirectoryV2Controller controller = new DirectoryV2Controller(credentialsGenerator);

    final Account account = mock(Account.class);
    final UUID uuid = UUID.fromString("11111111-1111-1111-1111-111111111111");
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(uuid);

    final ExternalServiceCredentials credentials = controller.getAuthToken(
        new AuthenticatedDevice(uuid, Device.PRIMARY_ID, Instant.now()));

    assertEquals("d369bc712e2e0dd36258", credentials.username());
    assertEquals("1633738643:4433b0fab41f25f79dd4", credentials.password());
  }

}
