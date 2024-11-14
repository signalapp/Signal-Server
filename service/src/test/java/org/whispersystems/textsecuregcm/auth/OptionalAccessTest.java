/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.ws.rs.WebApplicationException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.PniServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;

class OptionalAccessTest {

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  @ParameterizedTest
  @MethodSource
  void verify(final Optional<Account> requestAccount,
      final Optional<Anonymous> accessKey,
      final Optional<Account> targetAccount,
      final ServiceIdentifier targetIdentifier,
      final String deviceSelector,
      final OptionalInt expectedStatusCode) {

    expectedStatusCode.ifPresentOrElse(statusCode -> {
      final WebApplicationException webApplicationException = assertThrows(WebApplicationException.class,
          () -> OptionalAccess.verify(requestAccount, accessKey, targetAccount, targetIdentifier, deviceSelector));

      assertEquals(statusCode, webApplicationException.getResponse().getStatus());
    }, () -> assertDoesNotThrow(() ->
        OptionalAccess.verify(requestAccount, accessKey, targetAccount, targetIdentifier, deviceSelector)));
  }

  private static List<Arguments> verify() {
    final String unidentifiedAccessKey = RandomStringUtils.secure().nextAlphanumeric(16);

    final Anonymous correctUakHeader =
        new Anonymous(Base64.getEncoder().encodeToString(unidentifiedAccessKey.getBytes()));

    final Anonymous incorrectUakHeader =
        new Anonymous(Base64.getEncoder().encodeToString((unidentifiedAccessKey + "-incorrect").getBytes()));

    final Account targetAccount = mock(Account.class);
    final ServiceIdentifier targetAccountAciIdentifier = new AciServiceIdentifier(UUID.randomUUID());
    final ServiceIdentifier targetAccountPniIdentifier = new PniServiceIdentifier(UUID.randomUUID());
    when(targetAccount.getDevice(Device.PRIMARY_ID)).thenReturn(Optional.of(mock(Device.class)));
    when(targetAccount.getUnidentifiedAccessKey())
        .thenReturn(Optional.of(unidentifiedAccessKey.getBytes(StandardCharsets.UTF_8)));
    when(targetAccount.isIdentifiedBy(targetAccountAciIdentifier)).thenReturn(true);
    when(targetAccount.isIdentifiedBy(targetAccountPniIdentifier)).thenReturn(true);

    final Account allowAllTargetAccount = mock(Account.class);
    final ServiceIdentifier allowAllTargetAccountPniIdentifier = new PniServiceIdentifier(UUID.randomUUID());
    when(allowAllTargetAccount.getDevice(Device.PRIMARY_ID)).thenReturn(Optional.of(mock(Device.class)));
    when(allowAllTargetAccount.isUnrestrictedUnidentifiedAccess()).thenReturn(true);
    when(allowAllTargetAccount.isIdentifiedBy(allowAllTargetAccountPniIdentifier)).thenReturn(true);

    final Account noUakTargetAccount = mock(Account.class);
    final ServiceIdentifier noUakTargetAccountAciIdentifier = new AciServiceIdentifier(UUID.randomUUID());
    when(noUakTargetAccount.getDevice(Device.PRIMARY_ID)).thenReturn(Optional.of(mock(Device.class)));
    when(noUakTargetAccount.getUnidentifiedAccessKey()).thenReturn(Optional.empty());
    when(noUakTargetAccount.isIdentifiedBy(noUakTargetAccountAciIdentifier)).thenReturn(true);

    final Account inactiveTargetAccount = mock(Account.class);
    final ServiceIdentifier inactiveTargetAccountAciIdentifier = new AciServiceIdentifier(UUID.randomUUID());
    when(inactiveTargetAccount.getDevice(Device.PRIMARY_ID)).thenReturn(Optional.of(mock(Device.class)));
    when(inactiveTargetAccount.getUnidentifiedAccessKey())
        .thenReturn(Optional.of(unidentifiedAccessKey.getBytes(StandardCharsets.UTF_8)));
    when(inactiveTargetAccount.isIdentifiedBy(inactiveTargetAccountAciIdentifier)).thenReturn(true);

    return List.of(
        // Unidentified caller; correct UAK
        Arguments.of(Optional.empty(),
            Optional.of(correctUakHeader),
            Optional.of(targetAccount),
            targetAccountAciIdentifier,
            OptionalAccess.ALL_DEVICES_SELECTOR,
            OptionalInt.empty()),

        // Identified caller; no UAK needed
        Arguments.of(Optional.of(mock(Account.class)),
            Optional.empty(),
            Optional.of(targetAccount),
            targetAccountAciIdentifier,
            OptionalAccess.ALL_DEVICES_SELECTOR,
            OptionalInt.empty()),

        // Unidentified caller; target account not found
        Arguments.of(Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            new AciServiceIdentifier(UUID.randomUUID()),
            OptionalAccess.ALL_DEVICES_SELECTOR,
            OptionalInt.of(401)),

        // Identified caller; target account not found
        Arguments.of(Optional.of(mock(Account.class)),
            Optional.empty(),
            Optional.empty(),
            new AciServiceIdentifier(UUID.randomUUID()),
            OptionalAccess.ALL_DEVICES_SELECTOR,
            OptionalInt.of(404)),

        // Unidentified caller; target account found, but target device not found
        Arguments.of(Optional.empty(),
            Optional.of(correctUakHeader),
            Optional.of(targetAccount),
            targetAccountAciIdentifier,
            String.valueOf(Device.PRIMARY_ID + 1),
            OptionalInt.of(401)),

        // Unidentified caller; target account found, but incorrect UAK provided
        Arguments.of(Optional.empty(),
            Optional.of(incorrectUakHeader),
            Optional.of(targetAccount),
            targetAccountAciIdentifier,
            OptionalAccess.ALL_DEVICES_SELECTOR,
            OptionalInt.of(401)),

        // Unidentified caller; target account found, but has no UAK
        Arguments.of(Optional.empty(),
            Optional.of(correctUakHeader),
            Optional.of(noUakTargetAccount),
            noUakTargetAccountAciIdentifier,
            OptionalAccess.ALL_DEVICES_SELECTOR,
            OptionalInt.of(401)),

        // Unidentified caller; target account found, allows unrestricted unidentified access, so PNI target doesn't matter
        Arguments.of(Optional.empty(),
            Optional.of(incorrectUakHeader),
            Optional.of(allowAllTargetAccount),
            allowAllTargetAccountPniIdentifier,
            OptionalAccess.ALL_DEVICES_SELECTOR,
            OptionalInt.empty()),

        // Unidentified caller; target account found, but inactive
        Arguments.of(Optional.empty(),
            Optional.of(correctUakHeader),
            Optional.of(inactiveTargetAccount),
            inactiveTargetAccountAciIdentifier,
            OptionalAccess.ALL_DEVICES_SELECTOR,
            OptionalInt.empty()),

        // Malformed device ID
        Arguments.of(Optional.empty(),
            Optional.of(correctUakHeader),
            Optional.of(targetAccount),
            targetAccountAciIdentifier,
            "not a valid identifier",
            OptionalInt.of(422)),

        // Unidentified caller; target account found, but PNI identifier
        Arguments.of(Optional.empty(),
            Optional.of(correctUakHeader),
            Optional.of(targetAccount),
            targetAccountPniIdentifier,
            OptionalAccess.ALL_DEVICES_SELECTOR,
            OptionalInt.of(401))
    );
  }

  @Test
  void testTargetIdentifierIllegalArgument() {
    final String unidentifiedAccessKey = RandomStringUtils.secure().nextAlphanumeric(16);

    final Anonymous correctUakHeader =
        new Anonymous(Base64.getEncoder().encodeToString(unidentifiedAccessKey.getBytes()));

    final Account targetAccount = mock(Account.class);
    when(targetAccount.getDevice(Device.PRIMARY_ID)).thenReturn(Optional.of(mock(Device.class)));
    when(targetAccount.getUnidentifiedAccessKey())
        .thenReturn(Optional.of(unidentifiedAccessKey.getBytes(StandardCharsets.UTF_8)));

    assertThrows(IllegalArgumentException.class,
        () -> OptionalAccess.verify(Optional.empty(), Optional.of(correctUakHeader), Optional.of(targetAccount),
            new AciServiceIdentifier(UUID.randomUUID())));
  }

}
