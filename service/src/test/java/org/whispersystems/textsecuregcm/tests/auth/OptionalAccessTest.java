/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Base64;
import java.util.Optional;
import javax.ws.rs.WebApplicationException;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.auth.Anonymous;
import org.whispersystems.textsecuregcm.auth.OptionalAccess;
import org.whispersystems.textsecuregcm.storage.Account;

class OptionalAccessTest {

  @Test
  void testUnidentifiedMissingTarget() {
    try {
      OptionalAccess.verify(Optional.empty(), Optional.empty(), Optional.empty());
      throw new AssertionError("should fail");
    } catch (WebApplicationException e) {
      assertEquals(e.getResponse().getStatus(), 401);
    }
  }

  @Test
  void testUnidentifiedMissingTargetDevice() {
    Account account = mock(Account.class);
    when(account.isEnabled()).thenReturn(true);
    when(account.getDevice(eq(10))).thenReturn(Optional.empty());
    when(account.getUnidentifiedAccessKey()).thenReturn(Optional.of("1234".getBytes()));

    try {
      OptionalAccess.verify(Optional.empty(), Optional.of(new Anonymous(Base64.getEncoder().encodeToString("1234".getBytes()))), Optional.of(account), "10");
    } catch (WebApplicationException e) {
      assertEquals(e.getResponse().getStatus(), 401);
    }
  }

  @Test
  void testUnidentifiedBadTargetDevice() {
    Account account = mock(Account.class);
    when(account.isEnabled()).thenReturn(true);
    when(account.getDevice(eq(10))).thenReturn(Optional.empty());
    when(account.getUnidentifiedAccessKey()).thenReturn(Optional.of("1234".getBytes()));

    try {
      OptionalAccess.verify(Optional.empty(), Optional.of(new Anonymous(Base64.getEncoder().encodeToString("1234".getBytes()))), Optional.of(account), "$$");
    } catch (WebApplicationException e) {
      assertEquals(e.getResponse().getStatus(), 422);
    }
  }


  @Test
  void testUnidentifiedBadCode() {
    Account account = mock(Account.class);
    when(account.isEnabled()).thenReturn(true);
    when(account.getUnidentifiedAccessKey()).thenReturn(Optional.of("1234".getBytes()));

    try {
      OptionalAccess.verify(Optional.empty(), Optional.of(new Anonymous(Base64.getEncoder().encodeToString("5678".getBytes()))), Optional.of(account));
      throw new AssertionError("should fail");
    } catch (WebApplicationException e) {
      assertEquals(e.getResponse().getStatus(), 401);
    }
  }

  @Test
  void testIdentifiedMissingTarget() {
    Account account =  mock(Account.class);
    when(account.isEnabled()).thenReturn(true);

    try {
      OptionalAccess.verify(Optional.of(account), Optional.empty(), Optional.empty());
      throw new AssertionError("should fail");
    } catch (WebApplicationException e) {
      assertEquals(e.getResponse().getStatus(), 404);
    }
  }

  @Test
  void testUnsolicitedBadTarget() {
    Account account = mock(Account.class);
    when(account.isUnrestrictedUnidentifiedAccess()).thenReturn(false);
    when(account.isEnabled()).thenReturn(true);

    try {
      OptionalAccess.verify(Optional.empty(), Optional.empty(), Optional.of(account));
      throw new AssertionError("should fail");
    } catch (WebApplicationException e) {
      assertEquals(e.getResponse().getStatus(), 401);
    }
  }

  @Test
  void testUnsolicitedGoodTarget() {
    Account account = mock(Account.class);
    Anonymous random = mock(Anonymous.class);
    when(account.isUnrestrictedUnidentifiedAccess()).thenReturn(true);
    when(account.isEnabled()).thenReturn(true);
    OptionalAccess.verify(Optional.empty(), Optional.of(random), Optional.of(account));
  }

  @Test
  void testUnidentifiedGoodTarget() {
    Account account = mock(Account.class);
    when(account.getUnidentifiedAccessKey()).thenReturn(Optional.of("1234".getBytes()));
    when(account.isEnabled()).thenReturn(true);
    OptionalAccess.verify(Optional.empty(), Optional.of(new Anonymous(Base64.getEncoder().encodeToString("1234".getBytes()))), Optional.of(account));
  }

  @Test
  void testUnidentifiedInactive() {
    Account account = mock(Account.class);
    when(account.getUnidentifiedAccessKey()).thenReturn(Optional.of("1234".getBytes()));
    when(account.isEnabled()).thenReturn(false);

    try {
      OptionalAccess.verify(Optional.empty(), Optional.of(new Anonymous(Base64.getEncoder().encodeToString("1234".getBytes()))), Optional.of(account));
      throw new AssertionError();
    } catch (WebApplicationException e) {
      assertEquals(e.getResponse().getStatus(), 401);
    }
  }

  @Test
  void testIdentifiedGoodTarget() {
    Account source = mock(Account.class);
    Account target = mock(Account.class);
    when(target.isEnabled()).thenReturn(true);
    OptionalAccess.verify(Optional.of(source), Optional.empty(), Optional.of(target));
  }
}
