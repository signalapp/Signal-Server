/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.tests.util;

import java.security.Principal;
import org.whispersystems.websocket.ReusableAuth;
import org.whispersystems.websocket.auth.PrincipalSupplier;

public class TestPrincipal implements Principal {

  private final String name;

  private TestPrincipal(String name) {
    this.name = name;
  }

  @Override
  public String getName() {
    return name;
  }

  public static ReusableAuth<TestPrincipal> reusableAuth(final String name) {
    return ReusableAuth.authenticated(new TestPrincipal(name), PrincipalSupplier.forImmutablePrincipal());
  }
}
