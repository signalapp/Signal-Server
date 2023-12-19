/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.registration;

public class RegistrationFraudException extends Exception {
  public RegistrationFraudException(final RegistrationServiceSenderException cause) {
    super(null, cause, true, false);
  }
}
