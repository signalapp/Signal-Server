/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 */

package org.whispersystems.textsecuregcm.util;

import org.whispersystems.textsecuregcm.storage.Device;

public class RegistrationIdValidator {
  public static boolean validRegistrationId(int registrationId) {
    return registrationId > 0 && registrationId <= Device.MAX_REGISTRATION_ID;
  }
}
