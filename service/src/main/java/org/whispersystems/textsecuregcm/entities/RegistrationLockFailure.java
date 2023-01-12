/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;

public record RegistrationLockFailure(long timeRemaining, ExternalServiceCredentials backupCredentials) {

}
