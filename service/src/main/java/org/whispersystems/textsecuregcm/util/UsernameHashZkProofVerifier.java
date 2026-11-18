/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import org.signal.libsignal.usernames.BaseUsernameException;
import org.signal.libsignal.usernames.Username;
import org.signal.libsignal.usernames.UsernameHash;
import org.signal.libsignal.zkgroup.InvalidInputException;

public class UsernameHashZkProofVerifier {
  public void verifyProof(final byte[] proof, final byte[] hash) throws BaseUsernameException, InvalidInputException {
    Username.verifyProof(proof, new UsernameHash(hash));
  }
}
