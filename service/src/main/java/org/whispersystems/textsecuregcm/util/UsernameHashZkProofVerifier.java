package org.whispersystems.textsecuregcm.util;

import org.signal.libsignal.usernames.BaseUsernameException;
import org.signal.libsignal.usernames.Username;

public class UsernameHashZkProofVerifier {
  public void verifyProof(byte[] proof, byte[] hash) throws BaseUsernameException {
    Username.verifyProof(proof, hash);
  }
}
