/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import org.whispersystems.textsecuregcm.auth.StoredVerificationCode;
import java.util.Optional;

public interface VerificationCodeStore {

  void insert(String number, StoredVerificationCode verificationCode);

  Optional<StoredVerificationCode> findForNumber(String number);

  void remove(String number);
}
