/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import org.signal.libsignal.protocol.InvalidKeyException;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.kem.KEMPublicKey;
import org.whispersystems.textsecuregcm.entities.PreKey;

public class ValidPreKeyValidator implements ConstraintValidator<ValidPreKey, PreKey> {
  private ValidPreKey.PreKeyType type;

  @Override
  public void initialize(ValidPreKey annotation) {
    type = annotation.type();
  }

  @Override
  public boolean isValid(PreKey value, ConstraintValidatorContext context) {
    if (value == null) {
      return true;
    }
    try {
      switch (type) {
        case ECC -> Curve.decodePoint(value.getPublicKey(), 0);
        case KYBER -> new KEMPublicKey(value.getPublicKey());
      }
    } catch (IllegalArgumentException | InvalidKeyException e) {
      return false;
    }
    return true;
  }
}
