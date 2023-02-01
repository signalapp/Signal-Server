/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;


public class NicknameValidator implements ConstraintValidator<Nickname, String> {
  @Override
  public boolean isValid(final String nickname, final ConstraintValidatorContext context) {
    return UsernameGenerator.isValidNickname(nickname);
  }
}
