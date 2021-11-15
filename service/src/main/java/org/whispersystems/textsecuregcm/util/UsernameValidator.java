/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import org.apache.commons.lang3.StringUtils;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import java.util.regex.Pattern;

public class UsernameValidator implements ConstraintValidator<Username, String> {

  private static final Pattern USERNAME_PATTERN =
      Pattern.compile("^[a-z_][a-z0-9_]{3,25}$", Pattern.CASE_INSENSITIVE);

  @Override
  public boolean isValid(final String username, final ConstraintValidatorContext context) {
    return StringUtils.isNotBlank(username) && USERNAME_PATTERN.matcher(getCanonicalUsername(username)).matches();
  }

  public static String getCanonicalUsername(final String username) {
    return username != null ? username.toLowerCase() : null;
  }
}
