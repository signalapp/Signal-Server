/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE_USE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import javax.validation.Constraint;
import javax.validation.Payload;

@Target({FIELD, PARAMETER, TYPE_USE})
@Retention(RUNTIME)
@Constraint(validatedBy = {ValidPreKeyValidator.class})
@Documented
public @interface ValidPreKey {

  public enum PreKeyType {
      ECC,
      KYBER
  }

  PreKeyType type();

  String message() default "{org.whispersystems.textsecuregcm.util.ValidPreKey.message}";

  Class<?>[] groups() default { };

  Class<? extends Payload>[] payload() default { };

}
