/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import jakarta.validation.Constraint;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import jakarta.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.HexFormat;
import java.util.Objects;

/**
 * Constraint annotation that requires annotated entity is a valid hex encoded string.
 */
@Target({ FIELD, PARAMETER, METHOD })
@Retention(RUNTIME)
@Constraint(validatedBy = ValidHexString.Validator.class)
@Documented
public @interface ValidHexString {

  String message() default "value is not a valid hex string";

  Class<?>[] groups() default { };

  Class<? extends Payload>[] payload() default { };

  class Validator implements ConstraintValidator<ValidHexString, String> {

    @Override
    public boolean isValid(final String value, final ConstraintValidatorContext context) {
      if (Objects.isNull(value)) {
        return true;
      }
      try {
        HexFormat.of().parseHex(value);
        return true;
      } catch (IllegalArgumentException e) {
        return false;
      }
    }
  }
}
