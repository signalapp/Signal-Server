/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Objects;
import javax.validation.Constraint;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import javax.validation.Payload;

/**
 * Constraint annotation that requires annotated entity
 * to hold (or return) a string value that is a valid E164-normalized phone number.
 */
@Target({ FIELD, PARAMETER, METHOD })
@Retention(RUNTIME)
@Constraint(validatedBy = E164.Validator.class)
@Documented
public @interface E164 {

  String message() default "{org.whispersystems.textsecuregcm.util.E164.message}";

  Class<?>[] groups() default { };

  Class<? extends Payload>[] payload() default { };

  class Validator implements ConstraintValidator<E164, String> {

    @Override
    public boolean isValid(final String value, final ConstraintValidatorContext context) {
      if (Objects.isNull(value)) {
        return true;
      }
      if (!value.startsWith("+")) {
        return false;
      }
      try {
        Util.requireNormalizedNumber(value);
      } catch (final ImpossiblePhoneNumberException | NonNormalizedPhoneNumberException e) {
        return false;
      }
      return true;
    }
  }
}
