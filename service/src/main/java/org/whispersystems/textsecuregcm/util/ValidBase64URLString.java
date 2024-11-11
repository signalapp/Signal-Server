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
import java.util.Base64;
import java.util.Objects;

/**
 * Constraint annotation that requires annotated entity is a valid url-base64 encoded string.
 */
@Target({ FIELD, PARAMETER, METHOD })
@Retention(RUNTIME)
@Constraint(validatedBy = ValidBase64URLString.Validator.class)
@Documented
public @interface ValidBase64URLString {

  String message() default "value is not a valid base64 string";

  Class<?>[] groups() default { };

  Class<? extends Payload>[] payload() default { };

  class Validator implements ConstraintValidator<ValidBase64URLString, String> {

    @Override
    public boolean isValid(final String value, final ConstraintValidatorContext context) {
      if (Objects.isNull(value)) {
        return true;
      }
      try {
        Base64.getUrlDecoder().decode(value);
        return true;
      } catch (IllegalArgumentException e) {
        return false;
      }
    }
  }
}
