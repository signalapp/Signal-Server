/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.CONSTRUCTOR;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE_USE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.net.URI;
import java.util.Objects;

import jakarta.validation.Constraint;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import jakarta.validation.Payload;

/**
 * Constraint annotation that requires annotated entity is a valid HTTPS URI.
 */
@Target({ FIELD, METHOD, CONSTRUCTOR, PARAMETER, ANNOTATION_TYPE, TYPE_USE })
@Retention(RUNTIME)
@Constraint(validatedBy = {
  ValidHttpsURI.Validator.class,
})
@Documented
public @interface ValidHttpsURI {

  String message() default "value is not a valid https URI";

  Class<?>[] groups() default { };

  Class<? extends Payload>[] payload() default { };

  class Validator implements ConstraintValidator<ValidHttpsURI, URI> {

    @Override
    public boolean isValid(final URI value, final ConstraintValidatorContext context) {
      if (Objects.isNull(value)) {
        return false;
      }
      context.disableDefaultConstraintViolation();
      String scheme = value.getScheme();
      if (scheme == null || !scheme.equals("https")) {
        String msg = String.format("URI scheme must be https (was: %s)", scheme);
        context.buildConstraintViolationWithTemplate(msg).addConstraintViolation();
        return false;
      } else if (value.getHost() == null){
        String msg = "URI host must not be null";
        context.buildConstraintViolationWithTemplate(msg).addConstraintViolation();
        return false;
      } else {
        return true;
      }
    }
  }
}
