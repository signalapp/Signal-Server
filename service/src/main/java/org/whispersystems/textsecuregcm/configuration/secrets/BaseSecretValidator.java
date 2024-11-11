/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration.secrets;

import static java.util.Objects.requireNonNull;

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import java.lang.annotation.Annotation;

public abstract class BaseSecretValidator<A extends Annotation, T, S extends Secret<? extends T>> implements ConstraintValidator<A, S> {

  private final ConstraintValidator<A, T> validator;


  protected BaseSecretValidator(final ConstraintValidator<A, T> validator) {
    this.validator = requireNonNull(validator);
  }

  @Override
  public void initialize(final A constraintAnnotation) {
    validator.initialize(constraintAnnotation);
  }

  @Override
  public boolean isValid(final S value, final ConstraintValidatorContext context) {
    return validator.isValid(value.value(), context);
  }
}
