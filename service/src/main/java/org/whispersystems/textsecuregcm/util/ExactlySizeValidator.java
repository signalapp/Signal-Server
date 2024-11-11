/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class ExactlySizeValidator<T> implements ConstraintValidator<ExactlySize, T> {

  private Set<Integer> permittedSizes;

  @Override
  public void initialize(ExactlySize annotation) {
    permittedSizes = Arrays.stream(annotation.value()).boxed().collect(Collectors.toSet());
  }

  @Override
  public boolean isValid(T value, ConstraintValidatorContext context) {
    return permittedSizes.contains(size(value));
  }

  protected abstract int size(T value);
}
