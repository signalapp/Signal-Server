/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import java.lang.reflect.Method;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class E164Test {

  private static final Validator VALIDATOR = Validation.buildDefaultValidatorFactory().getValidator();

  private static final String E164_VALID = "+18005550123";

  private static final String E164_INVALID = "1(800)555-0123";

  private static final String EMPTY = "";

  @SuppressWarnings("FieldCanBeLocal")
  private static class Data {

    @E164
    private final String number;

    @E164
    private final Optional<String> optionalNumber;

    private Data(final String number, final Optional<String> optionalNumber) {
      this.number = number;
      this.optionalNumber = optionalNumber;
    }
  }

  private static class Methods {

    public void foo(@E164 final String number, @E164 final Optional<String> optionalNumber) {
      // noop
    }

    @E164
    public String bar() {
      return "nevermind";
    }

    @E164
    public Optional<String> barOptionalString() {
      return Optional.of("nevermind");
    }
  }

  private record Rec(@E164 String number, @E164 Optional<String> optionalNumber) {
  }

  @Test
  public void testRecord() {
    checkNoViolations(new Rec(E164_VALID, Optional.of(E164_VALID)));
    checkHasViolations(new Rec(E164_INVALID, Optional.of(E164_INVALID)));
    checkHasViolations(new Rec(EMPTY, Optional.of(EMPTY)));
  }

  @Test
  public void testClassField() {
    checkNoViolations(new Data(E164_VALID, Optional.of(E164_VALID)));
    checkHasViolations(new Data(E164_INVALID, Optional.of(E164_INVALID)));
    checkHasViolations(new Data(EMPTY, Optional.of(EMPTY)));
  }

  @Test
  public void testParameters() throws Exception {
    final Methods m = new Methods();
    final Method foo = Methods.class.getMethod("foo", String.class, Optional.class);

    final Set<ConstraintViolation<Methods>> violations1 =
        VALIDATOR.forExecutables().validateParameters(m, foo, new Object[] {E164_VALID, Optional.of(E164_VALID)});
    final Set<ConstraintViolation<Methods>> violations2 =
        VALIDATOR.forExecutables().validateParameters(m, foo, new Object[] {E164_INVALID, Optional.of(E164_INVALID)});
    final Set<ConstraintViolation<Methods>> violations3 =
        VALIDATOR.forExecutables().validateParameters(m, foo, new Object[] {EMPTY, Optional.of(EMPTY)});

    assertTrue(violations1.isEmpty());
    assertFalse(violations2.isEmpty());
    assertFalse(violations3.isEmpty());
  }

  @Test
  public void testReturnValue() throws Exception {
    final Methods m = new Methods();
    final Method bar = Methods.class.getMethod("bar");

    final Set<ConstraintViolation<Methods>> violations1 =
        VALIDATOR.forExecutables().validateReturnValue(m, bar, E164_VALID);
    final Set<ConstraintViolation<Methods>> violations2 =
        VALIDATOR.forExecutables().validateReturnValue(m, bar, E164_INVALID);
    final Set<ConstraintViolation<Methods>> violations3 =
        VALIDATOR.forExecutables().validateReturnValue(m, bar, EMPTY);

    assertTrue(violations1.isEmpty());
    assertFalse(violations2.isEmpty());
    assertFalse(violations3.isEmpty());
  }

  @Test
  public void testOptionalReturnValue() throws Exception {
    final Methods m = new Methods();
    final Method bar = Methods.class.getMethod("barOptionalString");

    final Set<ConstraintViolation<Methods>> violations1 =
        VALIDATOR.forExecutables().validateReturnValue(m, bar, Optional.of(E164_VALID));
    final Set<ConstraintViolation<Methods>> violations2 =
        VALIDATOR.forExecutables().validateReturnValue(m, bar, Optional.of(E164_INVALID));
    final Set<ConstraintViolation<Methods>> violations3 =
        VALIDATOR.forExecutables().validateReturnValue(m, bar, Optional.of(EMPTY));

    assertTrue(violations1.isEmpty());
    assertFalse(violations2.isEmpty());
    assertFalse(violations3.isEmpty());
  }

  private static <T> void checkNoViolations(final T object) {
    final Set<ConstraintViolation<T>> violations = VALIDATOR.validate(object);
    assertTrue(violations.isEmpty());
  }

  private static <T> void checkHasViolations(final T object) {
    final Set<ConstraintViolation<T>> violations = VALIDATOR.validate(object);
    assertFalse(violations.isEmpty());
  }
}
