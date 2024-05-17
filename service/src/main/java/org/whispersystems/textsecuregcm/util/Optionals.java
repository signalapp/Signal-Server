package org.whispersystems.textsecuregcm.util;

import java.util.Optional;
import java.util.function.BiFunction;

public class Optionals {

  private Optionals() {}

  /**
   * Apply a function to two optional arguments, returning empty if either argument is empty
   *
   * @param optionalT Optional of type T
   * @param optionalU Optional of type U
   * @param fun       Function of T and U that returns R
   * @return The function applied to the values of optionalT and optionalU, or empty
   */
  public static <T, U, R> Optional<R> zipWith(Optional<T> optionalT, Optional<U> optionalU, BiFunction<T, U, R> fun) {
    return optionalT.flatMap(t -> optionalU.map(u -> fun.apply(t, u)));
  }
}
