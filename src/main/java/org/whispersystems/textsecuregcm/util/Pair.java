package org.whispersystems.textsecuregcm.util;

public class Pair<T1, T2> {
  private final T1 v1;
  private final T2 v2;

  Pair(T1 v1, T2 v2) {
    this.v1 = v1;
    this.v2 = v2;
  }

  public T1 first(){
    return v1;
  }

  public T2 second(){
    return v2;
  }
}
