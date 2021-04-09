/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.util;

import java.util.Iterator;
import java.util.List;

public class IterablePair <T1, T2> implements Iterable<Pair<T1,T2>> {
  private final List<T1> first;
  private final List<T2> second;

  public IterablePair(List<T1> first, List<T2> second) {
    this.first = first;
    this.second = second;
  }

  @Override
  public Iterator<Pair<T1, T2>> iterator(){
    return new ParallelIterator<>( first.iterator(), second.iterator() );
  }

  public static class ParallelIterator <T1, T2> implements Iterator<Pair<T1, T2>> {

    private final Iterator<T1> it1;
    private final Iterator<T2> it2;

    public ParallelIterator(Iterator<T1> it1, Iterator<T2> it2) {
      this.it1 = it1; this.it2 = it2;
    }

    @Override
    public boolean hasNext() { return it1.hasNext() && it2.hasNext(); }

    @Override
    public Pair<T1, T2> next() {
      return new Pair<>(it1.next(), it2.next());
    }

    @Override
    public void remove(){
      it1.remove();
      it2.remove();
    }
  }
}
