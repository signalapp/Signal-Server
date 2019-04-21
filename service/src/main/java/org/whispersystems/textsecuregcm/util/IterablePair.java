/**
 * Copyright (C) 2013 Open WhisperSystems
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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