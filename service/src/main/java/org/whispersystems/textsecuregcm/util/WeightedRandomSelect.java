package org.whispersystems.textsecuregcm.util;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Select a random item according to its weight
 *
 * @param <T> the type of the objects to select from
 */
public class WeightedRandomSelect<T> {

  List<Pair<T, Long>> weightedItems;
  long totalWeight;

  public WeightedRandomSelect(List<Pair<T, Long>> weightedItems) throws IllegalArgumentException {
    this.weightedItems = weightedItems;
    this.totalWeight = weightedItems.stream().mapToLong(Pair::second).sum();

    weightedItems.stream().map(Pair::second).filter(w -> w < 0).findFirst().ifPresent(invalid -> {
      throw new IllegalArgumentException("Illegal selection weight " + invalid);
    });

    if (weightedItems.isEmpty() || totalWeight == 0) {
      throw new IllegalArgumentException("Cannot create an empty weighted random selector");
    }
  }

  public T select() {
    if (weightedItems.size() == 1) {
      return weightedItems.get(0).first();
    }
    long select = ThreadLocalRandom.current().nextLong(0, totalWeight);
    long current = 0;
    for (Pair<T, Long> item : weightedItems) {
      /*
        Accumulate weights for each item and select the first item whose
        cumulative weight exceeds the selected value. nextLong() is exclusive,
        so by the last item we're guaranteed to find a value as the
        last item's weight is one more than the maximum value of select.
      */
      current += item.second();
      if (current > select) {
        return item.first();
      }
    }
    throw new IllegalStateException("totalWeight " + totalWeight + " exceeds item weights");
  }

  public static <T> T select(List<Pair<T, Long>> weightedItems) {
    return new WeightedRandomSelect<T>(weightedItems).select();
  }

}
