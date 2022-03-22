package org.whispersystems.textsecuregcm.util;

import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class WeightedRandomSelectTest {

  @Test
  public void test5050() {
    final WeightedRandomSelect<String> selector = new WeightedRandomSelect<>(
        List.of(new Pair<>("a", 1L), new Pair<>("b", 1L)));
    final Map<String, Long> counts = Stream.generate(selector::select)
        .limit(1000)
        .collect(Collectors.groupingBy(s -> s, Collectors.counting()));
    assertThat(counts.get("a")).isGreaterThan(1);
    assertThat(counts.get("b")).isGreaterThan(1);
  }

  @Test
  public void testAlways() {
    final WeightedRandomSelect<String> selector = new WeightedRandomSelect<>(
        List.of(new Pair<>("a", 1L), new Pair<>("b", 0L)));
    final Map<String, Long> counts = Stream.generate(selector::select)
        .limit(1000)
        .collect(Collectors.groupingBy(s -> s, Collectors.counting()));
    assertThat(counts.get("a")).isEqualTo(1000);
    assertThat(counts).doesNotContainKey("b");
  }

  @Test
  public void testThree() {
    final WeightedRandomSelect<String> selector = new WeightedRandomSelect<>(
        List.of(new Pair<>("a", 33L), new Pair<>("b", 33L), new Pair<>("c", 33L)));
    final Map<String, Long> counts = Stream.generate(selector::select)
        .limit(1000)
        .collect(Collectors.groupingBy(s -> s, Collectors.counting()));
    assertThat(counts.get("a")).isGreaterThan(1);
    assertThat(counts.get("b")).isGreaterThan(1);
    assertThat(counts.get("c")).isGreaterThan(1);
  }
}
