package org.whispersystems.textsecuregcm.tests.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.storage.UsernameNotAvailableException;
import org.whispersystems.textsecuregcm.util.UsernameGenerator;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class UsernameGeneratorTest {

  private static final Duration TTL = Duration.ofMinutes(5);

  @ParameterizedTest(name = "[{index}]:{0} ({2})")
  @MethodSource
  public void nicknameValidation(String nickname, boolean valid, String testCaseName) {
    assertThat(UsernameGenerator.isValidNickname(nickname)).isEqualTo(valid);
  }

  static Stream<Arguments> nicknameValidation() {
    return Stream.of(
        Arguments.of("Test", true, "upper case"),
        Arguments.of("tesT", true, "upper case"),
        Arguments.of("te-st", false, "illegal character"),
        Arguments.of("ab\uD83D\uDC1B", false, "illegal character"),
        Arguments.of("1test", false, "illegal start"),
        Arguments.of("test#123", false, "illegal character"),
        Arguments.of("test.123", false, "illegal character"),
        Arguments.of("ab", false, "too short"),
        Arguments.of("", false, ""),
        Arguments.of("_123456789_123456789_123456789123", false, "33 characters"),

        Arguments.of("_test", true, ""),
        Arguments.of("test", true, ""),
        Arguments.of("test123", true, ""),
        Arguments.of("abc", true, ""),
        Arguments.of("_123456789_123456789_12345678912", true, "32 characters")
    );
  }

  @ParameterizedTest(name="[{index}]: {0}")
  @MethodSource
  public void nonStandardUsernames(final String username, final boolean isStandard) {
    assertThat(UsernameGenerator.isStandardFormat(username)).isEqualTo(isStandard);
  }

  static Stream<Arguments> nonStandardUsernames() {
    return Stream.of(
        Arguments.of("Test.123", true),
        Arguments.of("test.-123", false),
        Arguments.of("test.0", false),
        Arguments.of("test.", false),
        Arguments.of("test.1_00", false),

        Arguments.of("test.1", true),
        Arguments.of("abc.1234", true)
    );
  }

  @Test
  public void zeroPadDiscriminators() {
    final UsernameGenerator generator = new UsernameGenerator(4, 5, 1, TTL);
    assertThat(generator.fromParts("test", 1)).isEqualTo("test.0001");
    assertThat(generator.fromParts("test", 123)).isEqualTo("test.0123");
    assertThat(generator.fromParts("test", 9999)).isEqualTo("test.9999");
    assertThat(generator.fromParts("test", 99999)).isEqualTo("test.99999");
  }

  @Test
  public void expectedWidth() throws UsernameNotAvailableException {
    String username = new UsernameGenerator(1, 6, 1, TTL).generateAvailableUsername("test", t -> true);
    assertThat(extractDiscriminator(username)).isGreaterThan(0).isLessThan(10);

    username = new UsernameGenerator(2, 6, 1, TTL).generateAvailableUsername("test", t -> true);
    assertThat(extractDiscriminator(username)).isGreaterThan(0).isLessThan(100);
  }

  @Test
  public void expandDiscriminator() throws UsernameNotAvailableException {
    UsernameGenerator ug = new UsernameGenerator(1, 6, 10, TTL);
    final String username = ug.generateAvailableUsername("test", allowDiscriminator(d -> d >= 10000));
    int discriminator = extractDiscriminator(username);
    assertThat(discriminator).isGreaterThanOrEqualTo(10000).isLessThan(100000);
  }

  @Test
  public void expandDiscriminatorToMax() throws UsernameNotAvailableException {
    UsernameGenerator ug = new UsernameGenerator(1, 6, 10, TTL);
    final String username = ug.generateAvailableUsername("test", allowDiscriminator(d -> d >= 100000));
    int discriminator = extractDiscriminator(username);
    assertThat(discriminator).isGreaterThanOrEqualTo(100000).isLessThan(1000000);
  }

  @Test
  public void exhaustDiscriminator() {
    UsernameGenerator ug = new UsernameGenerator(1, 6, 10, TTL);
    Assertions.assertThrows(UsernameNotAvailableException.class, () -> {
      // allow greater than our max width
      ug.generateAvailableUsername("test", allowDiscriminator(d -> d >= 1000000));
    });
  }

  @Test
  public void randomCoverageMinWidth() throws UsernameNotAvailableException {
    UsernameGenerator ug = new UsernameGenerator(1, 6, 10, TTL);
    final Set<Integer> seen = new HashSet<>();
    for (int i = 0; i < 1000 && seen.size() < 9; i++) {
      seen.add(extractDiscriminator(ug.generateAvailableUsername("test", ignored -> true)));
    }
    // after 1K iterations, probability of a missed value is (9/10)^999
    assertThat(seen.size()).isEqualTo(9);
    assertThat(seen).allMatch(i -> i > 0 && i < 10);

  }

  @Test
  public void randomCoverageMidWidth() throws UsernameNotAvailableException {
    UsernameGenerator ug = new UsernameGenerator(1, 6, 10, TTL);
    final Set<Integer> seen = new HashSet<>();
    for (int i = 0; i < 100000 && seen.size() < 90; i++) {
      seen.add(extractDiscriminator(ug.generateAvailableUsername("test", allowDiscriminator(d -> d >= 10))));
    }
    // after 100K iterations, probability of a missed value is (99/100)^99999
    assertThat(seen.size()).isEqualTo(90);
    assertThat(seen).allMatch(i -> i >= 10 && i < 100);

  }

  private static Predicate<String> allowDiscriminator(Predicate<Integer> p) {
    return username -> p.test(extractDiscriminator(username));
  }

  private static int extractDiscriminator(final String username) {
    return Integer.parseInt(username.substring(username.indexOf(UsernameGenerator.SEPARATOR) + 1));
  }
}
