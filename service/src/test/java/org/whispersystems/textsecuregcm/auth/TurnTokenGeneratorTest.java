package org.whispersystems.textsecuregcm.auth;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TurnTokenGeneratorTest {

  @Test
  public void testAlwaysSelectFirst() throws JsonProcessingException {
    final String configString = """
        captcha:
          scoreFloor: 1.0
        turn:
          secret: bloop
          uriConfigs:
            - uris:
                - always1.org
                - always2.org
            - uris:
                - never.org
              weight: 0
         """;
    DynamicConfiguration config = DynamicConfigurationManager
        .parseConfiguration(configString, DynamicConfiguration.class)
        .orElseThrow();

    @SuppressWarnings("unchecked")
    DynamicConfigurationManager<DynamicConfiguration> mockDynamicConfigManager = mock(
        DynamicConfigurationManager.class);

    when(mockDynamicConfigManager.getConfiguration()).thenReturn(config);
    final TurnTokenGenerator turnTokenGenerator = new TurnTokenGenerator(mockDynamicConfigManager);

    final long COUNT = 1000;

    final Map<String, Long> urlCounts = Stream
        .generate(() -> turnTokenGenerator.generate(""))
        .limit(COUNT)
        .flatMap(token -> token.getUrls().stream())
        .collect(Collectors.groupingBy(i -> i, Collectors.counting()));

    assertThat(urlCounts.get("always1.org")).isEqualTo(COUNT);
    assertThat(urlCounts.get("always2.org")).isEqualTo(COUNT);
    assertThat(urlCounts).doesNotContainKey("never.org");
  }

  @Test
  public void testProbabilisticUrls() throws JsonProcessingException {
    final String configString = """
        captcha:
          scoreFloor: 1.0
        turn:
          secret: bloop
          uriConfigs:
            - uris:
                - always.org
                - sometimes1.org
              weight: 5
            - uris:
                - always.org
                - sometimes2.org
              weight: 5
         """;
    DynamicConfiguration config = DynamicConfigurationManager
        .parseConfiguration(configString, DynamicConfiguration.class)
        .orElseThrow();

    @SuppressWarnings("unchecked")
    DynamicConfigurationManager<DynamicConfiguration> mockDynamicConfigManager = mock(
        DynamicConfigurationManager.class);

    when(mockDynamicConfigManager.getConfiguration()).thenReturn(config);
    final TurnTokenGenerator turnTokenGenerator = new TurnTokenGenerator(mockDynamicConfigManager);

    final long COUNT = 1000;

    final Map<String, Long> urlCounts = Stream
        .generate(() -> turnTokenGenerator.generate(""))
        .limit(COUNT)
        .flatMap(token -> token.getUrls().stream())
        .collect(Collectors.groupingBy(i -> i, Collectors.counting()));

    assertThat(urlCounts.get("always.org")).isEqualTo(COUNT);
    assertThat(urlCounts.get("sometimes1.org")).isGreaterThan(0);
    assertThat(urlCounts.get("sometimes2.org")).isGreaterThan(0);
  }

  @Test
  public void testExplicitEnrollment() throws JsonProcessingException {
    final String configString = """
        captcha:
          scoreFloor: 1.0
        turn:
          secret: bloop
          uriConfigs:
            - uris:
                - enrolled.org
              weight: 0
              enrolledNumbers:
                - +15555555555
            - uris:
                - unenrolled.org
              weight: 1
         """;
    DynamicConfiguration config = DynamicConfigurationManager
        .parseConfiguration(configString, DynamicConfiguration.class)
        .orElseThrow();

    @SuppressWarnings("unchecked")
    DynamicConfigurationManager<DynamicConfiguration> mockDynamicConfigManager = mock(
        DynamicConfigurationManager.class);

    when(mockDynamicConfigManager.getConfiguration()).thenReturn(config);

    final TurnTokenGenerator turnTokenGenerator = new TurnTokenGenerator(mockDynamicConfigManager);
    TurnToken token = turnTokenGenerator.generate("+15555555555");
    assertThat(token.getUrls().get(0)).isEqualTo("enrolled.org");
    token = turnTokenGenerator.generate("+15555555556");
    assertThat(token.getUrls().get(0)).isEqualTo("unenrolled.org");

  }

}
