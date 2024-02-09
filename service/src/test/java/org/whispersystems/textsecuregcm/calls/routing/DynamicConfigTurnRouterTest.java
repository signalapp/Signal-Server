/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.calls.routing;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DynamicConfigTurnRouterTest {
  @Test
  public void testAlwaysSelectFirst() throws JsonProcessingException {
    final String configString = """
        captcha:
          scoreFloor: 1.0
        turn:
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

    final DynamicConfigTurnRouter configTurnRouter = new DynamicConfigTurnRouter(mockDynamicConfigManager);

    final long COUNT = 1000;

    final Map<String, Long> urlCounts = Stream
        .generate(configTurnRouter::randomUrls)
        .limit(COUNT)
        .flatMap(Collection::stream)
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
    final DynamicConfigTurnRouter configTurnRouter = new DynamicConfigTurnRouter(mockDynamicConfigManager);

    final long COUNT = 1000;

    final Map<String, Long> urlCounts = Stream
        .generate(configTurnRouter::randomUrls)
        .limit(COUNT)
        .flatMap(Collection::stream)
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
              enrolledAcis:
                - 732506d7-d04f-43a4-b1d7-8a3a91ebe8a6
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
    final DynamicConfigTurnRouter configTurnRouter = new DynamicConfigTurnRouter(mockDynamicConfigManager);

    List<String> urls = configTurnRouter.targetedUrls(UUID.fromString("732506d7-d04f-43a4-b1d7-8a3a91ebe8a6"));
    assertThat(urls.getFirst()).isEqualTo("enrolled.org");
    urls = configTurnRouter.targetedUrls(UUID.randomUUID());
    assertTrue(urls.isEmpty());
  }
}
