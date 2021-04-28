/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.matching.StringValuePattern;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.whispersystems.textsecuregcm.configuration.TorExitNodeConfiguration;
import org.whispersystems.textsecuregcm.redis.AbstractRedisClusterTest;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.junit.Assert.*;

public class TorExitNodeManagerTest extends AbstractRedisClusterTest {

  private ScheduledExecutorService scheduledExecutorService;
  private ExecutorService clientExecutorService;

  private TorExitNodeManager torExitNodeManager;

  private static final String LIST_PATH = "/list";

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(options().dynamicPort().dynamicHttpsPort());

  @Before
  public void setUp() throws Exception {
    final TorExitNodeConfiguration configuration = new TorExitNodeConfiguration();
    configuration.setListUrl("http://localhost:" + wireMockRule.port() + LIST_PATH);

    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    clientExecutorService = Executors.newSingleThreadExecutor();

    torExitNodeManager = new TorExitNodeManager(scheduledExecutorService, clientExecutorService, configuration);
  }

  @After
  public void tearDown() throws Exception {
    scheduledExecutorService.shutdown();
    scheduledExecutorService.awaitTermination(1, TimeUnit.SECONDS);

    clientExecutorService.shutdown();
    clientExecutorService.awaitTermination(1, TimeUnit.SECONDS);
  }

  @Test
  public void testIsTorExitNode() {
    assertFalse(torExitNodeManager.isTorExitNode("10.0.0.1"));
    assertFalse(torExitNodeManager.isTorExitNode("10.0.0.2"));

    final String etag = UUID.randomUUID().toString();

    wireMockRule.stubFor(get(urlEqualTo(LIST_PATH))
        .willReturn(aResponse()
            .withHeader("ETag", etag)
            .withBody("10.0.0.1\n10.0.0.2")));

    torExitNodeManager.refresh().join();

    verify(getRequestedFor(urlEqualTo(LIST_PATH)).withoutHeader("If-None-Match"));

    assertTrue(torExitNodeManager.isTorExitNode("10.0.0.1"));
    assertTrue(torExitNodeManager.isTorExitNode("10.0.0.2"));
    assertFalse(torExitNodeManager.isTorExitNode("10.0.0.3"));

    wireMockRule.stubFor(get(urlEqualTo(LIST_PATH)).withHeader("If-None-Match", equalTo(etag))
        .willReturn(aResponse().withStatus(304)));

    torExitNodeManager.refresh().join();

    verify(getRequestedFor(urlEqualTo(LIST_PATH)).withHeader("If-None-Match", equalTo(etag)));

    assertTrue(torExitNodeManager.isTorExitNode("10.0.0.1"));
    assertTrue(torExitNodeManager.isTorExitNode("10.0.0.2"));
    assertFalse(torExitNodeManager.isTorExitNode("10.0.0.3"));
  }
}
