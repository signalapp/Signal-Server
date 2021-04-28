/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.lifecycle.Managed;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.TorExitNodeConfiguration;
import org.whispersystems.textsecuregcm.http.FaultTolerantHttpClient;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * A utility for checking whether IP addresses belong to Tor exit nodes using the "bulk exit list."
 *
 * @see <a href="https://blog.torproject.org/changes-tor-exit-list-service">Changes to the Tor Exit List Service</a>
 */
public class TorExitNodeManager implements Managed {

  private final ScheduledExecutorService refreshScheduledExecutorService;
  private final Duration refreshDelay;
  private ScheduledFuture<?> refreshFuture;

  private final FaultTolerantHttpClient refreshClient;
  private final URI exitNodeListUri;

  private final AtomicReference<String> lastEtag = new AtomicReference<>(null);
  private final AtomicReference<Set<String>> exitNodeAddresses = new AtomicReference<>(Collections.emptySet());

  private static final Timer REFRESH_TIMER = Metrics.timer(name(TorExitNodeManager.class, "refresh"));
  private static final Counter REFRESH_ERRORS = Metrics.counter(name(TorExitNodeManager.class, "refreshErrors"));

  private static final Logger log = LoggerFactory.getLogger(TorExitNodeManager.class);

  public TorExitNodeManager(final ScheduledExecutorService scheduledExecutorService, final ExecutorService clientExecutorService, final TorExitNodeConfiguration configuration) {
    this.refreshScheduledExecutorService = scheduledExecutorService;

    this.exitNodeListUri = URI.create(configuration.getListUrl());
    this.refreshDelay = configuration.getRefreshInterval();

    refreshClient = FaultTolerantHttpClient.newBuilder()
        .withCircuitBreaker(configuration.getCircuitBreakerConfiguration())
        .withRetry(configuration.getRetryConfiguration())
        .withVersion(HttpClient.Version.HTTP_1_1)
        .withConnectTimeout(Duration.ofSeconds(10))
        .withRedirect(HttpClient.Redirect.NEVER)
        .withExecutor(clientExecutorService)
        .withName("tor-exit-node")
        .withSecurityProtocol(FaultTolerantHttpClient.SECURITY_PROTOCOL_TLS_1_3)
        .build();
  }

  public boolean isTorExitNode(final String address) {
    return exitNodeAddresses.get().contains(address);
  }

  @VisibleForTesting
  CompletableFuture<?> refresh() {
    final String etag = lastEtag.get();

    final HttpRequest request;
    {
      final HttpRequest.Builder builder = HttpRequest.newBuilder().GET().uri(exitNodeListUri);

      if (StringUtils.isNotBlank(etag)) {
        builder.header("If-None-Match", etag);
      }

      request = builder.build();
    }

    final long start = System.nanoTime();

    return refreshClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
        .whenComplete((response, cause) -> {
          REFRESH_TIMER.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);

          if (cause != null) {
            REFRESH_ERRORS.increment();
            log.warn("Failed to refresh Tor exit node list", cause);
          } else {
            if (response.statusCode() == 200) {
              exitNodeAddresses.set(response.body().lines().collect(Collectors.toSet()));
              response.headers().firstValue("ETag").ifPresent(newEtag -> lastEtag.compareAndSet(etag, newEtag));
            } else if (response.statusCode() != 304) {
              REFRESH_ERRORS.increment();
              log.warn("Failed to refresh Tor exit node list: {} ({})", response.statusCode(), response.body());
            }
          }
        });
  }

  @Override
  public synchronized void start() {
    if (refreshFuture != null) {
      refreshFuture.cancel(true);
    }

    refreshFuture = refreshScheduledExecutorService
        .scheduleAtFixedRate(this::refresh, 0, refreshDelay.toMillis(), TimeUnit.MILLISECONDS);
  }

  @Override
  public synchronized void stop() {
    if (refreshFuture != null) {
      refreshFuture.cancel(true);
    }
  }
}
