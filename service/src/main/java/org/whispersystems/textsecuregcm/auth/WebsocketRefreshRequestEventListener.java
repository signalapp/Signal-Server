/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import jakarta.ws.rs.container.ResourceInfo;
import jakarta.ws.rs.core.Context;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.glassfish.jersey.server.monitoring.RequestEvent.Type;
import org.glassfish.jersey.server.monitoring.RequestEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebsocketRefreshRequestEventListener implements RequestEventListener {

  private final DisconnectionRequestManager disconnectionRequestManager;
  private final WebsocketRefreshRequirementProvider[] providers;

  private static final Counter DISPLACED_ACCOUNTS = Metrics.counter(
      name(WebsocketRefreshRequestEventListener.class, "displacedAccounts"));

  private static final Counter DISPLACED_DEVICES = Metrics.counter(
      name(WebsocketRefreshRequestEventListener.class, "displacedDevices"));

  private static final Logger logger = LoggerFactory.getLogger(WebsocketRefreshRequestEventListener.class);

  public WebsocketRefreshRequestEventListener(
      final DisconnectionRequestManager disconnectionRequestManager,
      final WebsocketRefreshRequirementProvider... providers) {

    this.disconnectionRequestManager = disconnectionRequestManager;
    this.providers = providers;
  }

  @Context
  private ResourceInfo resourceInfo;

  @Override
  public void onEvent(final RequestEvent event) {
    if (event.getType() == Type.REQUEST_FILTERED) {
      for (final WebsocketRefreshRequirementProvider provider : providers) {
        provider.handleRequestFiltered(event);
      }
    } else if (event.getType() == Type.FINISHED) {
      final AtomicInteger displacedDevices = new AtomicInteger(0);

      Arrays.stream(providers)
          .flatMap(provider -> provider.handleRequestFinished(event).stream())
          .distinct()
          .forEach(pair -> {
            try {
              displacedDevices.incrementAndGet();
              disconnectionRequestManager.requestDisconnection(pair.first(), List.of(pair.second()));
            } catch (final Exception e) {
              logger.error("Could not displace device presence", e);
            }
          });

      if (displacedDevices.get() > 0) {
        DISPLACED_ACCOUNTS.increment();
        DISPLACED_DEVICES.increment(displacedDevices.get());
      }
    }
  }
}
