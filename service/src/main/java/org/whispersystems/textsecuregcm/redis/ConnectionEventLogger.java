/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.redis;

import io.lettuce.core.cluster.event.ClusterTopologyChangedEvent;
import io.lettuce.core.event.connection.ConnectionEvent;
import io.lettuce.core.resource.ClientResources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionEventLogger {

    private static final Logger logger = LoggerFactory.getLogger(ConnectionEventLogger.class);

    public static void logConnectionEvents(final ClientResources clientResources) {
        clientResources.eventBus().get().subscribe(event -> {
            if (event instanceof ConnectionEvent) {
                logger.debug("Connection event: {}", event);
            } else if (event instanceof ClusterTopologyChangedEvent) {
                logger.info("Cluster topology changed: {}", event);
            }
        });
    }
}
