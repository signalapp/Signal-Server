/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util.logging;

import ch.qos.logback.access.spi.IAccessEvent;
import ch.qos.logback.core.filter.Filter;
import org.whispersystems.websocket.logging.WebsocketEvent;

public class RequestLogManager {
    private static final RequestLogEnabledFilter<IAccessEvent> HTTP_REQUEST_LOG_FILTER = new RequestLogEnabledFilter<>();

    static Filter<IAccessEvent> getHttpRequestLogFilter() {
        return HTTP_REQUEST_LOG_FILTER;
    }

    public static void setRequestLoggingEnabled(final boolean enabled) {
        HTTP_REQUEST_LOG_FILTER.setRequestLoggingEnabled(enabled);
    }
}
