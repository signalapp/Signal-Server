/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import io.dropwizard.servlets.tasks.Task;
import org.whispersystems.textsecuregcm.storage.AccountDatabaseCrawlerCache;

import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

public class SetCrawlerAccelerationTask extends Task {

    private final AccountDatabaseCrawlerCache crawlerCache;

    public SetCrawlerAccelerationTask(final AccountDatabaseCrawlerCache crawlerCache) {
        super("set-crawler-accelerated");

        this.crawlerCache = crawlerCache;
    }

    @Override
    public void execute(final Map<String, List<String>> parameters, final PrintWriter out) {
        if (parameters.containsKey("accelerated") && parameters.get("accelerated").size() == 1) {
            final boolean accelerated = "true".equalsIgnoreCase(parameters.get("accelerated").get(0));

            crawlerCache.setAccelerated(accelerated);
            out.println("Set accelerated: " + accelerated);
        } else {
            out.println("Usage: set-crawler-accelerated?accelerated=[true|false]");
        }
    }
}
