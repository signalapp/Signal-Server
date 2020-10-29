/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import io.dropwizard.servlets.tasks.Task;
import org.whispersystems.textsecuregcm.util.logging.RequestLogManager;

import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

public class DisableRequestLoggingTask extends Task {

    public DisableRequestLoggingTask() {
        super("disable-request-logging");
    }

    @Override
    public void execute(final Map<String, List<String>> map, final PrintWriter printWriter) {
        RequestLogManager.setRequestLoggingEnabled(false);
    }
}
