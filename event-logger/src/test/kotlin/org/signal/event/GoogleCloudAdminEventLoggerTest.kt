/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.event

import com.google.cloud.logging.Logging
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock

class GoogleCloudAdminEventLoggerTest {

    @Test
    fun logEvent() {
        val logging = mock(Logging::class.java)
        val logger = GoogleCloudAdminEventLogger(logging, "my-project", "test")

        val event = RemoteConfigDeleteEvent("token", "test")
        logger.logEvent(event)
    }
}
