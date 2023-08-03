/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.event

import com.google.cloud.logging.Logging
import com.google.cloud.logging.LoggingOptions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.mockito.Mockito.mock

class GoogleCloudAdminEventLoggerTest {

    @Test
    fun logEvent() {
        val logging = mock(Logging::class.java)
        val logger = GoogleCloudAdminEventLogger(logging, "my-project", "test")

        val event = RemoteConfigDeleteEvent("token", "test")
        logger.logEvent(event)
    }

    @Test
    fun testGetService() {
        assertDoesNotThrow {
            // This is a canary for version conflicts between the cloud logging library and protobuf-java
            LoggingOptions.newBuilder()
                    .setProjectId("test")
                    .build()
                    .getService()
        }
    }
}
