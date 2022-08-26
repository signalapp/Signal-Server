/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.event

import com.google.cloud.logging.LogEntry
import com.google.cloud.logging.Logging
import com.google.cloud.logging.Payload.JsonPayload
import com.google.cloud.logging.Severity
import com.google.protobuf.Struct
import com.google.protobuf.util.JsonFormat
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json

interface AdminEventLogger {
    fun logEvent(event: Event, labels: Map<String, String>?)
    fun logEvent(event: Event) = logEvent(event, null)
}

class NoOpAdminEventLogger : AdminEventLogger {
    override fun logEvent(event: Event, labels: Map<String, String>?) {}
}

class GoogleCloudAdminEventLogger(private val logging: Logging, private val logName: String) : AdminEventLogger {
    override fun logEvent(event: Event, labels: Map<String, String>?) {
        val structBuilder = Struct.newBuilder()
        JsonFormat.parser().merge(Json.encodeToString(event), structBuilder)
        val struct = structBuilder.build()

        val logEntryBuilder = LogEntry.newBuilder(JsonPayload.of(struct))
                .setLogName(logName)
                .setSeverity(Severity.NOTICE);
        if (labels != null) {
            logEntryBuilder.setLabels(labels);
        }
        logging.write(listOf(logEntryBuilder.build()))
    }
}
