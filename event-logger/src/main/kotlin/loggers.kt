/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.event

interface Logger {
    fun logEvent(event: Event, labels: Map<String, String>?)
    fun logEvent(event: Event) = logEvent(event, null)
}

class NoOpLogger : Logger {
    override fun logEvent(event: Event, labels: Map<String, String>?) {}
}
