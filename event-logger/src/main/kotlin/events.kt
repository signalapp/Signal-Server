/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.event

import java.util.Collections
import kotlinx.serialization.Serializable

sealed interface Event

@Serializable
data class RemoteConfigSetEvent(
        val token: String,
        val name: String,
        val percentage: Int,
        val defaultValue: String? = null,
        val value: String? = null,
        val hashKey: String? = null,
        val uuids: Collection<String> = Collections.emptyList(),
) : Event

@Serializable
data class RemoteConfigDeleteEvent(
        val token: String,
        val name: String,
) : Event
