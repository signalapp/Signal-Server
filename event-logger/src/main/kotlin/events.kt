/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.event

import java.util.Collections
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import kotlinx.serialization.modules.subclass

val module = SerializersModule {
    polymorphic(Event::class) {
        subclass(RemoteConfigSetEvent::class)
        subclass(RemoteConfigDeleteEvent::class)
    }
}
val jsonFormat = Json { serializersModule = module }

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
