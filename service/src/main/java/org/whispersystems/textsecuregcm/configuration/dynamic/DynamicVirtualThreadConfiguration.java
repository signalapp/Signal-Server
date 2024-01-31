/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration.dynamic;

import java.util.Set;

public record DynamicVirtualThreadConfiguration(Set<String> allowedPinEvents) {}
