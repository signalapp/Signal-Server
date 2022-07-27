/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.entities;

import java.util.List;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public record IncomingMessageList(@NotNull @Valid List<@NotNull IncomingMessage> messages, boolean online, long timestamp) {
}
