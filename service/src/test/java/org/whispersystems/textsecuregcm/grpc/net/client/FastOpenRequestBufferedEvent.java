/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net.client;

import io.netty.buffer.ByteBuf;

public record FastOpenRequestBufferedEvent(ByteBuf fastOpenRequest) {}
