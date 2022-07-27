/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.entities;

public record IncomingMessage(int type, String destination, long destinationDeviceId, int destinationRegistrationId,
                              String content) {
}
