/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import java.util.Optional;
import java.util.UUID;

public interface ReportedMessageListener {

  void handleMessageReported(String sourceNumber, UUID messageGuid, UUID reporterUuid, Optional<byte[]> reportSpamToken);
}
