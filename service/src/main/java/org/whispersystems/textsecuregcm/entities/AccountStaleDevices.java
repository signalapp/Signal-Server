/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.util.ServiceIdentifierAdapter;

public record AccountStaleDevices(@JsonSerialize(using = ServiceIdentifierAdapter.ServiceIdentifierSerializer.class)
                                  @JsonDeserialize(using = ServiceIdentifierAdapter.ServiceIdentifierDeserializer.class)
                                  ServiceIdentifier uuid,

                                  StaleDevicesResponse devices) {
}
