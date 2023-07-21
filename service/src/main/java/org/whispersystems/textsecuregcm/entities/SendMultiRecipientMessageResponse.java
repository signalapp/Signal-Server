/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.annotations.VisibleForTesting;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.util.ServiceIdentifierAdapter;
import java.util.List;
import java.util.UUID;

public record SendMultiRecipientMessageResponse(@JsonSerialize(contentUsing = ServiceIdentifierAdapter.ServiceIdentifierSerializer.class)
                                                @JsonDeserialize(contentUsing = ServiceIdentifierAdapter.ServiceIdentifierDeserializer.class)
                                                List<ServiceIdentifier> uuids404) {
}
