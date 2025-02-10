/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import io.swagger.v3.oas.annotations.media.Schema;

import java.util.List;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.util.ServiceIdentifierAdapter;

public record SendMultiRecipientMessageResponse(
    @Schema(description = "a list of the service identifiers in the request that do not correspond to registered Signal users; will only be present if a group send endorsement was supplied for the request")
    @JsonSerialize(contentUsing = ServiceIdentifierAdapter.ServiceIdentifierSerializer.class)
    @JsonDeserialize(contentUsing = ServiceIdentifierAdapter.ServiceIdentifierDeserializer.class)
    List<ServiceIdentifier> uuids404) {
}
