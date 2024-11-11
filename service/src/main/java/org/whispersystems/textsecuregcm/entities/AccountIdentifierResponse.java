/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import jakarta.validation.constraints.NotNull;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.util.ServiceIdentifierAdapter;

public record AccountIdentifierResponse(@NotNull
                                        @JsonSerialize(using = ServiceIdentifierAdapter.ServiceIdentifierSerializer.class)
                                        @JsonDeserialize(using = ServiceIdentifierAdapter.AciServiceIdentifierDeserializer.class)
                                        AciServiceIdentifier uuid) {}
