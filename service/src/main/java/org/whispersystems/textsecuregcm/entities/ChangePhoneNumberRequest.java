/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.validation.constraints.NotBlank;

public record ChangePhoneNumberRequest(@NotBlank String number,
                                       @NotBlank String code,
                                       @JsonProperty("reglock") @Nullable String registrationLock,
                                       @Nullable String pniIdentityKey,
                                       @Nullable List<IncomingMessage> deviceMessages,
                                       @Nullable Map<Long, SignedPreKey> devicePniSignedPrekeys,
                                       @Nullable Map<Long, Integer> pniRegistrationIds) {
}
