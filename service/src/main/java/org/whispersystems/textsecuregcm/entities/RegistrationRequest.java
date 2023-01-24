/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import org.whispersystems.textsecuregcm.util.ByteArrayAdapter;

public record RegistrationRequest(String sessionId,
                                  @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class) byte[] recoveryPassword,
                                  @NotNull @Valid AccountAttributes accountAttributes,
                                  boolean skipDeviceTransfer) implements PhoneVerificationRequest {


}
