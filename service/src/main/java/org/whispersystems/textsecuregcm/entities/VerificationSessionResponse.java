/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import java.util.List;
import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.registration.VerificationSession;

public record VerificationSessionResponse(String id, @Nullable Long nextSms, @Nullable Long nextCall,
                                          @Nullable Long nextVerificationAttempt, boolean allowedToRequestCode,
                                          List<VerificationSession.Information> requestedInformation,
                                          boolean verified) {

}
