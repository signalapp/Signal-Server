/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.AssertTrue;
import jakarta.ws.rs.ClientErrorException;
import java.util.Base64;
import org.apache.http.HttpStatus;

public interface PhoneVerificationRequest {

  enum VerificationType {
    SESSION,
    RECOVERY_PASSWORD
  }

  String sessionId();

  byte[] recoveryPassword();

  // for the @AssertTrue to work with bean validation, method name must follow 'isSmth()'/'getSmth()' naming convention
  @AssertTrue
  @Schema(hidden = true)
  default boolean isValid() {
    // checking that exactly one of sessionId/recoveryPassword is non-empty
    return isNotBlank(sessionId()) ^ (recoveryPassword() != null && recoveryPassword().length > 0);
  }

  default PhoneVerificationRequest.VerificationType verificationType() {
    return isNotBlank(sessionId()) ? PhoneVerificationRequest.VerificationType.SESSION
        : PhoneVerificationRequest.VerificationType.RECOVERY_PASSWORD;
  }

  default byte[] decodeSessionId() {
    try {
      return Base64.getUrlDecoder().decode(sessionId());
    } catch (final IllegalArgumentException e) {
      throw new ClientErrorException("Malformed session ID", HttpStatus.SC_UNPROCESSABLE_ENTITY);
    }
  }
}
