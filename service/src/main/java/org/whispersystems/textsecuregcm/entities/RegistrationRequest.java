/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.Base64;
import javax.validation.Valid;
import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.NotNull;
import javax.ws.rs.ClientErrorException;
import org.apache.http.HttpStatus;
import org.whispersystems.textsecuregcm.util.ByteArrayAdapter;

public record RegistrationRequest(String sessionId,
                                  @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class) byte[] recoveryPassword,
                                  @NotNull @Valid AccountAttributes accountAttributes,
                                  boolean skipDeviceTransfer) {

  public enum VerificationType {
    SESSION,
    RECOVERY_PASSWORD
  }

  // for the @AssertTrue to work with bean validation, method name must follow 'isSmth()'/'getSmth()' naming convention
  @AssertTrue
  public boolean isValid() {
    // checking that exactly one of sessionId/recoveryPassword is non-empty
    return isNotBlank(sessionId) ^ (recoveryPassword != null && recoveryPassword.length > 0);
  }

  public VerificationType verificationType() {
    return isNotBlank(sessionId) ? VerificationType.SESSION : VerificationType.RECOVERY_PASSWORD;
  }

  public byte[] decodeSessionId() {
    try {
      return Base64.getUrlDecoder().decode(sessionId());
    } catch (final IllegalArgumentException e) {
      throw new ClientErrorException("Malformed session ID", HttpStatus.SC_UNPROCESSABLE_ENTITY);
    }
  }
}
