/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.AssertTrue;
import jakarta.ws.rs.ClientErrorException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.http.HttpStatus;

public interface PhoneVerificationRequest {

  enum VerificationType {
    SESSION,
    RECOVERY_PASSWORD,
    RECEIPT_CREDENTIAL
  }

  String sessionId();

  byte[] recoveryPassword();

  default byte[] receiptCredentialPresentation() {
    return null;
  }

  // for the @AssertTrue to work with bean validation, method name must follow 'isSmth()'/'getSmth()' naming convention
  @AssertTrue
  @Schema(hidden = true)
  default boolean isValid() {
    // exactly one of sessionId/recoveryPassword/receiptCredentialPresentation should be present
    return presentVerificationTypes().size() == 1;
  }

  default PhoneVerificationRequest.VerificationType verificationType() {
    return presentVerificationTypes().get(0);
  }

  default List<VerificationType> presentVerificationTypes() {
    final List<VerificationType> types = new ArrayList<>(1);
    if (isNotBlank(sessionId())) {
      types.add(VerificationType.SESSION);
    }
    if (ArrayUtils.isNotEmpty(recoveryPassword())) {
      types.add(VerificationType.RECOVERY_PASSWORD);
    }
    if (ArrayUtils.isNotEmpty(receiptCredentialPresentation())) {
      types.add(VerificationType.RECEIPT_CREDENTIAL);
    }
    return types;
  }

  default byte[] decodeSessionId() {
    try {
      return Base64.getUrlDecoder().decode(sessionId());
    } catch (final IllegalArgumentException e) {
      throw new ClientErrorException("Malformed session ID", HttpStatus.SC_UNPROCESSABLE_ENTITY);
    }
  }
}
