/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import java.util.Base64;
import javax.annotation.Nullable;
import org.signal.registration.rpc.RegistrationSessionMetadata;

public record RegistrationServiceSession(byte[] id,
                                         String number,
                                         boolean verified,
                                         @Nullable Long nextSms,
                                         @Nullable Long nextVoiceCall,
                                         @Nullable Long nextVerificationAttempt,
                                         long expiration) {


  public String encodedSessionId() {
    return encodeSessionId(id);
  }

  public static String encodeSessionId(final byte[] sessionId) {
    return Base64.getUrlEncoder().encodeToString(sessionId);
  }

  public RegistrationServiceSession(byte[] id, String number, RegistrationSessionMetadata remoteSession) {
    this(id, number, remoteSession.getVerified(),
        remoteSession.getMayRequestSms() ? remoteSession.getNextSmsSeconds() : null,
        remoteSession.getMayRequestVoiceCall() ? remoteSession.getNextVoiceCallSeconds() : null,
        remoteSession.getMayCheckCode() ? remoteSession.getNextCodeCheckSeconds() : null,
        remoteSession.getExpirationSeconds());
  }

}
