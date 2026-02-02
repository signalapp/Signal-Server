/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.registration;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.List;
import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.storage.SerializedExpireableJsonDynamoStore;
import org.whispersystems.textsecuregcm.telephony.CarrierData;

/**
 * Server-internal stored session object. Primarily used by
 * {@link org.whispersystems.textsecuregcm.controllers.VerificationController} to manage the steps required to begin
 * requesting codes from Registration Service, in order to get a verified session to be provided to
 * {@link org.whispersystems.textsecuregcm.controllers.RegistrationController}.
 *
 * @param sessionId               the session ID returned by Registration Service
 * @param pushChallenge           the value of a push challenge sent to a client, after it submitted a push token
 * @param carrierData             information about the phone number's carrier if available
 * @param requestedInformation    information requested that a client send to the server
 * @param submittedInformation    information that a client has submitted and that the server has verified
 * @param smsSenderOverride       if present, indicates a sender override argument that should be forwarded to the
 *                                Registration Service when requesting a code
 * @param voiceSenderOverride     if present, indicates a sender override argument that should be forwarded to the
 *                                Registration Service when requesting a code
 * @param allowedToRequestCode    whether the client is allowed to request a code. This request will be forwarded to
 *                                Registration Service
 * @param createdTimestamp        when this session was created
 * @param updatedTimestamp        when this session was updated
 * @param remoteExpirationSeconds when the remote
 *                                {@link org.whispersystems.textsecuregcm.entities.RegistrationServiceSession} expires
 * @see org.whispersystems.textsecuregcm.entities.RegistrationServiceSession
 * @see org.whispersystems.textsecuregcm.entities.VerificationSessionResponse
 */
public record VerificationSession(
    String sessionId,
    @Nullable String pushChallenge,
    @Nullable CarrierData carrierData,
    List<Information> requestedInformation,
    List<Information> submittedInformation,
    @Nullable String smsSenderOverride,
    @Nullable String voiceSenderOverride,
    boolean allowedToRequestCode,
    long createdTimestamp,
    long updatedTimestamp,
    long remoteExpirationSeconds) implements SerializedExpireableJsonDynamoStore.Expireable {

  @Override
  public long getExpirationEpochSeconds() {
    return Instant.ofEpochMilli(updatedTimestamp).plusSeconds(remoteExpirationSeconds).getEpochSecond();
  }

  public enum Information {
    @JsonProperty("pushChallenge")
    PUSH_CHALLENGE,
    @JsonProperty("captcha")
    CAPTCHA
  }
}
