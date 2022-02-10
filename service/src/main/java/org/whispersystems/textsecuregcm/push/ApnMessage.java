/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.UUID;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class ApnMessage {

  public enum Type {
    NOTIFICATION, CHALLENGE, RATE_LIMIT_CHALLENGE
  }

  public static final String APN_VOIP_NOTIFICATION_PAYLOAD    = "{\"aps\":{\"sound\":\"default\",\"alert\":{\"loc-key\":\"APN_Message\"}}}";
  public static final String APN_NSE_NOTIFICATION_PAYLOAD     = "{\"aps\":{\"mutable-content\":1,\"alert\":{\"loc-key\":\"APN_Message\"}}}";
  public static final String APN_CHALLENGE_PAYLOAD            = "{\"aps\":{\"sound\":\"default\",\"alert\":{\"loc-key\":\"APN_Message\"}}, \"challenge\" : \"%s\"}";
  public static final String APN_RATE_LIMIT_CHALLENGE_PAYLOAD = "{\"aps\":{\"sound\":\"default\",\"alert\":{\"loc-key\":\"APN_Message\"}}, \"rateLimitChallenge\" : \"%s\"}";
  public static final long   MAX_EXPIRATION                   = Integer.MAX_VALUE * 1000L;

  private final String           apnId;
  private final long             deviceId;
  private final boolean          isVoip;
  private final Type             type;
  private final Optional<String> challengeData;

  @Nullable
  private final UUID             uuid;

  public ApnMessage(String apnId, @Nullable UUID uuid, long deviceId, boolean isVoip, Type type, Optional<String> challengeData) {
    this.apnId         = apnId;
    this.uuid          = uuid;
    this.deviceId      = deviceId;
    this.isVoip        = isVoip;
    this.type          = type;
    this.challengeData = challengeData;
  }
  
  public boolean isVoip() {
    return isVoip;
  }

  public String getApnId() {
    return apnId;
  }

  public String getMessage() {
    switch (type) {
      case NOTIFICATION:
        return this.isVoip() ? APN_VOIP_NOTIFICATION_PAYLOAD : APN_NSE_NOTIFICATION_PAYLOAD;

      case CHALLENGE:
        return String.format(APN_CHALLENGE_PAYLOAD, challengeData.orElseThrow(AssertionError::new));

      case RATE_LIMIT_CHALLENGE:
        return String.format(APN_RATE_LIMIT_CHALLENGE_PAYLOAD, challengeData.orElseThrow(AssertionError::new));

      default:
        throw new AssertionError();
    }
  }

  @Nullable
  public String getCollapseId() {
    if (type == Type.NOTIFICATION && !isVoip) {
      return "incoming-message";
    }
    return null;
  }

  @VisibleForTesting
  public Optional<String> getChallengeData() {
    return challengeData;
  }

  public long getExpirationTime() {
    return MAX_EXPIRATION;
  }

  public Optional<UUID> getUuid() {
    return Optional.ofNullable(uuid);
  }

  public long getDeviceId() {
    return deviceId;
  }
}
