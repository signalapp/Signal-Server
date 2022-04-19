/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;


import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.auth.AuthenticationCredentials;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;
import org.whispersystems.textsecuregcm.util.Util;

public class Device {

  public static final long MASTER_ID = 1;

  @JsonProperty
  private long    id;

  @JsonProperty
  private String  name;

  @JsonProperty
  private String  authToken;

  @JsonProperty
  private String  salt;

  @JsonProperty
  private String  gcmId;

  @JsonProperty
  private String  apnId;

  @JsonProperty
  private String  voipApnId;

  @JsonProperty
  private long pushTimestamp;

  @JsonProperty
  private long uninstalledFeedback;

  @JsonProperty
  private boolean fetchesMessages;

  @JsonProperty
  private int registrationId;

  @JsonProperty
  private SignedPreKey signedPreKey;

  @JsonProperty("pniSignedPreKey")
  private SignedPreKey phoneNumberIdentitySignedPreKey;

  @JsonProperty
  private long lastSeen;

  @JsonProperty
  private long created;

  @JsonProperty
  private String userAgent;

  @JsonProperty
  private DeviceCapabilities capabilities;

  public Device() {}

  public Device(long id, String name, String authToken, String salt,
      String gcmId, String apnId,
      String voipApnId, boolean fetchesMessages,
      int registrationId, SignedPreKey signedPreKey,
      long lastSeen, long created, String userAgent,
      long uninstalledFeedback, DeviceCapabilities capabilities) {
    this.id = id;
    this.name = name;
    this.authToken = authToken;
    this.salt = salt;
    this.gcmId = gcmId;
    this.apnId = apnId;
    this.voipApnId = voipApnId;
    this.fetchesMessages = fetchesMessages;
    this.registrationId = registrationId;
    this.signedPreKey = signedPreKey;
    this.lastSeen = lastSeen;
    this.created = created;
    this.userAgent = userAgent;
    this.uninstalledFeedback = uninstalledFeedback;
    this.capabilities = capabilities;
  }

  public String getApnId() {
    return apnId;
  }

  public void setApnId(String apnId) {
    this.apnId = apnId;

    if (apnId != null) {
      this.pushTimestamp = System.currentTimeMillis();
    }
  }

  public String getVoipApnId() {
    return voipApnId;
  }

  public void setVoipApnId(String voipApnId) {
    this.voipApnId = voipApnId;
  }

  public void setUninstalledFeedbackTimestamp(long uninstalledFeedback) {
    this.uninstalledFeedback = uninstalledFeedback;
  }

  public long getUninstalledFeedbackTimestamp() {
    return uninstalledFeedback;
  }

  public void setLastSeen(long lastSeen) {
    this.lastSeen = lastSeen;
  }

  public long getLastSeen() {
    return lastSeen;
  }

  public void setCreated(long created) {
    this.created = created;
  }

  public long getCreated() {
    return this.created;
  }

  public String getGcmId() {
    return gcmId;
  }

  public void setGcmId(String gcmId) {
    this.gcmId = gcmId;

    if (gcmId != null) {
      this.pushTimestamp = System.currentTimeMillis();
    }
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setAuthenticationCredentials(AuthenticationCredentials credentials) {
    this.authToken = credentials.getHashedAuthenticationToken();
    this.salt      = credentials.getSalt();
  }

  public AuthenticationCredentials getAuthenticationCredentials() {
    return new AuthenticationCredentials(authToken, salt);
  }

  @Nullable
  public DeviceCapabilities getCapabilities() {
    return capabilities;
  }

  public void setCapabilities(DeviceCapabilities capabilities) {
    this.capabilities = capabilities;
  }

  public boolean isEnabled() {
    boolean hasChannel = fetchesMessages || !Util.isEmpty(getApnId()) || !Util.isEmpty(getGcmId());

    return (id == MASTER_ID && hasChannel && signedPreKey != null) ||
           (id != MASTER_ID && hasChannel && signedPreKey != null && lastSeen > (System.currentTimeMillis() - TimeUnit.DAYS.toMillis(30)));
  }

  public boolean getFetchesMessages() {
    return fetchesMessages;
  }

  public void setFetchesMessages(boolean fetchesMessages) {
    this.fetchesMessages = fetchesMessages;
  }

  public boolean isMaster() {
    return getId() == MASTER_ID;
  }

  public int getRegistrationId() {
    return registrationId;
  }

  public void setRegistrationId(int registrationId) {
    this.registrationId = registrationId;
  }

  public SignedPreKey getSignedPreKey() {
    return signedPreKey;
  }

  public void setSignedPreKey(SignedPreKey signedPreKey) {
    this.signedPreKey = signedPreKey;
  }

  public SignedPreKey getPhoneNumberIdentitySignedPreKey() {
    return phoneNumberIdentitySignedPreKey;
  }

  public void setPhoneNumberIdentitySignedPreKey(final SignedPreKey phoneNumberIdentitySignedPreKey) {
    this.phoneNumberIdentitySignedPreKey = phoneNumberIdentitySignedPreKey;
  }

  public long getPushTimestamp() {
    return pushTimestamp;
  }

  public void setUserAgent(String userAgent) {
    this.userAgent = userAgent;
  }

  public String getUserAgent() {
    return this.userAgent;
  }

  public boolean isGroupsV2Supported() {
    final boolean groupsV2Supported;

    if (this.capabilities != null) {
      final boolean ios = this.apnId != null || this.voipApnId != null;

      groupsV2Supported = this.capabilities.isGv2_3() || (ios && this.capabilities.isGv2_2());
    } else {
      groupsV2Supported = false;
    }

    return groupsV2Supported;
  }

  @Override
  public boolean equals(Object other) {
    return (other instanceof Device that) && this.id == that.id;
  }

  @Override
  public int hashCode() {
    return (int)this.id;
  }

  public static class DeviceCapabilities {
    @JsonProperty
    private boolean gv2;

    @JsonProperty("gv2-2")
    private boolean gv2_2;

    @JsonProperty("gv2-3")
    private boolean gv2_3;

    @JsonProperty
    private boolean storage;

    @JsonProperty
    private boolean transfer;

    @JsonProperty("gv1-migration")
    private boolean gv1Migration;

    @JsonProperty
    private boolean senderKey;

    @JsonProperty
    private boolean announcementGroup;

    @JsonProperty
    private boolean changeNumber;

    @JsonProperty
    private boolean pni;

    @JsonProperty
    private boolean stories;

    @JsonProperty
    private boolean giftBadges;

    public DeviceCapabilities() {
    }

    public DeviceCapabilities(boolean gv2, final boolean gv2_2, final boolean gv2_3, boolean storage, boolean transfer,
        boolean gv1Migration, final boolean senderKey, final boolean announcementGroup, final boolean changeNumber,
        final boolean pni, final boolean stories, final boolean giftBadges) {
      this.gv2 = gv2;
      this.gv2_2 = gv2_2;
      this.gv2_3 = gv2_3;
      this.storage = storage;
      this.transfer = transfer;
      this.gv1Migration = gv1Migration;
      this.senderKey = senderKey;
      this.announcementGroup = announcementGroup;
      this.changeNumber = changeNumber;
      this.pni = pni;
      this.stories = stories;
      this.giftBadges = giftBadges;
    }

    public boolean isGv2() {
      return gv2;
    }

    public boolean isGv2_2() {
      return gv2_2;
    }

    public boolean isGv2_3() {
      return gv2_3;
    }

    public boolean isStorage() {
      return storage;
    }

    public boolean isTransfer() {
      return transfer;
    }

    public boolean isGv1Migration() {
      return gv1Migration;
    }

    public boolean isSenderKey() {
      return senderKey;
    }

    public boolean isAnnouncementGroup() {
      return announcementGroup;
    }

    public boolean isChangeNumber() {
      return changeNumber;
    }

    public boolean isPni() {
      return pni;
    }

    public boolean isStories() {
      return stories;
    }

    public boolean isGiftBadges() {
      return giftBadges;
    }
  }
}
