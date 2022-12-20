/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;


import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.OptionalInt;
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

  @Nullable
  @JsonProperty("pniRegistrationId")
  private Integer phoneNumberIdentityRegistrationId;

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

  /**
   * Has this device been manually locked?
   *
   * We lock a device by prepending "!" to its token.
   * This character cannot normally appear in valid tokens.
   *
   * @return true if the credential was locked, false otherwise.
   */
  public boolean hasLockedCredentials() {
    AuthenticationCredentials auth = getAuthenticationCredentials();
    return auth.getHashedAuthenticationToken().startsWith("!");
  }

  /**
   * Lock device by invalidating authentication tokens.
   *
   * This should only be used from Account::lockAuthenticationCredentials.
   *
   * See that method for more information.
   */
  public void lockAuthenticationCredentials() {
    AuthenticationCredentials oldAuth = getAuthenticationCredentials();
    String token = "!" + oldAuth.getHashedAuthenticationToken();
    String salt = oldAuth.getSalt();
    setAuthenticationCredentials(new AuthenticationCredentials(token, salt));
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

  public OptionalInt getPhoneNumberIdentityRegistrationId() {
    return phoneNumberIdentityRegistrationId != null ? OptionalInt.of(phoneNumberIdentityRegistrationId) : OptionalInt.empty();
  }

  public void setPhoneNumberIdentityRegistrationId(final int phoneNumberIdentityRegistrationId) {
    this.phoneNumberIdentityRegistrationId = phoneNumberIdentityRegistrationId;
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

  public static class DeviceCapabilities {
    @JsonProperty
    private boolean storage;

    @JsonProperty
    private boolean transfer;

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

    @JsonProperty
    private boolean paymentActivation;

    public DeviceCapabilities() {
    }

    public DeviceCapabilities(boolean storage, boolean transfer,
        final boolean senderKey, final boolean announcementGroup, final boolean changeNumber,
        final boolean pni, final boolean stories, final boolean giftBadges, final boolean paymentActivation) {
      this.storage = storage;
      this.transfer = transfer;
      this.senderKey = senderKey;
      this.announcementGroup = announcementGroup;
      this.changeNumber = changeNumber;
      this.pni = pni;
      this.stories = stories;
      this.giftBadges = giftBadges;
      this.paymentActivation = paymentActivation;
    }

    public boolean isStorage() {
      return storage;
    }

    public boolean isTransfer() {
      return transfer;
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

    public boolean isPaymentActivation() {
      return paymentActivation;
    }
  }
}
