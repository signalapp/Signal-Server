/**
 * Copyright (C) 2013 Open WhisperSystems
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.whispersystems.textsecuregcm.storage;


import org.whispersystems.textsecuregcm.auth.AuthenticationCredentials;

import java.io.Serializable;

public class Account implements Serializable {

  public static final int MEMCACHE_VERION = 1;

  private long    id;
  private String  number;
  private long    deviceId;
  private String  hashedAuthenticationToken;
  private String  salt;
  private String  signalingKey;
  /**
   * In order for us to tell a client that an account is "inactive" (ie go use SMS for transport), we check that all
   * non-fetching Accounts don't have push registrations. In this way, we can ensure that we have some form of transport
   * available for all Accounts on all "active" numbers.
   */
  private String  gcmRegistrationId;
  private String  apnRegistrationId;
  private boolean supportsSms;
  private boolean fetchesMessages;

  public Account() {}

  public Account(long id, String number, long deviceId, String hashedAuthenticationToken, String salt,
                 String signalingKey, String gcmRegistrationId, String apnRegistrationId,
                 boolean supportsSms, boolean fetchesMessages)
  {
    this.id                        = id;
    this.number                    = number;
    this.deviceId                  = deviceId;
    this.hashedAuthenticationToken = hashedAuthenticationToken;
    this.salt                      = salt;
    this.signalingKey              = signalingKey;
    this.gcmRegistrationId         = gcmRegistrationId;
    this.apnRegistrationId         = apnRegistrationId;
    this.supportsSms               = supportsSms;
    this.fetchesMessages           = fetchesMessages;
  }

  public String getApnRegistrationId() {
    return apnRegistrationId;
  }

  public void setApnRegistrationId(String apnRegistrationId) {
    this.apnRegistrationId = apnRegistrationId;
  }

  public String getGcmRegistrationId() {
    return gcmRegistrationId;
  }

  public void setGcmRegistrationId(String gcmRegistrationId) {
    this.gcmRegistrationId = gcmRegistrationId;
  }

  public void setNumber(String number) {
    this.number = number;
  }

  public String getNumber() {
    return number;
  }

  public long getDeviceId() {
    return deviceId;
  }

  public void setDeviceId(long deviceId) {
    this.deviceId = deviceId;
  }

  public void setAuthenticationCredentials(AuthenticationCredentials credentials) {
    this.hashedAuthenticationToken = credentials.getHashedAuthenticationToken();
    this.salt                      = credentials.getSalt();
  }

  public AuthenticationCredentials getAuthenticationCredentials() {
    return new AuthenticationCredentials(hashedAuthenticationToken, salt);
  }

  public String getSignalingKey() {
    return signalingKey;
  }

  public void setSignalingKey(String signalingKey) {
    this.signalingKey = signalingKey;
  }

  public boolean getSupportsSms() {
    return supportsSms;
  }

  public void setSupportsSms(boolean supportsSms) {
    this.supportsSms = supportsSms;
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public void setFetchesMessages(boolean fetchesMessages) {
    this.fetchesMessages = fetchesMessages;
  }

  public boolean getFetchesMessages() {
    return fetchesMessages;
  }
}
