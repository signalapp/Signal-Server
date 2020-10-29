/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.whispersystems.textsecuregcm.util.ByteArrayAdapter;

import java.util.Arrays;

@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class ClientContact {

  @JsonSerialize(using = ByteArrayAdapter.Serializing.class)
  @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
  @JsonProperty
  private byte[]  token;

  @JsonProperty
  private boolean voice;

  @JsonProperty
  private boolean video;

  private String  relay;
  private boolean inactive;

  public ClientContact(byte[] token, String relay, boolean voice, boolean video) {
    this.token = token;
    this.relay = relay;
    this.voice = voice;
    this.video = video;
  }

  public ClientContact() {}

  public byte[] getToken() {
    return token;
  }

  public String getRelay() {
    return relay;
  }

  public void setRelay(String relay) {
    this.relay = relay;
  }

  public boolean isInactive() {
    return inactive;
  }

  public void setInactive(boolean inactive) {
    this.inactive = inactive;
  }

  public boolean isVoice() {
    return voice;
  }

  public void setVoice(boolean voice) {
    this.voice = voice;
  }

  public boolean isVideo() {
    return video;
  }

  public void setVideo(boolean video) {
    this.video = video;
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) return false;
    if (!(other instanceof ClientContact)) return false;

    ClientContact that = (ClientContact)other;

    return
        Arrays.equals(this.token, that.token) &&
        this.inactive == that.inactive &&
        this.voice == that.voice &&
        this.video == that.video &&
        (this.relay == null ? (that.relay == null) : this.relay.equals(that.relay));
  }

  public int hashCode() {
    return Arrays.hashCode(this.token);
  }

}
