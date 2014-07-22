package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;

import java.io.Serializable;

public class SignedPreKey extends PreKeyV2 {

  @JsonProperty
  @NotEmpty
  private String signature;

  public SignedPreKey() {}

  public SignedPreKey(long keyId, String publicKey, String signature) {
    super(keyId, publicKey);
    this.signature = signature;
  }

  public String getSignature() {
    return signature;
  }

  @Override
  public boolean equals(Object object) {
    if (object == null || !(object instanceof SignedPreKey)) return false;
    SignedPreKey that = (SignedPreKey) object;

    if (signature == null) {
      return super.equals(object) && that.signature == null;
    } else {
      return super.equals(object) && this.signature.equals(that.signature);
    }
  }

  @Override
  public int hashCode() {
    if (signature == null) {
      return super.hashCode();
    } else {
      return super.hashCode() ^ signature.hashCode();
    }
  }

}
