package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.whispersystems.textsecuregcm.crypto.Curve;
import org.whispersystems.textsecuregcm.crypto.ECPrivateKey;
import org.whispersystems.textsecuregcm.util.ByteArrayAdapter;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

public class UnidentifiedDeliveryConfiguration {

  @JsonProperty
  @JsonSerialize(using = ByteArrayAdapter.Serializing.class)
  @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
  @NotNull
  private byte[] certificate;

  @JsonProperty
  @JsonSerialize(using = ByteArrayAdapter.Serializing.class)
  @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
  @NotNull
  @Size(min = 32, max = 32)
  private byte[] privateKey;

  @NotNull
  private int expiresDays;

  public byte[] getCertificate() {
    return certificate;
  }

  public ECPrivateKey getPrivateKey() {
    return Curve.decodePrivatePoint(privateKey);
  }

  public int getExpiresDays() {
    return expiresDays;
  }
}
