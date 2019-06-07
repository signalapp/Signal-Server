package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.hibernate.validator.constraints.NotEmpty;

public class SecureStorageServiceConfiguration {

  @NotEmpty
  @JsonProperty
  private String userAuthenticationTokenSharedSecret;

  public byte[] getUserAuthenticationTokenSharedSecret() throws DecoderException {
    return Hex.decodeHex(userAuthenticationTokenSharedSecret.toCharArray());
  }
  
}
