package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

public class RedPhoneConfiguration {

  @JsonProperty
  private String authKey;

  public Optional<byte[]> getAuthorizationKey() throws DecoderException {
    if (authKey == null || authKey.trim().length() == 0) {
      return Optional.absent();
    }

    return Optional.of(Hex.decodeHex(authKey.toCharArray()));
  }
}
