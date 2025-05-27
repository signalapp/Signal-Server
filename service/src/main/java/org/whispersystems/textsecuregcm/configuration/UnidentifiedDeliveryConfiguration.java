/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.signal.libsignal.protocol.InvalidKeyException;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECPrivateKey;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretBytes;
import org.whispersystems.textsecuregcm.util.ExactlySize;

public record UnidentifiedDeliveryConfiguration(@NotNull @NotEmpty  byte[] certificate,
                                                @ExactlySize(32) SecretBytes privateKey,
                                                int expiresDays) {
  public ECPrivateKey ecPrivateKey() throws InvalidKeyException {
    return Curve.decodePrivatePoint(privateKey.value());
  }
}
