/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DeliveryCertificate {

  private final byte[] certificate;

  @JsonCreator
  public DeliveryCertificate(
      @JsonProperty("certificate") byte[] certificate) {
    this.certificate = certificate;
  }

  public byte[] getCertificate() {
    return certificate;
  }
}
