/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;
import java.util.List;

public class ClientContactTokens {

  @NotNull
  @JsonProperty
  private List<String> contacts;

  public List<String> getContacts() {
    return contacts;
  }

  public ClientContactTokens() {}

  public ClientContactTokens(List<String> contacts) {
    this.contacts = contacts;
  }

}
