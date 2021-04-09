/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;

public class AttachmentUri {

  @JsonProperty
  private String location;

  public AttachmentUri(URL uri) {
    this.location = uri.toString();
  }

  public AttachmentUri() {}

  public URL getLocation() throws MalformedURLException {
    return URI.create(location).toURL();
  }
}
