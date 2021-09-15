/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.net.URL;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

public class BadgeConfiguration {
  private final String id;
  private final URL imageUrl;

  @JsonCreator
  public BadgeConfiguration(
      @JsonProperty("id") final String id,
      @JsonProperty("imageUrl") @JsonDeserialize(converter = URLDeserializationConverter.class) final URL imageUrl) {
    this.id = id;
    this.imageUrl = imageUrl;
  }

  @NotEmpty
  public String getId() {
    return id;
  }

  @NotNull
  @JsonSerialize(converter = URLSerializationConverter.class)
  public URL getImageUrl() {
    return imageUrl;
  }
}
