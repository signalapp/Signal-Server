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
  private final String name;
  private final URL imageUrl;

  @JsonCreator
  public BadgeConfiguration(
      @JsonProperty("name") final String name,
      @JsonProperty("imageUrl") @JsonDeserialize(converter = URLDeserializationConverter.class) final URL imageUrl) {
    this.name = name;
    this.imageUrl = imageUrl;
  }

  @NotEmpty
  public String getName() {
    return name;
  }

  @NotNull
  @JsonSerialize(converter = URLSerializationConverter.class)
  public URL getImageUrl() {
    return imageUrl;
  }
}
