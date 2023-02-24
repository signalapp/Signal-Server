/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import javax.annotation.Nonnull;

public class SystemMapper {

  private static final ObjectMapper JSON_MAPPER = configureMapper(new ObjectMapper());

  private static final ObjectMapper YAML_MAPPER = configureMapper(new YAMLMapper());


  @Nonnull
  public static ObjectMapper jsonMapper() {
    return JSON_MAPPER;
  }

  @Nonnull
  public static ObjectMapper yamlMapper() {
    return YAML_MAPPER;
  }

  public static ObjectMapper configureMapper(final ObjectMapper mapper) {
    return mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE)
        .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
        .setVisibility(PropertyAccessor.CREATOR, JsonAutoDetect.Visibility.PUBLIC_ONLY)
        .registerModules(
            new JavaTimeModule(),
            new Jdk8Module());
  }
}
