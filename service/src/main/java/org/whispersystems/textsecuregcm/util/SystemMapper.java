/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.vdurmont.semver4j.Semver;
import java.io.IOException;
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
        .registerModules(
            applicationModule(),
            new JavaTimeModule(),
            new Jdk8Module());
  }

  private static Module applicationModule() {
    return new SimpleModule()
        .addDeserializer(Semver.class, new JsonDeserializer<>() {
          @Override
          public Semver deserialize(final JsonParser p, final DeserializationContext ctxt) throws IOException {
            final String strValue = p.readValueAs(String.class);
            return strValue != null ? new Semver(strValue) : null;
          }
        });
  }
}
