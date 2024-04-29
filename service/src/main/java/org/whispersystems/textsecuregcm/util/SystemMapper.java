/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.FilterProvider;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import io.dropwizard.jackson.DiscoverableSubtypeResolver;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretsModule;

public class SystemMapper {

  private static final ObjectMapper JSON_MAPPER = configureMapper(new ObjectMapper());

  private static final ObjectMapper YAML_MAPPER = configureMapper(new YAMLMapper())
      .setSubtypeResolver(new DiscoverableSubtypeResolver());


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
        .setFilterProvider(new SimpleFilterProvider().setDefaultFilter(SimpleBeanPropertyFilter.serializeAll()))
        .setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE)
        .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
        .setVisibility(PropertyAccessor.CREATOR, JsonAutoDetect.Visibility.PUBLIC_ONLY)
        .registerModules(
            SecretsModule.INSTANCE,
            new JavaTimeModule(),
            new Jdk8Module());
  }

  public static FilterProvider excludingField(final Class<?> clazz, final List<String> fieldsToExclude) {
    final String filterId = clazz.getSimpleName();

    // validate that the target class is annotated with @JsonFilter,
    final List<JsonFilter> jsonFilterAnnotations = Arrays.stream(clazz.getAnnotations())
        .map(a -> a instanceof JsonFilter jsonFilter ? jsonFilter : null)
        .filter(Objects::nonNull)
        .toList();
    if (jsonFilterAnnotations.size() != 1 || !jsonFilterAnnotations.get(0).value().equals(filterId)) {
      throw new IllegalStateException("""
          Class `%1$s` must have a single annotation of type `JsonFilter` 
          with the value equal to the name of the class itself: `@JsonFilter("%1$s")`
          """.formatted(filterId));
    }

    return new SimpleFilterProvider()
        .addFilter(filterId, SimpleBeanPropertyFilter.serializeAllExcept(fieldsToExclude.toArray(new String[0])));
  }
}
