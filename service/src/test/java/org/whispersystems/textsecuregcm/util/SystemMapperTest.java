/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class SystemMapperTest {

  private static final ObjectMapper MAPPER = SystemMapper.configureMapper(new ObjectMapper());

  private static final String JSON_NO_FIELD = """
        {}
        """.trim();

  private static final String JSON_NULL_FIELD = """
        {"name":null}
        """.trim();

  private static final String JSON_WITH_FIELD = """
        {"name":"value"}
        """.trim();

  interface Data {
    Optional<String> name();
  }

  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  public record DataRecord(Optional<String> name) implements Data {
  }

  public static class DataClass implements Data {

    @JsonProperty
    private Optional<String> name = Optional.empty();

    public DataClass() {
    }

    public DataClass(final Optional<String> name) {
      this.name = name;
    }

    @Override
    public Optional<String> name() {
      return name;
    }
  }

  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  public static class DataClass2 extends DataClass {

    public DataClass2(final Optional<String> name) {
      super(name);
    }
  }

  @ParameterizedTest
  @ValueSource(classes = {DataClass.class, DataRecord.class})
  public void testOptionalField(final Class<? extends Data> clazz) throws Exception {
    assertTrue(MAPPER.readValue(JSON_NO_FIELD, clazz).name().isEmpty());
    assertTrue(MAPPER.readValue(JSON_NULL_FIELD, clazz).name().isEmpty());
    assertEquals("value", MAPPER.readValue(JSON_WITH_FIELD, clazz).name().orElseThrow());
  }

  @ParameterizedTest
  @MethodSource("provideStringsForIsBlank")
  public void testSerialization(final Data data, final String expectedJson) throws Exception {
    assertEquals(expectedJson, MAPPER.writeValueAsString(data));
  }

  private static Stream<Arguments> provideStringsForIsBlank() {
    return Stream.of(
        Arguments.of(new DataClass(Optional.of("value")), JSON_WITH_FIELD),
        Arguments.of(new DataClass(Optional.empty()), JSON_NULL_FIELD),
        Arguments.of(new DataClass(null), JSON_NULL_FIELD),
        Arguments.of(new DataClass2(Optional.of("value")), JSON_WITH_FIELD),
        Arguments.of(new DataClass2(Optional.of("value")), JSON_WITH_FIELD),
        Arguments.of(new DataClass2(Optional.empty()), JSON_NO_FIELD),
        Arguments.of(new DataRecord(Optional.of("value")), JSON_WITH_FIELD),
        Arguments.of(new DataRecord(Optional.empty()), JSON_NO_FIELD),
        Arguments.of(new DataRecord(null), JSON_NO_FIELD)
    );
  }

  public record NotAnnotatedWithJsonFilter(String data) {
  }

  @JsonFilter("AnnotatedWithJsonFilter")
  public record AnnotatedWithJsonFilter(String data, String excluded) {
  }

  @Test
  public void testFiltering() throws Exception {
    assertThrows(IllegalStateException.class, () -> SystemMapper.excludingField(NotAnnotatedWithJsonFilter.class, List.of("data")));
    final ObjectWriter writer = SystemMapper.jsonMapper()
        .writer(SystemMapper.excludingField(AnnotatedWithJsonFilter.class, List.of("excluded")));
    final AnnotatedWithJsonFilter obj = new AnnotatedWithJsonFilter("valData", "valExcluded");
    final String json = writer.writeValueAsString(obj);
    final Map<?, ?> serializedFields = SystemMapper.jsonMapper().readValue(json, Map.class);
    assertEquals(Map.of("data", "valData"), serializedFields);
  }
}
