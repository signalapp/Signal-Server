package org.whispersystems.textsecuregcm.tests.util;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

import static io.dropwizard.testing.FixtureHelpers.fixture;

public class JsonHelpers {

  private static final ObjectMapper objectMapper = new ObjectMapper();

  public static String asJson(Object object) throws JsonProcessingException {
    return objectMapper.writeValueAsString(object);
  }

  public static <T> T fromJson(String value, Class<T> clazz) throws IOException {
    return objectMapper.readValue(value, clazz);
  }

  public static String jsonFixture(String filename) throws IOException {
    return objectMapper.writeValueAsString(objectMapper.readValue(fixture(filename), JsonNode.class));
  }
}
