/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration.secrets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonMappingException;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.constraints.NotEmpty;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.util.ExactlySize;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

public class SecretsTest {

  private static final String SECRET_REF = "secret_string";

  private static final String SECRET_LIST_REF = "secret_string_list";

  private static final String SECRET_BYTES_REF = "secret_bytes";

  private static final String SECRET_BYTES_LIST_REF = "secret_bytes_list";

  public record TestData(SecretString secret,
                         SecretBytes secretBytes,
                         SecretStringList secretList,
                         SecretBytesList secretBytesList) {
  }

  private static final String VALID_CONFIG_YAML = """
      secret: secret://%s
      secretBytes: secret://%s
      secretList: secret://%s
      secretBytesList: secret://%s
      """.formatted(SECRET_REF, SECRET_BYTES_REF, SECRET_LIST_REF, SECRET_BYTES_LIST_REF);


  @Test
  public void testDeserialization() throws Exception {
    final String secretString = "secret_string";
    final byte[] secretBytes = TestRandomUtil.nextBytes(16);
    final String secretBytesBase64 = Base64.getEncoder().encodeToString(secretBytes);
    final List<String> secretStringList = List.of("secret1", "secret2", "secret3");
    final List<byte[]> secretBytesList = List.of(TestRandomUtil.nextBytes(16), TestRandomUtil.nextBytes(16), TestRandomUtil.nextBytes(16));
    final List<String> secretBytesListBase64 = secretBytesList.stream().map(Base64.getEncoder()::encodeToString).toList();
    final Map<String, Secret<?>> storeMap = Map.of(
        SECRET_REF, new SecretString(secretString),
        SECRET_BYTES_REF, new SecretString(secretBytesBase64),
        SECRET_LIST_REF, new SecretStringList(secretStringList),
        SECRET_BYTES_LIST_REF, new SecretStringList(secretBytesListBase64)
    );
    SecretsModule.INSTANCE.setSecretStore(new SecretStore(storeMap));

    final TestData result = SystemMapper.yamlMapper().readValue(VALID_CONFIG_YAML, TestData.class);
    assertEquals(secretString, result.secret().value());
    assertEquals(secretStringList, result.secretList().value());
    assertArrayEquals(secretBytes, result.secretBytes().value());
    for (int i = 0; i < secretBytesList.size(); i++) {
      assertArrayEquals(secretBytesList.get(i), result.secretBytesList().value().get(i));
    }
  }

  @Test
  public void testValueWithoutPrefix() throws Exception {
    final String config = """
      secret: ref
      """;
    SecretsModule.INSTANCE.setSecretStore(new SecretStore(Collections.emptyMap()));
    assertThrows(JsonMappingException.class, () -> SystemMapper.yamlMapper().readValue(config, TestData.class));
  }

  @Test
  public void testNoSecretInTheStore() throws Exception {
    final String config = """
      secret: secret://missing
      secretBytes: secret://missing
      secretList: secret://missing
      secretBytesList: secret://missing
      """;
    SecretsModule.INSTANCE.setSecretStore(new SecretStore(Collections.emptyMap()));
    assertThrows(JsonMappingException.class, () -> SystemMapper.yamlMapper().readValue(config, TestData.class));
  }

  @Test
  public void testSecretStoreNotSet() throws Exception {
    assertThrows(JsonMappingException.class, () -> SystemMapper.yamlMapper().readValue(VALID_CONFIG_YAML, TestData.class));
  }

  @Test
  public void testReadFromJson() throws Exception {
    // checking that valid json secrets bundle is read correctly
    final SecretStore secretStore = SecretStore.fromYamlStringSecretsBundle("""
        secret_string: value
        secret_string_list:
          - value1
          - value2
          - value3
        """);
    assertEquals("value", secretStore.secretString("secret_string").value());
    assertEquals(List.of("value1", "value2", "value3"), secretStore.secretStringList("secret_string_list").value());

    // checking that secrets bundle can't have objects as values
    assertThrows(IllegalArgumentException.class, () -> SecretStore.fromYamlStringSecretsBundle("""
        secret_string: value
        not_a_string_or_list:
          k: v
        """));

    // checking that secrets bundle can't have numbers as values
    assertThrows(IllegalArgumentException.class, () -> SecretStore.fromYamlStringSecretsBundle("""
        secret_string: value
        not_a_string_or_list: 42
        """));
  }

  record NotEmptySecretStringList(@NotEmpty SecretStringList secret) {
  }

  record NotEmptySecretBytesList(@NotEmpty SecretBytesList secret) {
  }

  record ExactlySizeBytesSecret(@ExactlySize(32) SecretBytes secret) {
  }

  @Test
  public void testValidators() throws Exception {
    final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

    // @NotEmpty SecretStringList
    assertFalse(validator.validate(new NotEmptySecretStringList(new SecretStringList(List.of()))).isEmpty());
    assertTrue(validator.validate(new NotEmptySecretStringList(new SecretStringList(List.of("smth")))).isEmpty());

    // @NotEmpty SecretBytesList
    assertFalse(validator.validate(new NotEmptySecretBytesList(new SecretBytesList(List.of()))).isEmpty());
    assertTrue(validator.validate(new NotEmptySecretBytesList(new SecretBytesList(List.of(new byte[4])))).isEmpty());

    // @ExactlySize SecretBytes
    assertFalse(validator.validate(new ExactlySizeBytesSecret(new SecretBytes(new byte[16]))).isEmpty());
    assertTrue(validator.validate(new ExactlySizeBytesSecret(new SecretBytes(new byte[32]))).isEmpty());
  }
}
