/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration.secrets;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.whispersystems.textsecuregcm.util.SystemMapper;

public class SecretStore {

  private final Map<String, Secret<?>> secrets;


  public static SecretStore fromYamlFileSecretsBundle(final String filename) {
    try {
      @SuppressWarnings("unchecked")
      final Map<String, Object> secretsBundle = SystemMapper.yamlMapper().readValue(new File(filename), Map.class);
      return fromSecretsBundle(secretsBundle);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to parse YAML file [%s]".formatted(filename), e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public SecretStore(final Map<String, Secret<?>> secrets) {
    this.secrets = Map.copyOf(secrets);
  }

  public SecretString secretString(final String reference) {
    return fromStore(reference, SecretString.class);
  }

  public SecretBytes secretBytesFromBase64String(final String reference) {
    final SecretString secret = fromStore(reference, SecretString.class);
    return new SecretBytes(decodeBase64(secret.value()));
  }

  public SecretStringList secretStringList(final String reference) {
    return fromStore(reference, SecretStringList.class);
  }

  public SecretBytesList secretBytesListFromBase64Strings(final String reference) {
    final List<String> secrets = secretStringList(reference).value();
    final List<byte[]> byteSecrets = secrets.stream().map(SecretStore::decodeBase64).toList();
    return new SecretBytesList(byteSecrets);
  }

  private <T extends Secret<?>> T fromStore(final String name, final Class<T> expected) {
    final Secret<?> secret = secrets.get(name);
    if (secret == null) {
      throw new IllegalArgumentException("Secret [%s] is not present in the secrets bundle".formatted(name));
    }
    if (!expected.isInstance(secret)) {
      throw new IllegalArgumentException("Secret [%s] is of type [%s] but caller expects type [%s]".formatted(
          name, secret.getClass().getSimpleName(), expected.getSimpleName()));
    }
    return expected.cast(secret);
  }

  @VisibleForTesting
  public static SecretStore fromYamlStringSecretsBundle(final String secretsBundleYaml) {
    try {
      @SuppressWarnings("unchecked")
      final Map<String, Object> secretsBundle = SystemMapper.yamlMapper().readValue(secretsBundleYaml, Map.class);
      return fromSecretsBundle(secretsBundle);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to parse JSON", e);
    }
  }

  private static SecretStore fromSecretsBundle(final Map<String, Object> secretsBundle) {
    final Map<String, Secret<?>> store = new HashMap<>();
    secretsBundle.forEach((k, v) -> {
      if (v instanceof final String str) {
        store.put(k, new SecretString(str));
        return;
      }
      if (v instanceof final List<?> list) {
        final List<String> secrets = list.stream().map(o -> {
          if (o instanceof final String s) {
            return s;
          }
          throw new IllegalArgumentException("Secrets bundle JSON object is only supposed to have values of types String and list of Strings");
        }).toList();
        store.put(k, new SecretStringList(secrets));
        return;
      }
      throw new IllegalArgumentException("Secrets bundle JSON object is only supposed to have values of types String and list of Strings");
    });
    return new SecretStore(store);
  }

  private static byte[] decodeBase64(final String str) {
    return Base64.getDecoder().decode(str);
  }
}
