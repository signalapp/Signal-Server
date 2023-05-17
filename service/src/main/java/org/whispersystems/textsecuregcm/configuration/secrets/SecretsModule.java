/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration.secrets;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

public class SecretsModule extends SimpleModule {

  public static final SecretsModule INSTANCE = new SecretsModule();

  public static final String PREFIX = "secret://";

  private final AtomicReference<SecretStore> secretStoreHolder = new AtomicReference<>(null);


  private SecretsModule() {
    addDeserializer(SecretString.class, createDeserializer(SecretStore::secretString));
    addDeserializer(SecretBytes.class, createDeserializer(SecretStore::secretBytesFromBase64String));
    addDeserializer(SecretStringList.class, createDeserializer(SecretStore::secretStringList));
    addDeserializer(SecretBytesList.class, createDeserializer(SecretStore::secretBytesListFromBase64Strings));
  }

  public void setSecretStore(final SecretStore secretStore) {
    this.secretStoreHolder.set(requireNonNull(secretStore));
  }

  private <T> JsonDeserializer<T> createDeserializer(final BiFunction<SecretStore, String, T> constructor) {
    return new JsonDeserializer<>() {
      @Override
      public T deserialize(final JsonParser p, final DeserializationContext ctxt) throws IOException, JacksonException {
        final SecretStore secretStore = secretStoreHolder.get();
        if (secretStore == null) {
          throw new IllegalStateException(
              "An instance of a SecretStore must be set for the SecretsModule via setSecretStore() method");
        }
        final String reference = p.getValueAsString();
        if (!reference.startsWith(PREFIX) || reference.length() <= PREFIX.length()) {
          throw new IllegalArgumentException(
              "Value of a secret field must start with a [%s] prefix and refer to an entry in a secrets bundle".formatted(PREFIX));
        }
        return constructor.apply(secretStore, reference.substring(PREFIX.length()));
      }
    };
  }
}
