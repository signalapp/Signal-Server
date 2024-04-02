package org.whispersystems.textsecuregcm.configuration;

import org.whispersystems.textsecuregcm.configuration.secrets.SecretString;
import javax.validation.constraints.NotNull;

public record Cdn3StorageManagerConfiguration(
    @NotNull String baseUri,
    @NotNull String clientId,
    @NotNull SecretString clientSecret,
    @NotNull Integer numHttpClients) {

  public Cdn3StorageManagerConfiguration {
    if (numHttpClients == null) {
      numHttpClients = 2;
    }
  }
}
