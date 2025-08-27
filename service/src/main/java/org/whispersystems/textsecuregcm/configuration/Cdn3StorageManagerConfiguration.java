package org.whispersystems.textsecuregcm.configuration;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.util.Collections;
import java.util.Map;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretString;
import javax.annotation.Nullable;

/**
 * Configuration for the cdn3 storage manager
 *
 * @param baseUri        The base URI of the storage manager
 * @param clientId       The cloudflare client ID to use to authenticate to the storage manager
 * @param clientSecret   The cloudflare client secret to use to authenticate to the storage manager
 * @param sourceSchemes  A map of cdn id to a retrieval scheme understood by the storage-manager. This is used by the
 *                       storage-manager when copying to determine how to read a source object. Current schemes are
 *                       'gcs' and 'r2'
 * @param numHttpClients The number http clients to use with the storage-manager to support request striping
 * @param circuitBreakerConfigurationName The name of a circuit breaker configuration for the storage-manager http
 *                                        client; if `null`, uses the global default configuration
 * @param retryConfigurationName          The name of a retry configuration for the storage-manager http client; if
 *                                        `null`, uses the global default configuration
 */
public record Cdn3StorageManagerConfiguration(
    @NotNull String baseUri,
    @NotNull String clientId,
    @NotNull SecretString clientSecret,
    @NotNull Map<Integer, String> sourceSchemes,
    @NotNull Integer numHttpClients,
    @Nullable String circuitBreakerConfigurationName,
    @Nullable String retryConfigurationName) {

  public Cdn3StorageManagerConfiguration {
    if (numHttpClients == null) {
      numHttpClients = 2;
    }
    if (sourceSchemes == null) {
      sourceSchemes = Collections.emptyMap();
    }
  }
}
