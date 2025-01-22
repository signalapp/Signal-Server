package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dropwizard.core.setup.Environment;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretBytes;
import org.whispersystems.textsecuregcm.registration.IdentityTokenCallCredentials;
import org.whispersystems.textsecuregcm.registration.RegistrationServiceClient;

@JsonTypeName("default")
public record RegistrationServiceConfiguration(@NotBlank String host,
                                               int port,
                                               @NotBlank String credentialConfigurationJson,
                                               @NotBlank String identityTokenAudience,
                                               @NotBlank String registrationCaCertificate,
                                               @NotNull SecretBytes collationKeySalt) implements
    RegistrationServiceClientFactory {

  @Override
  public RegistrationServiceClient build(final Environment environment, final Executor callbackExecutor,
      final ScheduledExecutorService identityRefreshExecutor) {
    try {
      final IdentityTokenCallCredentials callCredentials = IdentityTokenCallCredentials.fromCredentialConfig(
          credentialConfigurationJson, identityTokenAudience, identityRefreshExecutor);

      environment.lifecycle().manage(callCredentials);

      return new RegistrationServiceClient(host, port, callCredentials, registrationCaCertificate, collationKeySalt.value(),
          identityRefreshExecutor);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
