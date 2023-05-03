package org.whispersystems.textsecuregcm.configuration;

import javax.validation.constraints.NotBlank;

public record RegistrationServiceConfiguration(@NotBlank String host,
                                               int port,
                                               @NotBlank String credentialConfigurationJson,
                                               @NotBlank String identityTokenAudience,
                                               @NotBlank String registrationCaCertificate) {
}
