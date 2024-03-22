package org.whispersystems.textsecuregcm.configuration;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretString;

public record NoiseWebSocketTunnelConfiguration(@Positive int port, @NotNull SecretString recognizedProxySecret) {
}
