package org.whispersystems.textsecuregcm.configuration.dynamic;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.vdurmont.semver4j.Semver;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.whispersystems.textsecuregcm.util.ua.ClientPlatform;
import javax.validation.constraints.NotNull;

public class DynamicRateLimitChallengeConfiguration {

  @JsonProperty
  @NotNull
  private Map<ClientPlatform, Semver> clientSupportedVersions = Collections.emptyMap();

  @VisibleForTesting
  Map<ClientPlatform, Semver> getClientSupportedVersions() {
    return clientSupportedVersions;
  }

  public Optional<Semver> getMinimumSupportedVersion(final ClientPlatform platform) {
    return Optional.ofNullable(clientSupportedVersions.get(platform));
  }
}
