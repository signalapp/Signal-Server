package org.whispersystems.textsecuregcm.registration;

import com.google.auth.oauth2.ExternalAccountCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.common.base.Suppliers;
import io.grpc.CallCredentials;
import io.grpc.Metadata;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IdentityTokenCallCredentials extends CallCredentials {

  private final Supplier<String> identityTokenSupplier;

  private static final Duration IDENTITY_TOKEN_LIFETIME = Duration.ofHours(1);
  private static final Duration IDENTITY_TOKEN_REFRESH_BUFFER = Duration.ofMinutes(10);

  private static final Metadata.Key<String> AUTHORIZATION_METADATA_KEY =
      Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER);

  private static final Logger logger = LoggerFactory.getLogger(IdentityTokenCallCredentials.class);

  IdentityTokenCallCredentials(final Supplier<String> identityTokenSupplier) {
    this.identityTokenSupplier = identityTokenSupplier;
  }

  static IdentityTokenCallCredentials fromCredentialConfig(final String credentialConfigJson, final String audience) throws IOException {
    try (final InputStream configInputStream = new ByteArrayInputStream(credentialConfigJson.getBytes(StandardCharsets.UTF_8))) {
      final ExternalAccountCredentials credentials = ExternalAccountCredentials.fromStream(configInputStream);
      final ImpersonatedCredentials impersonatedCredentials = ImpersonatedCredentials.create(credentials,
          credentials.getServiceAccountEmail(), null, List.of(), (int) IDENTITY_TOKEN_LIFETIME.toSeconds());

      final Supplier<String> idTokenSupplier = Suppliers.memoizeWithExpiration(() -> {
            try {
              impersonatedCredentials.getSourceCredentials().refresh();
              return impersonatedCredentials.idTokenWithAudience(audience, null).getTokenValue();
            } catch (final IOException e) {
              logger.warn("Failed to retrieve identity token", e);
              throw new UncheckedIOException(e);
            }
          },
          IDENTITY_TOKEN_LIFETIME.minus(IDENTITY_TOKEN_REFRESH_BUFFER).toMillis(),
          TimeUnit.MILLISECONDS);

      return new IdentityTokenCallCredentials(idTokenSupplier);
    }
  }

  @Override
  public void applyRequestMetadata(final RequestInfo requestInfo,
      final Executor appExecutor,
      final MetadataApplier applier) {

    @Nullable final String identityTokenValue = identityTokenSupplier.get();

    if (identityTokenValue != null) {
      final Metadata metadata = new Metadata();
      metadata.put(AUTHORIZATION_METADATA_KEY, "Bearer " + identityTokenValue);

      applier.apply(metadata);
    }
  }

  @Override
  public void thisUsesUnstableApi() {
  }
}
