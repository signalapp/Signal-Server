package org.whispersystems.textsecuregcm.registration;

import io.grpc.CallCredentials;
import io.grpc.Metadata;
import java.util.concurrent.Executor;

class ApiKeyCallCredentials extends CallCredentials {

  private final String apiKey;

  private static final Metadata.Key<String> API_KEY_METADATA_KEY =
      Metadata.Key.of("x-signal-api-key", Metadata.ASCII_STRING_MARSHALLER);

  ApiKeyCallCredentials(final String apiKey) {
    this.apiKey = apiKey;
  }

  @Override
  public void applyRequestMetadata(final RequestInfo requestInfo,
      final Executor appExecutor,
      final MetadataApplier applier) {

    final Metadata metadata = new Metadata();
    metadata.put(API_KEY_METADATA_KEY, apiKey);

    applier.apply(metadata);
  }

  @Override
  public void thisUsesUnstableApi() {
  }
}
