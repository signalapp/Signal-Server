package org.whispersystems.textsecuregcm.backup;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.cert.CertificateException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Stream;
import javax.ws.rs.core.Response;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.configuration.RetryConfiguration;
import org.whispersystems.textsecuregcm.http.FaultTolerantHttpClient;

public class Cdn3RemoteStorageManager implements RemoteStorageManager {

  private final FaultTolerantHttpClient httpClient;

  public Cdn3RemoteStorageManager(
      final ScheduledExecutorService retryExecutor,
      final CircuitBreakerConfiguration circuitBreakerConfiguration,
      final RetryConfiguration retryConfiguration,
      final List<String> caCertificates) throws CertificateException {
    this.httpClient = FaultTolerantHttpClient.newBuilder()
        .withName("cdn3-remote-storage")
        .withCircuitBreaker(circuitBreakerConfiguration)
        .withExecutor(Executors.newCachedThreadPool())
        .withRetryExecutor(retryExecutor)
        .withRetry(retryConfiguration)
        .withConnectTimeout(Duration.ofSeconds(10))
        .withVersion(HttpClient.Version.HTTP_2)
        .withTrustedServerCertificates(caCertificates.toArray(new String[0]))
        .build();
  }

  @Override
  public int cdnNumber() {
    return 3;
  }

  @Override
  public CompletionStage<Void> copy(
      final URI sourceUri,
      final int expectedSourceLength,
      final MediaEncryptionParameters encryptionParameters,
      final MessageBackupUploadDescriptor uploadDescriptor) {

    if (uploadDescriptor.cdn() != cdnNumber()) {
      throw new IllegalArgumentException("Cdn3RemoteStorageManager can only copy to cdn3");
    }

    final BackupMediaEncrypter encrypter = new BackupMediaEncrypter(encryptionParameters);

    final HttpRequest request = HttpRequest.newBuilder().GET().uri(sourceUri).build();
    return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofPublisher()).thenCompose(response -> {
          if (response.statusCode() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new CompletionException(new SourceObjectNotFoundException());
          } else if (response.statusCode() != Response.Status.OK.getStatusCode()) {
            throw new CompletionException(new IOException("error reading from source: " + response.statusCode()));
          }

          final int actualSourceLength = Math.toIntExact(response.headers().firstValueAsLong("Content-Length")
              .orElseThrow(() -> new CompletionException(new IOException("upstream missing Content-Length"))));

          if (actualSourceLength != expectedSourceLength) {
            throw new CompletionException(
                new InvalidLengthException("Provided sourceLength " + expectedSourceLength + " was " + actualSourceLength));
          }

          final int expectedEncryptedLength = encrypter.outputSize(actualSourceLength);
          final HttpRequest.BodyPublisher encryptedBody = HttpRequest.BodyPublishers.fromPublisher(
              encrypter.encryptBody(response.body()), expectedEncryptedLength);

          final String[] headers = Stream.concat(
                  uploadDescriptor.headers().entrySet()
                      .stream()
                      .flatMap(e -> Stream.of(e.getKey(), e.getValue())),
                  Stream.of("Upload-Length", Integer.toString(expectedEncryptedLength), "Tus-Resumable", "1.0.0"))
              .toArray(String[]::new);

          final HttpRequest put = HttpRequest.newBuilder()
              .uri(URI.create(uploadDescriptor.signedUploadLocation()))
              .headers(headers)
              .POST(encryptedBody)
              .build();

          return httpClient.sendAsync(put, HttpResponse.BodyHandlers.discarding());
        })
        .thenAccept(response -> {
          if (response.statusCode() != Response.Status.CREATED.getStatusCode() &&
              response.statusCode() != Response.Status.OK.getStatusCode()) {
            throw new CompletionException(new IOException("Failed to copy object: " + response.statusCode()));
          }
        });
  }
}
