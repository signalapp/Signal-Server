package org.whispersystems.textsecuregcm.backup;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.Cdn3StorageManagerConfiguration;
import org.whispersystems.textsecuregcm.http.FaultTolerantHttpClient;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import org.whispersystems.textsecuregcm.util.HttpUtils;
import org.whispersystems.textsecuregcm.util.SystemMapper;

public class Cdn3RemoteStorageManager implements RemoteStorageManager {

  private static final Logger logger = LoggerFactory.getLogger(Cdn3RemoteStorageManager.class);

  private final FaultTolerantHttpClient storageManagerHttpClient;
  private final String storageManagerBaseUrl;
  private final String clientId;
  private final String clientSecret;
  private final Map<Integer, String> sourceSchemes;

  static final String CLIENT_ID_HEADER = "CF-Access-Client-Id";
  static final String CLIENT_SECRET_HEADER = "CF-Access-Client-Secret";

  private static final String STORAGE_MANAGER_STATUS_COUNTER_NAME = MetricsUtil.name(Cdn3RemoteStorageManager.class,
      "storageManagerStatus");

  private static final String STORAGE_MANAGER_TIMER_NAME = MetricsUtil.name(Cdn3RemoteStorageManager.class,
      "storageManager");
  private static final String OPERATION_TAG_NAME = "op";
  private static final String STATUS_TAG_NAME = "status";

  private static final String OBJECT_REMOVED_ON_DELETE_COUNTER_NAME = MetricsUtil.name(Cdn3RemoteStorageManager.class, "objectRemovedOnDelete");

  public Cdn3RemoteStorageManager(
      final ExecutorService httpExecutor,
      final ScheduledExecutorService retryExecutor,
      final Cdn3StorageManagerConfiguration configuration) {

    // strip trailing "/" for easier URI construction
    this.storageManagerBaseUrl = StringUtils.removeEnd(configuration.baseUri(), "/");
    this.clientId = configuration.clientId();
    this.clientSecret = configuration.clientSecret().value();

    // Client used for calls to storage-manager
    this.storageManagerHttpClient = FaultTolerantHttpClient.newBuilder()
        .withName("cdn3-storage-manager")
        .withCircuitBreaker(configuration.circuitBreaker())
        .withExecutor(httpExecutor)
        .withRetryExecutor(retryExecutor)
        .withRetry(configuration.retry())
        .withConnectTimeout(Duration.ofSeconds(10))
        .withVersion(HttpClient.Version.HTTP_2)
        .withNumClients(configuration.numHttpClients())
        .build();
    this.sourceSchemes = configuration.sourceSchemes();
  }

  @Override
  public int cdnNumber() {
    return 3;
  }

  @Override
  public CompletionStage<Void> copy(
      final int sourceCdn,
      final String sourceKey,
      final int expectedSourceLength,
      final MediaEncryptionParameters encryptionParameters,
      final String destinationKey) {
    final String sourceScheme = this.sourceSchemes.get(sourceCdn);
    if (sourceScheme == null) {
      return CompletableFuture.failedFuture(
          new SourceObjectNotFoundException("Cdn3RemoteStorageManager cannot copy from " + sourceCdn));
    }
    final String requestBody = new Cdn3CopyRequest(
        encryptionParameters,
        new Cdn3CopyRequest.SourceDescriptor(sourceScheme, sourceKey),
        expectedSourceLength,
        destinationKey).json();

    final Timer.Sample sample = Timer.start();
    final HttpRequest request = HttpRequest.newBuilder()
        .PUT(HttpRequest.BodyPublishers.ofString(requestBody))
        .uri(URI.create(copyUrl()))
        .header("Content-Type", "application/json")
        .header(CLIENT_ID_HEADER, clientId)
        .header(CLIENT_SECRET_HEADER, clientSecret)
        .build();
    return this.storageManagerHttpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
        .thenAccept(response -> {
          Metrics.counter(STORAGE_MANAGER_STATUS_COUNTER_NAME,
                  OPERATION_TAG_NAME, "copy",
                  STATUS_TAG_NAME, Integer.toString(response.statusCode()))
              .increment();
          if (response.statusCode() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw ExceptionUtils.wrap(new SourceObjectNotFoundException());
          } else if (response.statusCode() == Response.Status.CONFLICT.getStatusCode()) {
            throw ExceptionUtils.wrap(new InvalidLengthException(response.body()));
          } else if (!HttpUtils.isSuccessfulResponse(response.statusCode())) {
            logger.info("Failed to copy via storage-manager {} {}", response.statusCode(), response.body());
            throw ExceptionUtils.wrap(new IOException("Failed to copy object: " + response.statusCode()));
          }
        })
        .whenComplete((ignored, ignoredException) ->
            sample.stop(Metrics.timer(STORAGE_MANAGER_TIMER_NAME, OPERATION_TAG_NAME, "copy")));
  }

  /**
   * Serialized copy request for cdn3 storage manager
   */
  record Cdn3CopyRequest(
      String encryptionKey, String hmacKey,
      SourceDescriptor source, int expectedSourceLength,
      String dst) {

    Cdn3CopyRequest(MediaEncryptionParameters parameters, SourceDescriptor source, int expectedSourceLength,
        String dst) {
      this(Base64.getEncoder().encodeToString(parameters.aesEncryptionKey().getEncoded()),
          Base64.getEncoder().encodeToString(parameters.hmacSHA256Key().getEncoded()),
          source, expectedSourceLength, dst);
    }

    record SourceDescriptor(String scheme, String key) {}

    String json() {
      try {
        return SystemMapper.jsonMapper().writeValueAsString(this);
      } catch (JsonProcessingException e) {
        throw new IllegalStateException("Could not serialize copy request", e);
      }
    }
  }

  @Override
  public CompletionStage<ListResult> list(
      final String prefix,
      final Optional<String> cursor,
      final long limit) {
    final Timer.Sample sample = Timer.start();

    final Map<String, String> queryParams = new HashMap<>();
    queryParams.put("prefix", prefix);
    queryParams.put("limit", Long.toString(limit));
    cursor.ifPresent(s -> queryParams.put("cursor", cursor.get()));

    final HttpRequest request = HttpRequest.newBuilder().GET()
        .uri(URI.create("%s%s".formatted(listUrl(), HttpUtils.queryParamString(queryParams.entrySet()))))
        .header(CLIENT_ID_HEADER, clientId)
        .header(CLIENT_SECRET_HEADER, clientSecret)
        .build();

    return this.storageManagerHttpClient.sendAsync(request, HttpResponse.BodyHandlers.ofInputStream())
        .thenApply(response -> {
          Metrics.counter(STORAGE_MANAGER_STATUS_COUNTER_NAME,
                  OPERATION_TAG_NAME, "list",
                  STATUS_TAG_NAME, Integer.toString(response.statusCode()))
              .increment();
          try {
            return parseListResponse(response, prefix);
          } catch (IOException e) {
            throw ExceptionUtils.wrap(e);
          }
        })
        .whenComplete((ignored, ignoredException) ->
            sample.stop(Metrics.timer(STORAGE_MANAGER_TIMER_NAME, OPERATION_TAG_NAME, "list")));
  }

  /**
   * Serialized list response from storage manager
   */
  record Cdn3ListResponse(@NotNull List<Entry> objects, @Nullable String cursor) {

    record Entry(@NotNull String key, @NotNull long size) {}
  }

  private static ListResult parseListResponse(final HttpResponse<InputStream> httpListResponse, final String prefix)
      throws IOException {
    if (!HttpUtils.isSuccessfulResponse(httpListResponse.statusCode())) {
      throw new IOException("Failed to list objects: " + httpListResponse.statusCode());
    }
    final Cdn3ListResponse result = SystemMapper.jsonMapper()
        .readValue(httpListResponse.body(), Cdn3ListResponse.class);

    final List<ListResult.Entry> objects = new ArrayList<>(result.objects.size());
    for (Cdn3ListResponse.Entry entry : result.objects) {
      if (!entry.key().startsWith(prefix)) {
        logger.error("unexpected listing result from cdn3 - entry {} does not contain requested prefix {}",
            entry.key(), prefix);
        throw new IOException("prefix listing returned unexpected result");
      }
      objects.add(new ListResult.Entry(entry.key().substring(prefix.length()), entry.size()));
    }
    return new ListResult(objects, Optional.ofNullable(result.cursor));
  }


  /**
   * Serialized usage response from storage manager
   */
  record UsageResponse(@NotNull long numObjects, @NotNull long bytesUsed) {}


  @Override
  public CompletionStage<UsageInfo> calculateBytesUsed(final String prefix) {
    final Timer.Sample sample = Timer.start();
    final HttpRequest request = HttpRequest.newBuilder().GET()
        .uri(URI.create("%s%s".formatted(
            usageUrl(),
            HttpUtils.queryParamString(Map.of("prefix", prefix).entrySet()))))
        .header(CLIENT_ID_HEADER, clientId)
        .header(CLIENT_SECRET_HEADER, clientSecret)
        .build();
    return this.storageManagerHttpClient.sendAsync(request, HttpResponse.BodyHandlers.ofInputStream())
        .thenApply(response -> {
          Metrics.counter(STORAGE_MANAGER_STATUS_COUNTER_NAME,
                  OPERATION_TAG_NAME, "usage",
                  STATUS_TAG_NAME, Integer.toString(response.statusCode()))
              .increment();
          try {
            return parseUsageResponse(response);
          } catch (IOException e) {
            throw ExceptionUtils.wrap(e);
          }
        })
        .whenComplete((ignored, ignoredException) ->
            sample.stop(Metrics.timer(STORAGE_MANAGER_TIMER_NAME, OPERATION_TAG_NAME, "usage")));
  }

  private static UsageInfo parseUsageResponse(final HttpResponse<InputStream> httpUsageResponse) throws IOException {
    if (!HttpUtils.isSuccessfulResponse(httpUsageResponse.statusCode())) {
      throw new IOException("Failed to retrieve usage: " + httpUsageResponse.statusCode());
    }
    final UsageResponse response = SystemMapper.jsonMapper().readValue(httpUsageResponse.body(), UsageResponse.class);
    return new UsageInfo(response.bytesUsed(), response.numObjects);
  }

  /**
   * Serialized delete response from storage manager
   */
  record DeleteResponse(@NotNull long bytesDeleted) {}

  public CompletionStage<Long> delete(final String key) {
    final Timer.Sample sample = Timer.start();
    final HttpRequest request = HttpRequest.newBuilder().DELETE()
        .uri(URI.create(deleteUrl(key)))
        .header(CLIENT_ID_HEADER, clientId)
        .header(CLIENT_SECRET_HEADER, clientSecret)
        .build();
    return this.storageManagerHttpClient.sendAsync(request, HttpResponse.BodyHandlers.ofInputStream())
        .thenApply(response -> {
          Metrics.counter(STORAGE_MANAGER_STATUS_COUNTER_NAME,
                  OPERATION_TAG_NAME, "delete",
                  STATUS_TAG_NAME, Integer.toString(response.statusCode()))
              .increment();
          try {
            long bytesDeleted = parseDeleteResponse(response);
            Metrics.counter(OBJECT_REMOVED_ON_DELETE_COUNTER_NAME,
                    "removed", Boolean.toString(bytesDeleted > 0))
                .increment();
            return bytesDeleted;
          } catch (IOException e) {
            throw ExceptionUtils.wrap(e);
          }
        })
        .whenComplete((ignored, ignoredException) ->
            sample.stop(Metrics.timer(STORAGE_MANAGER_TIMER_NAME, OPERATION_TAG_NAME, "delete")));
  }

  private long parseDeleteResponse(final HttpResponse<InputStream> httpDeleteResponse) throws IOException {
    if (!HttpUtils.isSuccessfulResponse(httpDeleteResponse.statusCode())) {
      throw new IOException("Failed to retrieve usage: " + httpDeleteResponse.statusCode());
    }
    return SystemMapper.jsonMapper().readValue(httpDeleteResponse.body(), DeleteResponse.class).bytesDeleted();
  }

  private String deleteUrl(final String key) {
    return "%s/%s/%s".formatted(storageManagerBaseUrl, Cdn3BackupCredentialGenerator.CDN_PATH, key);
  }

  private String usageUrl() {
    return "%s/usage".formatted(storageManagerBaseUrl);
  }

  private String listUrl() {
    return "%s/%s/".formatted(storageManagerBaseUrl, Cdn3BackupCredentialGenerator.CDN_PATH);
  }

  private String copyUrl() {
    return "%s/copy".formatted(storageManagerBaseUrl);
  }
}
