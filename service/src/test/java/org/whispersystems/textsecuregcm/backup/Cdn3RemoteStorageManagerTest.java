package org.whispersystems.textsecuregcm.backup;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import jakarta.ws.rs.core.HttpHeaders;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.configuration.Cdn3StorageManagerConfiguration;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.configuration.RetryConfiguration;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretString;
import org.whispersystems.textsecuregcm.util.CompletableFutureTestUtil;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

@ExtendWith(DropwizardExtensionsSupport.class)
public class Cdn3RemoteStorageManagerTest {

  private static final byte[] HMAC_KEY = TestRandomUtil.nextBytes(32);
  private static final byte[] AES_KEY = TestRandomUtil.nextBytes(32);

  @RegisterExtension
  private static final WireMockExtension wireMock = WireMockExtension.newInstance()
      .options(wireMockConfig().dynamicPort())
      .build();

  private RemoteStorageManager remoteStorageManager;

  @BeforeEach
  public void init() {
    remoteStorageManager = new Cdn3RemoteStorageManager(
        Executors.newCachedThreadPool(),
        Executors.newSingleThreadScheduledExecutor(),
        new Cdn3StorageManagerConfiguration(
            wireMock.url("storage-manager/"),
            "clientId",
            new SecretString("clientSecret"),
            Map.of(2, "gcs", 3, "r2"),
            2,
            new CircuitBreakerConfiguration(),
            new RetryConfiguration()));
  }

  @ParameterizedTest
  @ValueSource(ints = {2, 3})
  public void copy(final int sourceCdn) throws JsonProcessingException {
    final MediaEncryptionParameters encryptionParameters = new MediaEncryptionParameters(AES_KEY, HMAC_KEY);
    final String scheme = switch (sourceCdn) {
      case 2 -> "gcs";
      case 3 -> "r2";
      default -> throw new AssertionError();
    };
    final Cdn3RemoteStorageManager.Cdn3CopyRequest expectedCopyRequest = new Cdn3RemoteStorageManager.Cdn3CopyRequest(
        encryptionParameters,
        new Cdn3RemoteStorageManager.Cdn3CopyRequest.SourceDescriptor(scheme, "a/test/source"),
        100,
        "a/destination");
    wireMock.stubFor(put(urlEqualTo("/storage-manager/copy"))
        .withHeader(HttpHeaders.CONTENT_TYPE, equalTo("application/json"))
        .withRequestBody(WireMock.equalToJson(SystemMapper.jsonMapper().writeValueAsString(expectedCopyRequest)))
        .willReturn(aResponse().withStatus(204)));
    assertThatNoException().isThrownBy(() ->
        remoteStorageManager.copy(
                sourceCdn,
                "a/test/source",
                100,
                encryptionParameters,
                "a/destination")
            .toCompletableFuture().join());
  }

  @Test
  public void copyIncorrectLength() {
    wireMock.stubFor(put(urlPathEqualTo("/storage-manager/copy")).willReturn(aResponse().withStatus(409)));
    CompletableFutureTestUtil.assertFailsWithCause(InvalidLengthException.class,
        remoteStorageManager.copy(
            2,
            "a/test/source",
            100,
            new MediaEncryptionParameters(AES_KEY, HMAC_KEY),
            "a/destination").toCompletableFuture());
  }

  @Test
  public void copySourceMissing() {
    wireMock.stubFor(put(urlPathEqualTo("/storage-manager/copy")).willReturn(aResponse().withStatus(404)));
    CompletableFutureTestUtil.assertFailsWithCause(SourceObjectNotFoundException.class,
        remoteStorageManager.copy(
            2,
            "a/test/source",
            100,
            new MediaEncryptionParameters(AES_KEY, HMAC_KEY),
            "a/destination").toCompletableFuture());
  }

  @Test
  public void copyUnknownCdn() {
    CompletableFutureTestUtil.assertFailsWithCause(SourceObjectNotFoundException.class,
        remoteStorageManager.copy(
            0,
            "a/test/source",
            100,
            new MediaEncryptionParameters(AES_KEY, HMAC_KEY),
            "a/destination").toCompletableFuture());
  }

  @Test
  public void list() throws JsonProcessingException {
    wireMock.stubFor(get(urlPathEqualTo("/storage-manager/backups/"))
        .withQueryParam("prefix", equalTo("abc/"))
        .withQueryParam("limit", equalTo("3"))
        .withHeader(Cdn3RemoteStorageManager.CLIENT_ID_HEADER, equalTo("clientId"))
        .withHeader(Cdn3RemoteStorageManager.CLIENT_SECRET_HEADER, equalTo("clientSecret"))
        .willReturn(aResponse()
            .withBody(SystemMapper.jsonMapper().writeValueAsString(new Cdn3RemoteStorageManager.Cdn3ListResponse(
                List.of(
                    new Cdn3RemoteStorageManager.Cdn3ListResponse.Entry("abc/x/y", 3),
                    new Cdn3RemoteStorageManager.Cdn3ListResponse.Entry("abc/y", 4),
                    new Cdn3RemoteStorageManager.Cdn3ListResponse.Entry("abc/z", 5)
                ), "cursor")))));
    final RemoteStorageManager.ListResult result = remoteStorageManager
        .list("abc/", Optional.empty(), 3)
        .toCompletableFuture().join();
    assertThat(result.cursor()).get().isEqualTo("cursor");
    assertThat(result.objects()).hasSize(3);

    // should strip the common prefix
    assertThat(result.objects()).isEqualTo(List.of(
        new RemoteStorageManager.ListResult.Entry("x/y", 3),
        new RemoteStorageManager.ListResult.Entry("y", 4),
        new RemoteStorageManager.ListResult.Entry("z", 5)));
  }

  @Test
  public void prefixMissing() throws JsonProcessingException {
    wireMock.stubFor(get(urlPathEqualTo("/storage-manager/backups/"))
        .willReturn(aResponse()
            .withBody(SystemMapper.jsonMapper().writeValueAsString(new Cdn3RemoteStorageManager.Cdn3ListResponse(
                List.of(new Cdn3RemoteStorageManager.Cdn3ListResponse.Entry("x", 3)),
                "cursor")))));
    CompletableFutureTestUtil.assertFailsWithCause(IOException.class,
        remoteStorageManager.list("abc/", Optional.empty(), 3).toCompletableFuture());
  }

  @Test
  public void usage() throws JsonProcessingException {
    wireMock.stubFor(get(urlPathEqualTo("/storage-manager/usage"))
        .withQueryParam("prefix", equalTo("abc/"))
        .withHeader(Cdn3RemoteStorageManager.CLIENT_ID_HEADER, equalTo("clientId"))
        .withHeader(Cdn3RemoteStorageManager.CLIENT_SECRET_HEADER, equalTo("clientSecret"))
        .willReturn(aResponse()
            .withBody(SystemMapper.jsonMapper().writeValueAsString(new Cdn3RemoteStorageManager.UsageResponse(
                17,
                113)))));
    final UsageInfo result = remoteStorageManager.calculateBytesUsed("abc/")
        .toCompletableFuture()
        .join();
    assertThat(result.numObjects()).isEqualTo(17);
    assertThat(result.bytesUsed()).isEqualTo(113);
  }

  @Test
  public void delete() throws JsonProcessingException {
    wireMock.stubFor(WireMock.delete(urlEqualTo("/storage-manager/backups/abc/def"))
        .withHeader(Cdn3RemoteStorageManager.CLIENT_ID_HEADER, equalTo("clientId"))
        .withHeader(Cdn3RemoteStorageManager.CLIENT_SECRET_HEADER, equalTo("clientSecret"))
        .willReturn(aResponse()
            .withBody(SystemMapper.jsonMapper().writeValueAsString(new Cdn3RemoteStorageManager.DeleteResponse(9L)))));
    final long deleted = remoteStorageManager.delete("abc/def").toCompletableFuture().join();
    assertThat(deleted).isEqualTo(9L);
  }
}
