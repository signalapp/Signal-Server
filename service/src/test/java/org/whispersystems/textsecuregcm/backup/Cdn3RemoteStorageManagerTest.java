package org.whispersystems.textsecuregcm.backup;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.Mac;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.configuration.RetryConfiguration;
import org.whispersystems.textsecuregcm.util.CompletableFutureTestUtil;

@ExtendWith(DropwizardExtensionsSupport.class)
public class Cdn3RemoteStorageManagerTest {

  private static byte[] HMAC_KEY = getRandomBytes(32);
  private static byte[] AES_KEY = getRandomBytes(32);
  private static byte[] IV = getRandomBytes(16);

  @RegisterExtension
  private final WireMockExtension wireMock = WireMockExtension.newInstance()
      .options(wireMockConfig().dynamicPort())
      .build();

  private static String SMALL_CDN2 = "a small object from cdn2";
  private static String SMALL_CDN3 = "a small object from cdn3";
  private static String LARGE = "a".repeat(1024 * 1024 * 5);

  private RemoteStorageManager remoteStorageManager;

  @BeforeEach
  public void init() throws CertificateException {
    remoteStorageManager = new Cdn3RemoteStorageManager(
        Executors.newSingleThreadScheduledExecutor(),
        new CircuitBreakerConfiguration(),
        new RetryConfiguration(),
        Collections.emptyList());

    wireMock.stubFor(get(urlEqualTo("/cdn2/source/small"))
        .willReturn(aResponse()
            .withHeader("Content-Length", Integer.toString(SMALL_CDN2.length()))
            .withBody(SMALL_CDN2)));

    wireMock.stubFor(get(urlEqualTo("/cdn3/source/small"))
        .willReturn(aResponse()
            .withHeader("Content-Length", Integer.toString(SMALL_CDN3.length()))
            .withBody(SMALL_CDN3)));

    wireMock.stubFor(get(urlEqualTo("/cdn3/source/large"))
        .willReturn(aResponse()
            .withHeader("Content-Length", Integer.toString(LARGE.length()))
            .withBody(LARGE)));

    wireMock.stubFor(get(urlEqualTo("/cdn3/source/missing"))
        .willReturn(aResponse().withStatus(404)));
  }

  @ParameterizedTest
  @ValueSource(ints = {2, 3})
  public void copySmall(final int sourceCdn)
      throws InvalidAlgorithmParameterException, InvalidKeyException, IllegalBlockSizeException, BadPaddingException {

    final String expectedSource = switch (sourceCdn) {
      case 2 -> SMALL_CDN2;
      case 3 -> SMALL_CDN3;
      default -> throw new AssertionError();
    };

    wireMock.stubFor(post(urlEqualTo("/cdn3/dest"))
        .willReturn(aResponse()
            .withStatus(201)));

    remoteStorageManager.copy(
            URI.create(wireMock.url("/cdn" + sourceCdn + "/source/small")),
            expectedSource.length(),
            new MediaEncryptionParameters(AES_KEY, HMAC_KEY, IV),
            new MessageBackupUploadDescriptor(3, "test", Collections.emptyMap(), wireMock.url("/cdn3/dest")))
        .toCompletableFuture().join();

    final byte[] destBody = wireMock.findAll(postRequestedFor(urlEqualTo("/cdn3/dest"))).get(0).getBody();
    assertThat(new String(decrypt(destBody), StandardCharsets.UTF_8))
        .isEqualTo(expectedSource);
  }

  @Test
  public void copyLarge()
      throws InvalidAlgorithmParameterException, IllegalBlockSizeException, BadPaddingException, InvalidKeyException {
    wireMock.stubFor(post(urlEqualTo("/cdn3/dest"))
        .willReturn(aResponse()
            .withStatus(201)));
    final MediaEncryptionParameters params = new MediaEncryptionParameters(AES_KEY, HMAC_KEY, IV);
    remoteStorageManager.copy(
            URI.create(wireMock.url("/cdn3/source/large")),
            LARGE.length(),
            params,
            new MessageBackupUploadDescriptor(3, "test", Collections.emptyMap(), wireMock.url("/cdn3/dest")))
        .toCompletableFuture().join();

    final byte[] destBody = wireMock.findAll(postRequestedFor(urlEqualTo("/cdn3/dest"))).get(0).getBody();
    assertThat(destBody.length).isEqualTo(new BackupMediaEncrypter(params).outputSize(LARGE.length()));
    assertThat(new String(decrypt(destBody), StandardCharsets.UTF_8)).isEqualTo(LARGE);
  }

  @Test
  public void incorrectLength() {
    CompletableFutureTestUtil.assertFailsWithCause(InvalidLengthException.class,
        remoteStorageManager.copy(
                URI.create(wireMock.url("/cdn3/source/small")),
                SMALL_CDN3.length() - 1,
                new MediaEncryptionParameters(AES_KEY, HMAC_KEY, IV),
                new MessageBackupUploadDescriptor(3, "test", Collections.emptyMap(), wireMock.url("/cdn3/dest")))
            .toCompletableFuture());
  }

  @Test
  public void sourceMissing() {
    CompletableFutureTestUtil.assertFailsWithCause(SourceObjectNotFoundException.class,
        remoteStorageManager.copy(
                URI.create(wireMock.url("/cdn3/source/missing")),
                1,
                new MediaEncryptionParameters(AES_KEY, HMAC_KEY, IV),
                new MessageBackupUploadDescriptor(3, "test", Collections.emptyMap(), wireMock.url("/cdn3/dest")))
            .toCompletableFuture());
  }

  private byte[] decrypt(final byte[] encrypted)
      throws InvalidAlgorithmParameterException, InvalidKeyException, IllegalBlockSizeException, BadPaddingException {

    final Mac mac;
    try {
      mac = Mac.getInstance("HmacSHA256");
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    }

    mac.init(new SecretKeySpec(HMAC_KEY, "HmacSHA256"));
    mac.update(encrypted, 0, encrypted.length - mac.getMacLength());
    assertArrayEquals(mac.doFinal(),
        Arrays.copyOfRange(encrypted, encrypted.length - mac.getMacLength(), encrypted.length));
    assertArrayEquals(IV, Arrays.copyOf(encrypted, 16));

    final Cipher cipher;
    try {
      cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
    } catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
      throw new AssertionError(e);
    }
    cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(AES_KEY, "AES"), new IvParameterSpec(IV));
    return cipher.doFinal(encrypted, IV.length, encrypted.length - IV.length - mac.getMacLength());
  }

  private static byte[] getRandomBytes(int length) {
    byte[] result = new byte[length];
    ThreadLocalRandom.current().nextBytes(result);
    return result;
  }
}
