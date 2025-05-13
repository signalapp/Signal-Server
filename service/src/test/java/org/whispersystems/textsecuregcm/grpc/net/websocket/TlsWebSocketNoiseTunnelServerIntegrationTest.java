package org.whispersystems.textsecuregcm.grpc.net.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.junit.jupiter.api.Test;
import org.signal.chat.rpc.GetRequestAttributesRequest;
import org.signal.chat.rpc.GetRequestAttributesResponse;
import org.signal.chat.rpc.RequestAttributesGrpc;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.signal.libsignal.protocol.ecc.ECPublicKey;
import org.whispersystems.textsecuregcm.grpc.GrpcTestUtils;
import org.whispersystems.textsecuregcm.grpc.net.AbstractNoiseTunnelServerIntegrationTest;
import org.whispersystems.textsecuregcm.grpc.net.GrpcClientConnectionManager;
import org.whispersystems.textsecuregcm.grpc.net.client.CloseFrameEvent;
import org.whispersystems.textsecuregcm.grpc.net.client.NoiseTunnelClient;
import org.whispersystems.textsecuregcm.storage.ClientPublicKeysManager;

class TlsWebSocketNoiseTunnelServerIntegrationTest extends AbstractNoiseTunnelServerIntegrationTest {
  private NoiseWebSocketTunnelServer tlsNoiseWebSocketTunnelServer;
  private X509Certificate serverTlsCertificate;


  // Please note that this certificate/key are used only for testing and are not used anywhere outside of this test.
  // They were generated with:
  //
  // ```shell
  // openssl req -newkey ec:<(openssl ecparam -name secp384r1) -keyout test.key -nodes -x509 -days 36500 -out test.crt -subj "/CN=localhost"
  // ```
  private static final String SERVER_CERTIFICATE = """
      -----BEGIN CERTIFICATE-----
      MIIBvDCCAUKgAwIBAgIUU16rjelaT/wClEM/SrW96VJbsiMwCgYIKoZIzj0EAwIw
      FDESMBAGA1UEAwwJbG9jYWxob3N0MCAXDTI0MDEyNTIzMjA0OVoYDzIxMjQwMTAx
      MjMyMDQ5WjAUMRIwEAYDVQQDDAlsb2NhbGhvc3QwdjAQBgcqhkjOPQIBBgUrgQQA
      IgNiAAQOKblDCvMdPKFZ7MRePDRbSnJ4fAUoyOlOfWW1UC7NH8X2Zug4DxCtjXCV
      jttLE0TjLvgAvlJAO53+WFZV6mAm9Hds2gXMLczRZZ7g74cHyh5qFRvKJh2GeDBq
      SlS8LQqjUzBRMB0GA1UdDgQWBBSk5UGHMmYrnaXZx+sZ1NixL5p0GTAfBgNVHSME
      GDAWgBSk5UGHMmYrnaXZx+sZ1NixL5p0GTAPBgNVHRMBAf8EBTADAQH/MAoGCCqG
      SM49BAMCA2gAMGUCMC/2Nbz2niZzz+If26n1TS68GaBlPhEqQQH4kX+De6xfeLCw
      XcCmGFLqypzWFEF+8AIxAJ2Pok9Kv2Zn+wl5KnU7d7zOcrKBZHkjXXlkMso9RWsi
      iOr9sHiO8Rn2u0xRKgU5Ig==
      -----END CERTIFICATE-----
      """;

  // BEGIN/END PRIVATE KEY header/footer removed for easier parsing
  private static final String SERVER_PRIVATE_KEY = """
      MIG2AgEAMBAGByqGSM49AgEGBSuBBAAiBIGeMIGbAgEBBDDSQpS2WpySnwihcuNj
      kOVBDXGOw2UbeG/DiFSNXunyQ+8DpyGSkKk4VsluPzrepXyhZANiAAQOKblDCvMd
      PKFZ7MRePDRbSnJ4fAUoyOlOfWW1UC7NH8X2Zug4DxCtjXCVjttLE0TjLvgAvlJA
      O53+WFZV6mAm9Hds2gXMLczRZZ7g74cHyh5qFRvKJh2GeDBqSlS8LQo=
      """;
  @Override
  protected void start(
      final NioEventLoopGroup eventLoopGroup,
      final Executor delegatedTaskExecutor,
      final GrpcClientConnectionManager grpcClientConnectionManager,
      final ClientPublicKeysManager clientPublicKeysManager,
      final ECKeyPair serverKeyPair,
      final LocalAddress authenticatedGrpcServerAddress,
      final LocalAddress anonymousGrpcServerAddress,
      final String recognizedProxySecret) throws Exception {
    final CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
    serverTlsCertificate = (X509Certificate) certificateFactory.generateCertificate(
        new ByteArrayInputStream(SERVER_CERTIFICATE.getBytes(StandardCharsets.UTF_8)));
    final PrivateKey serverTlsPrivateKey;
    final KeyFactory keyFactory = KeyFactory.getInstance("EC");
    serverTlsPrivateKey =
        keyFactory.generatePrivate(new PKCS8EncodedKeySpec(Base64.getMimeDecoder().decode(SERVER_PRIVATE_KEY)));
    tlsNoiseWebSocketTunnelServer = new NoiseWebSocketTunnelServer(0,
        new X509Certificate[]{serverTlsCertificate},
        serverTlsPrivateKey,
        eventLoopGroup,
        delegatedTaskExecutor,
        grpcClientConnectionManager,
        clientPublicKeysManager,
        serverKeyPair,
        authenticatedGrpcServerAddress,
        anonymousGrpcServerAddress,
        recognizedProxySecret);
    tlsNoiseWebSocketTunnelServer.start();
  }

  @Override
  protected void stop() throws InterruptedException {
    tlsNoiseWebSocketTunnelServer.stop();
  }

  @Override
  protected NoiseTunnelClient.Builder clientBuilder(final NioEventLoopGroup eventLoopGroup,
      final ECPublicKey serverPublicKey) {
    return new NoiseTunnelClient
        .Builder(tlsNoiseWebSocketTunnelServer.getLocalAddress(), eventLoopGroup, serverPublicKey)
        .setUseTls(serverTlsCertificate);
  }

  @Test
  void getRequestAttributes() throws InterruptedException {
    final String remoteAddress = "4.5.6.7";
    final String acceptLanguage = "en";
    final String userAgent = "Signal-Desktop/1.2.3 Linux";

    final HttpHeaders headers = new DefaultHttpHeaders()
        .add(WebsocketHandshakeCompleteHandler.RECOGNIZED_PROXY_SECRET_HEADER, RECOGNIZED_PROXY_SECRET)
        .add("X-Forwarded-For", remoteAddress);

    try (final NoiseTunnelClient client = anonymous()
        .setHeaders(headers)
        .setUserAgent(userAgent)
        .setAcceptLanguage(acceptLanguage)
        .build()) {

      final ManagedChannel channel = buildManagedChannel(client.getLocalAddress());

      try {
        final GetRequestAttributesResponse response = RequestAttributesGrpc.newBlockingStub(channel)
            .getRequestAttributes(GetRequestAttributesRequest.newBuilder().build());

        assertEquals(remoteAddress, response.getRemoteAddress());
        assertEquals(List.of(acceptLanguage), response.getAcceptableLanguagesList());
        assertEquals(userAgent, response.getUserAgent());
      } finally {
        channel.shutdown();
      }
    }
  }

  @Test
  void connectAuthenticatedToAnonymousService() throws InterruptedException, ExecutionException, TimeoutException {
    try (final NoiseTunnelClient client = authenticated()
        .setWebsocketUri(NoiseTunnelClient.ANONYMOUS_WEBSOCKET_URI)
        .build()) {
      final ManagedChannel channel = buildManagedChannel(client.getLocalAddress());

      try {
        //noinspection ResultOfMethodCallIgnored
        GrpcTestUtils.assertStatusException(Status.UNAVAILABLE,
            () -> RequestAttributesGrpc.newBlockingStub(channel)
                .getRequestAttributes(GetRequestAttributesRequest.newBuilder().build()));
      } finally {
        channel.shutdown();
      }
      assertClosedWith(client, CloseFrameEvent.CloseReason.NOISE_HANDSHAKE_ERROR);
    }
  }


  @Test
  void connectAnonymousToAuthenticatedService() throws InterruptedException, ExecutionException, TimeoutException {
    try (final NoiseTunnelClient client = anonymous()
        .setWebsocketUri(NoiseTunnelClient.AUTHENTICATED_WEBSOCKET_URI)
        .build()) {
      final ManagedChannel channel = buildManagedChannel(client.getLocalAddress());

      try {
        //noinspection ResultOfMethodCallIgnored
        GrpcTestUtils.assertStatusException(Status.UNAVAILABLE,
            () -> RequestAttributesGrpc.newBlockingStub(channel)
                .getRequestAttributes(GetRequestAttributesRequest.newBuilder().build()));
      } finally {
        channel.shutdown();
      }
      assertClosedWith(client, CloseFrameEvent.CloseReason.NOISE_HANDSHAKE_ERROR);
    }
  }

  @Test
  void rejectIllegalRequests() throws Exception {

    final KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
    keyStore.load(null, null);
    keyStore.setCertificateEntry("tunnel", serverTlsCertificate);

    final TrustManagerFactory trustManagerFactory =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());

    trustManagerFactory.init(keyStore);

    final SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(null, trustManagerFactory.getTrustManagers(), new SecureRandom());

    final URI authenticatedUri =
        new URI("https", null, "localhost", tlsNoiseWebSocketTunnelServer.getLocalAddress().getPort(), "/authenticated",
            null, null);

    final URI incorrectUri =
        new URI("https", null, "localhost", tlsNoiseWebSocketTunnelServer.getLocalAddress().getPort(), "/incorrect",
            null, null);

    try (final HttpClient httpClient = HttpClient.newBuilder().sslContext(sslContext).build()) {
      assertEquals(405, httpClient.send(HttpRequest.newBuilder()
                  .uri(authenticatedUri)
                  .PUT(HttpRequest.BodyPublishers.ofString("test"))
                  .build(),
              HttpResponse.BodyHandlers.ofString()).statusCode(),
          "Non-GET requests should not be allowed");

      assertEquals(426, httpClient.send(HttpRequest.newBuilder()
                  .GET()
                  .uri(authenticatedUri)
                  .build(),
              HttpResponse.BodyHandlers.ofString()).statusCode(),
          "GET requests without upgrade headers should not be allowed");

      assertEquals(404, httpClient.send(HttpRequest.newBuilder()
                  .GET()
                  .uri(incorrectUri)
                  .build(),
              HttpResponse.BodyHandlers.ofString()).statusCode(),
          "GET requests to unrecognized URIs should not be allowed");
    }
  }


}
