package org.whispersystems.textsecuregcm.grpc.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.grpc.ManagedChannel;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.signal.chat.rpc.GetAuthenticatedDeviceRequest;
import org.signal.chat.rpc.GetAuthenticatedDeviceResponse;
import org.signal.chat.rpc.GetRequestAttributesRequest;
import org.signal.chat.rpc.GetRequestAttributesResponse;
import org.signal.chat.rpc.RequestAttributesGrpc;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.signal.libsignal.protocol.ecc.ECPublicKey;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.grpc.ProhibitAuthenticationInterceptor;
import org.whispersystems.textsecuregcm.auth.grpc.RequireAuthenticationInterceptor;
import org.whispersystems.textsecuregcm.grpc.GrpcTestUtils;
import org.whispersystems.textsecuregcm.grpc.RequestAttributesInterceptor;
import org.whispersystems.textsecuregcm.grpc.RequestAttributesServiceImpl;
import org.whispersystems.textsecuregcm.storage.ClientPublicKeysManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.UUIDUtil;

class WebSocketNoiseTunnelServerIntegrationTest extends AbstractLeakDetectionTest {

  private static NioEventLoopGroup nioEventLoopGroup;
  private static DefaultEventLoopGroup defaultEventLoopGroup;
  private static ExecutorService delegatedTaskExecutor;

  private static X509Certificate serverTlsCertificate;

  private ClientConnectionManager clientConnectionManager;
  private ClientPublicKeysManager clientPublicKeysManager;

  private ECKeyPair rootKeyPair;
  private ECKeyPair clientKeyPair;

  private ManagedLocalGrpcServer authenticatedGrpcServer;
  private ManagedLocalGrpcServer anonymousGrpcServer;

  private WebsocketNoiseTunnelServer websocketNoiseTunnelServer;

  private static final UUID ACCOUNT_IDENTIFIER = UUID.randomUUID();
  private static final byte DEVICE_ID = Device.PRIMARY_ID;

  private static final String RECOGNIZED_PROXY_SECRET = RandomStringUtils.randomAlphanumeric(16);

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

  @BeforeAll
  static void setUpBeforeAll() throws CertificateException {
    nioEventLoopGroup = new NioEventLoopGroup();
    defaultEventLoopGroup = new DefaultEventLoopGroup();
    delegatedTaskExecutor = Executors.newSingleThreadExecutor();

    final CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
    serverTlsCertificate = (X509Certificate) certificateFactory.generateCertificate(
        new ByteArrayInputStream(SERVER_CERTIFICATE.getBytes(StandardCharsets.UTF_8)));
  }

  @BeforeEach
  void setUp() throws NoSuchAlgorithmException, InvalidKeySpecException, IOException, InterruptedException {

    final PrivateKey serverTlsPrivateKey;
    {
      final KeyFactory keyFactory = KeyFactory.getInstance("EC");
      serverTlsPrivateKey =
          keyFactory.generatePrivate(new PKCS8EncodedKeySpec(Base64.getMimeDecoder().decode(SERVER_PRIVATE_KEY)));
    }

    rootKeyPair = Curve.generateKeyPair();
    clientKeyPair = Curve.generateKeyPair();
    final ECKeyPair serverKeyPair = Curve.generateKeyPair();

    clientConnectionManager = new ClientConnectionManager();

    clientPublicKeysManager = mock(ClientPublicKeysManager.class);
    when(clientPublicKeysManager.findPublicKey(any(), anyByte()))
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    when(clientPublicKeysManager.findPublicKey(ACCOUNT_IDENTIFIER, DEVICE_ID))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(clientKeyPair.getPublicKey())));

    final LocalAddress authenticatedGrpcServerAddress = new LocalAddress("test-grpc-service-authenticated");
    final LocalAddress anonymousGrpcServerAddress = new LocalAddress("test-grpc-service-anonymous");

    authenticatedGrpcServer = new ManagedLocalGrpcServer(authenticatedGrpcServerAddress, defaultEventLoopGroup) {
      @Override
      protected void configureServer(final ServerBuilder<?> serverBuilder) {
        serverBuilder.addService(new RequestAttributesServiceImpl())
            .intercept(new RequestAttributesInterceptor(clientConnectionManager))
            .intercept(new RequireAuthenticationInterceptor(clientConnectionManager));
      }
    };

    authenticatedGrpcServer.start();

    anonymousGrpcServer = new ManagedLocalGrpcServer(anonymousGrpcServerAddress, defaultEventLoopGroup) {
      @Override
      protected void configureServer(final ServerBuilder<?> serverBuilder) {
        serverBuilder.addService(new RequestAttributesServiceImpl())
            .intercept(new RequestAttributesInterceptor(clientConnectionManager))
            .intercept(new ProhibitAuthenticationInterceptor(clientConnectionManager));
      }
    };

    anonymousGrpcServer.start();

    websocketNoiseTunnelServer = new WebsocketNoiseTunnelServer(0,
        new X509Certificate[] { serverTlsCertificate },
        serverTlsPrivateKey,
        nioEventLoopGroup,
        delegatedTaskExecutor,
        clientConnectionManager,
        clientPublicKeysManager,
        serverKeyPair,
        rootKeyPair.getPrivateKey().calculateSignature(serverKeyPair.getPublicKey().getPublicKeyBytes()),
        authenticatedGrpcServerAddress,
        anonymousGrpcServerAddress,
        RECOGNIZED_PROXY_SECRET);

    websocketNoiseTunnelServer.start();
  }

  @AfterEach
  void tearDown() throws InterruptedException {
    websocketNoiseTunnelServer.stop();
    authenticatedGrpcServer.stop();
    anonymousGrpcServer.stop();
  }

  @AfterAll
  static void tearDownAfterAll() throws InterruptedException {
    nioEventLoopGroup.shutdownGracefully(100, 100, TimeUnit.MILLISECONDS).await();
    defaultEventLoopGroup.shutdownGracefully(100, 100, TimeUnit.MILLISECONDS).await();

    delegatedTaskExecutor.shutdown();
    //noinspection ResultOfMethodCallIgnored
    delegatedTaskExecutor.awaitTermination(1, TimeUnit.SECONDS);
  }

  @Test
  void connectAuthenticated() throws InterruptedException {
    try (final WebSocketNoiseTunnelClient webSocketNoiseTunnelClient = buildAndStartAuthenticatedClient()) {
      final ManagedChannel channel = buildManagedChannel(webSocketNoiseTunnelClient.getLocalAddress());

      try {
        final GetAuthenticatedDeviceResponse response = RequestAttributesGrpc.newBlockingStub(channel)
            .getAuthenticatedDevice(GetAuthenticatedDeviceRequest.newBuilder().build());

        assertEquals(UUIDUtil.toByteString(ACCOUNT_IDENTIFIER), response.getAccountIdentifier());
        assertEquals(DEVICE_ID, response.getDeviceId());
      } finally {
        channel.shutdown();
      }
    }
  }

  @Test
  void connectAuthenticatedBadServerKeySignature() throws InterruptedException {
    final WebSocketCloseListener webSocketCloseListener = mock(WebSocketCloseListener.class);


    // Try to verify the server's public key with something other than the key with which it was signed
    try (final WebSocketNoiseTunnelClient webSocketNoiseTunnelClient =
        buildAndStartAuthenticatedClient(webSocketCloseListener, Curve.generateKeyPair().getPublicKey(), new DefaultHttpHeaders())) {

      final ManagedChannel channel = buildManagedChannel(webSocketNoiseTunnelClient.getLocalAddress());

      try {
        //noinspection ResultOfMethodCallIgnored
        GrpcTestUtils.assertStatusException(Status.UNAVAILABLE,
            () -> RequestAttributesGrpc.newBlockingStub(channel)
                .getRequestAttributes(GetRequestAttributesRequest.newBuilder().build()));
      } finally {
        channel.shutdown();
      }
    }

    verify(webSocketCloseListener).handleWebSocketClosedByClient(ApplicationWebSocketCloseReason.NOISE_HANDSHAKE_ERROR.getStatusCode());
  }

  @Test
  void connectAuthenticatedMismatchedClientPublicKey() throws InterruptedException {
    final WebSocketCloseListener webSocketCloseListener = mock(WebSocketCloseListener.class);

    when(clientPublicKeysManager.findPublicKey(ACCOUNT_IDENTIFIER, DEVICE_ID))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(Curve.generateKeyPair().getPublicKey())));

    try (final WebSocketNoiseTunnelClient webSocketNoiseTunnelClient =
        buildAndStartAuthenticatedClient(webSocketCloseListener)) {

      final ManagedChannel channel = buildManagedChannel(webSocketNoiseTunnelClient.getLocalAddress());

      try {
        //noinspection ResultOfMethodCallIgnored
        GrpcTestUtils.assertStatusException(Status.UNAVAILABLE,
            () -> RequestAttributesGrpc.newBlockingStub(channel)
                .getRequestAttributes(GetRequestAttributesRequest.newBuilder().build()));
      } finally {
        channel.shutdown();
      }
    }

    verify(webSocketCloseListener).handleWebSocketClosedByServer(ApplicationWebSocketCloseReason.CLIENT_AUTHENTICATION_ERROR.getStatusCode());
  }

  @Test
  void connectAuthenticatedUnrecognizedDevice() throws InterruptedException {
    final WebSocketCloseListener webSocketCloseListener = mock(WebSocketCloseListener.class);

    when(clientPublicKeysManager.findPublicKey(ACCOUNT_IDENTIFIER, DEVICE_ID))
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    try (final WebSocketNoiseTunnelClient webSocketNoiseTunnelClient =
        buildAndStartAuthenticatedClient(webSocketCloseListener)) {

      final ManagedChannel channel = buildManagedChannel(webSocketNoiseTunnelClient.getLocalAddress());

      try {
        //noinspection ResultOfMethodCallIgnored
        GrpcTestUtils.assertStatusException(Status.UNAVAILABLE,
            () -> RequestAttributesGrpc.newBlockingStub(channel)
                .getRequestAttributes(GetRequestAttributesRequest.newBuilder().build()));
      } finally {
        channel.shutdown();
      }
    }

    verify(webSocketCloseListener).handleWebSocketClosedByServer(ApplicationWebSocketCloseReason.CLIENT_AUTHENTICATION_ERROR.getStatusCode());
  }

  @Test
  void connectAuthenticatedToAnonymousService() throws InterruptedException {
    final WebSocketCloseListener webSocketCloseListener = mock(WebSocketCloseListener.class);

    try (final WebSocketNoiseTunnelClient webSocketNoiseTunnelClient = new WebSocketNoiseTunnelClient(
        websocketNoiseTunnelServer.getLocalAddress(),
        URI.create("wss://localhost/anonymous"),
        true,
        clientKeyPair,
        rootKeyPair.getPublicKey(),
        ACCOUNT_IDENTIFIER,
        DEVICE_ID,
        new DefaultHttpHeaders(),
        serverTlsCertificate,
        nioEventLoopGroup,
        webSocketCloseListener)
        .start()) {

      final ManagedChannel channel = buildManagedChannel(webSocketNoiseTunnelClient.getLocalAddress());

      try {
        //noinspection ResultOfMethodCallIgnored
        GrpcTestUtils.assertStatusException(Status.UNAVAILABLE,
            () -> RequestAttributesGrpc.newBlockingStub(channel)
                .getRequestAttributes(GetRequestAttributesRequest.newBuilder().build()));
      } finally {
        channel.shutdown();
      }
    }

    verify(webSocketCloseListener).handleWebSocketClosedByClient(ApplicationWebSocketCloseReason.NOISE_HANDSHAKE_ERROR.getStatusCode());
  }

  @Test
  void connectAnonymous() throws InterruptedException {
    try (final WebSocketNoiseTunnelClient webSocketNoiseTunnelClient = buildAndStartAnonymousClient()) {
      final ManagedChannel channel = buildManagedChannel(webSocketNoiseTunnelClient.getLocalAddress());

      try {
        final GetAuthenticatedDeviceResponse response = RequestAttributesGrpc.newBlockingStub(channel)
            .getAuthenticatedDevice(GetAuthenticatedDeviceRequest.newBuilder().build());

        assertTrue(response.getAccountIdentifier().isEmpty());
        assertEquals(0, response.getDeviceId());
      } finally {
        channel.shutdown();
      }
    }
  }

  @Test
  void connectAnonymousBadServerKeySignature() throws InterruptedException {
    final WebSocketCloseListener webSocketCloseListener = mock(WebSocketCloseListener.class);

    // Try to verify the server's public key with something other than the key with which it was signed
    try (final WebSocketNoiseTunnelClient webSocketNoiseTunnelClient =
        buildAndStartAnonymousClient(webSocketCloseListener, Curve.generateKeyPair().getPublicKey(), new DefaultHttpHeaders())) {

      final ManagedChannel channel = buildManagedChannel(webSocketNoiseTunnelClient.getLocalAddress());

      try {
        //noinspection ResultOfMethodCallIgnored
        GrpcTestUtils.assertStatusException(Status.UNAVAILABLE,
            () -> RequestAttributesGrpc.newBlockingStub(channel)
                .getRequestAttributes(GetRequestAttributesRequest.newBuilder().build()));
      } finally {
        channel.shutdown();
      }
    }

    verify(webSocketCloseListener).handleWebSocketClosedByClient(ApplicationWebSocketCloseReason.NOISE_HANDSHAKE_ERROR.getStatusCode());
  }

  @Test
  void connectAnonymousToAuthenticatedService() throws InterruptedException {
    final WebSocketCloseListener webSocketCloseListener = mock(WebSocketCloseListener.class);

    try (final WebSocketNoiseTunnelClient websocketNoiseTunnelClient = new WebSocketNoiseTunnelClient(
        websocketNoiseTunnelServer.getLocalAddress(),
        URI.create("wss://localhost/authenticated"),
        false,
        null,
        rootKeyPair.getPublicKey(),
        null,
        (byte) 0,
        new DefaultHttpHeaders(),
        serverTlsCertificate,
        nioEventLoopGroup,
        webSocketCloseListener)
        .start()) {

      final ManagedChannel channel = buildManagedChannel(websocketNoiseTunnelClient.getLocalAddress());

      try {
        //noinspection ResultOfMethodCallIgnored
        GrpcTestUtils.assertStatusException(Status.UNAVAILABLE,
            () -> RequestAttributesGrpc.newBlockingStub(channel)
                .getRequestAttributes(GetRequestAttributesRequest.newBuilder().build()));
      } finally {
        channel.shutdown();
      }
    }

    verify(webSocketCloseListener).handleWebSocketClosedByClient(ApplicationWebSocketCloseReason.NOISE_HANDSHAKE_ERROR.getStatusCode());
  }

  private ManagedChannel buildManagedChannel(final LocalAddress localAddress) {
    return NettyChannelBuilder.forAddress(localAddress)
        .channelType(LocalChannel.class)
        .eventLoopGroup(defaultEventLoopGroup)
        .usePlaintext()
        .build();
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
        new URI("https", null, "localhost", websocketNoiseTunnelServer.getLocalAddress().getPort(), "/authenticated", null, null);

    final URI incorrectUri =
        new URI("https", null, "localhost", websocketNoiseTunnelServer.getLocalAddress().getPort(), "/incorrect", null, null);

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

  @Test
  void getRequestAttributes() throws InterruptedException {
    final String remoteAddress = "4.5.6.7";
    final String acceptLanguage = "en";
    final String userAgent = "Signal-Desktop/1.2.3 Linux";

    final HttpHeaders headers = new DefaultHttpHeaders()
        .add(WebsocketHandshakeCompleteHandler.RECOGNIZED_PROXY_SECRET_HEADER, RECOGNIZED_PROXY_SECRET)
        .add("X-Forwarded-For", remoteAddress)
        .add("Accept-Language", acceptLanguage)
        .add("User-Agent", userAgent);

    try (final WebSocketNoiseTunnelClient webSocketNoiseTunnelClient =
        buildAndStartAnonymousClient(WebSocketCloseListener.NOOP_LISTENER, rootKeyPair.getPublicKey(), headers)) {

      final ManagedChannel channel = buildManagedChannel(webSocketNoiseTunnelClient.getLocalAddress());

      try {
        final GetRequestAttributesResponse response = RequestAttributesGrpc.newBlockingStub(channel)
            .getRequestAttributes(GetRequestAttributesRequest.newBuilder().build());

        assertEquals(remoteAddress, response.getRemoteAddress());
        assertEquals(List.of(acceptLanguage), response.getAcceptableLanguagesList());

        assertEquals("DESKTOP", response.getUserAgent().getPlatform());
        assertEquals("1.2.3", response.getUserAgent().getVersion());
        assertEquals("Linux", response.getUserAgent().getAdditionalSpecifiers());
      } finally {
        channel.shutdown();
      }
    }
  }

  @Test
  void closeForReauthentication() throws InterruptedException {
    final CountDownLatch connectionCloseLatch = new CountDownLatch(1);
    final AtomicInteger serverCloseStatusCode = new AtomicInteger(0);
    final AtomicBoolean closedByServer = new AtomicBoolean(false);

    final WebSocketCloseListener webSocketCloseListener = new WebSocketCloseListener() {

      @Override
      public void handleWebSocketClosedByClient(final int statusCode) {
        serverCloseStatusCode.set(statusCode);
        closedByServer.set(false);
        connectionCloseLatch.countDown();
      }

      @Override
      public void handleWebSocketClosedByServer(final int statusCode) {
        serverCloseStatusCode.set(statusCode);
        closedByServer.set(true);
        connectionCloseLatch.countDown();
      }
    };

    try (final WebSocketNoiseTunnelClient webSocketNoiseTunnelClient = buildAndStartAuthenticatedClient(webSocketCloseListener)) {

      final ManagedChannel channel = buildManagedChannel(webSocketNoiseTunnelClient.getLocalAddress());

      try {
        final GetAuthenticatedDeviceResponse response = RequestAttributesGrpc.newBlockingStub(channel)
            .getAuthenticatedDevice(GetAuthenticatedDeviceRequest.newBuilder().build());

        assertEquals(UUIDUtil.toByteString(ACCOUNT_IDENTIFIER), response.getAccountIdentifier());
        assertEquals(DEVICE_ID, response.getDeviceId());

        clientConnectionManager.closeConnection(new AuthenticatedDevice(ACCOUNT_IDENTIFIER, DEVICE_ID));
        assertTrue(connectionCloseLatch.await(2, TimeUnit.SECONDS));

        assertEquals(ApplicationWebSocketCloseReason.REAUTHENTICATION_REQUIRED.getStatusCode(),
            serverCloseStatusCode.get());

        assertTrue(closedByServer.get());
      } finally {
        channel.shutdown();
      }
    }
  }

  private WebSocketNoiseTunnelClient buildAndStartAuthenticatedClient() throws InterruptedException {
    return buildAndStartAuthenticatedClient(WebSocketCloseListener.NOOP_LISTENER);
  }

  private WebSocketNoiseTunnelClient buildAndStartAuthenticatedClient(final WebSocketCloseListener webSocketCloseListener)
      throws InterruptedException {

    return buildAndStartAuthenticatedClient(webSocketCloseListener, rootKeyPair.getPublicKey(), new DefaultHttpHeaders());
  }

  private WebSocketNoiseTunnelClient buildAndStartAuthenticatedClient(final WebSocketCloseListener webSocketCloseListener,
      final ECPublicKey rootPublicKey,
      final HttpHeaders headers) throws InterruptedException {

    return new WebSocketNoiseTunnelClient(websocketNoiseTunnelServer.getLocalAddress(),
        WebSocketNoiseTunnelClient.AUTHENTICATED_WEBSOCKET_URI,
        true,
        clientKeyPair,
        rootPublicKey,
        ACCOUNT_IDENTIFIER,
        DEVICE_ID,
        headers,
        serverTlsCertificate,
        nioEventLoopGroup,
        webSocketCloseListener)
        .start();
  }

  private WebSocketNoiseTunnelClient buildAndStartAnonymousClient() throws InterruptedException {
    return buildAndStartAnonymousClient(WebSocketCloseListener.NOOP_LISTENER, rootKeyPair.getPublicKey(), new DefaultHttpHeaders());
  }

  private WebSocketNoiseTunnelClient buildAndStartAnonymousClient(final WebSocketCloseListener webSocketCloseListener,
      final ECPublicKey rootPublicKey,
      final HttpHeaders headers) throws InterruptedException {

    return new WebSocketNoiseTunnelClient(websocketNoiseTunnelServer.getLocalAddress(),
        WebSocketNoiseTunnelClient.ANONYMOUS_WEBSOCKET_URI,
        false,
        null,
        rootPublicKey,
        null,
        (byte) 0,
        headers,
        serverTlsCertificate,
        nioEventLoopGroup,
        webSocketCloseListener)
        .start();
  }
}
