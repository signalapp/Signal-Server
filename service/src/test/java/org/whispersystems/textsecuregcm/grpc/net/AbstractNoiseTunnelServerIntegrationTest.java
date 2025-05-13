package org.whispersystems.textsecuregcm.grpc.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.codec.haproxy.HAProxyCommand;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyProtocolVersion;
import io.netty.handler.codec.haproxy.HAProxyProxiedProtocol;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.signal.chat.rpc.EchoRequest;
import org.signal.chat.rpc.EchoResponse;
import org.signal.chat.rpc.EchoServiceGrpc;
import org.signal.chat.rpc.GetAuthenticatedDeviceRequest;
import org.signal.chat.rpc.GetAuthenticatedDeviceResponse;
import org.signal.chat.rpc.GetRequestAttributesRequest;
import org.signal.chat.rpc.RequestAttributesGrpc;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.signal.libsignal.protocol.ecc.ECPublicKey;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.grpc.ProhibitAuthenticationInterceptor;
import org.whispersystems.textsecuregcm.auth.grpc.RequireAuthenticationInterceptor;
import org.whispersystems.textsecuregcm.grpc.ChannelShutdownInterceptor;
import org.whispersystems.textsecuregcm.grpc.EchoServiceImpl;
import org.whispersystems.textsecuregcm.grpc.GrpcTestUtils;
import org.whispersystems.textsecuregcm.grpc.RequestAttributesInterceptor;
import org.whispersystems.textsecuregcm.grpc.RequestAttributesServiceImpl;
import org.whispersystems.textsecuregcm.grpc.net.client.CloseFrameEvent;
import org.whispersystems.textsecuregcm.grpc.net.client.NoiseTunnelClient;
import org.whispersystems.textsecuregcm.storage.ClientPublicKeysManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.UUIDUtil;

public abstract class AbstractNoiseTunnelServerIntegrationTest extends AbstractLeakDetectionTest {

  private static NioEventLoopGroup nioEventLoopGroup;
  private static DefaultEventLoopGroup defaultEventLoopGroup;
  private static ExecutorService delegatedTaskExecutor;
  private static ExecutorService serverCallExecutor;

  private GrpcClientConnectionManager grpcClientConnectionManager;
  private ClientPublicKeysManager clientPublicKeysManager;

  private ECKeyPair serverKeyPair;
  private ECKeyPair clientKeyPair;

  private ManagedLocalGrpcServer authenticatedGrpcServer;
  private ManagedLocalGrpcServer anonymousGrpcServer;

  private static final UUID ACCOUNT_IDENTIFIER = UUID.randomUUID();
  private static final byte DEVICE_ID = Device.PRIMARY_ID;

  public static final String RECOGNIZED_PROXY_SECRET = RandomStringUtils.secure().nextAlphanumeric(16);

  @BeforeAll
  static void setUpBeforeAll() {
    nioEventLoopGroup = new NioEventLoopGroup();
    defaultEventLoopGroup = new DefaultEventLoopGroup();
    delegatedTaskExecutor = Executors.newVirtualThreadPerTaskExecutor();
    serverCallExecutor = Executors.newVirtualThreadPerTaskExecutor();
  }

  @BeforeEach
  void setUp() throws Exception {

    clientKeyPair = Curve.generateKeyPair();
    serverKeyPair = Curve.generateKeyPair();

    grpcClientConnectionManager = new GrpcClientConnectionManager();

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
        serverBuilder
            .executor(serverCallExecutor)
            .addService(new RequestAttributesServiceImpl())
            .addService(new EchoServiceImpl())
            .intercept(new ChannelShutdownInterceptor(grpcClientConnectionManager))
            .intercept(new RequestAttributesInterceptor(grpcClientConnectionManager))
            .intercept(new RequireAuthenticationInterceptor(grpcClientConnectionManager));
      }
    };

    authenticatedGrpcServer.start();

    anonymousGrpcServer = new ManagedLocalGrpcServer(anonymousGrpcServerAddress, defaultEventLoopGroup) {
      @Override
      protected void configureServer(final ServerBuilder<?> serverBuilder) {
        serverBuilder
            .executor(serverCallExecutor)
            .addService(new RequestAttributesServiceImpl())
            .intercept(new RequestAttributesInterceptor(grpcClientConnectionManager))
            .intercept(new ProhibitAuthenticationInterceptor(grpcClientConnectionManager));
      }
    };

    anonymousGrpcServer.start();
    this.start(
        nioEventLoopGroup,
        delegatedTaskExecutor,
        grpcClientConnectionManager,
        clientPublicKeysManager,
        serverKeyPair,
        authenticatedGrpcServerAddress, anonymousGrpcServerAddress,
        RECOGNIZED_PROXY_SECRET);
  }


  protected abstract void start(
      final NioEventLoopGroup eventLoopGroup,
      final Executor delegatedTaskExecutor,
      final GrpcClientConnectionManager grpcClientConnectionManager,
      final ClientPublicKeysManager clientPublicKeysManager,
      final ECKeyPair serverKeyPair,
      final LocalAddress authenticatedGrpcServerAddress,
      final LocalAddress anonymousGrpcServerAddress,
      final String recognizedProxySecret) throws Exception;
  protected abstract void stop() throws Exception;
  protected abstract NoiseTunnelClient.Builder clientBuilder(final NioEventLoopGroup eventLoopGroup, final ECPublicKey serverPublicKey);

  public void assertClosedWith(final NoiseTunnelClient client, final CloseFrameEvent.CloseReason reason)
      throws ExecutionException, InterruptedException, TimeoutException {
    final CloseFrameEvent result = client.closeFrameFuture().get(1, TimeUnit.SECONDS);
    assertEquals(reason, result.closeReason());
  }

  @AfterEach
  void tearDown() throws Exception {
    authenticatedGrpcServer.stop();
    anonymousGrpcServer.stop();
    this.stop();
  }

  @AfterAll
  static void tearDownAfterAll() throws InterruptedException {
    nioEventLoopGroup.shutdownGracefully(100, 100, TimeUnit.MILLISECONDS).await();
    defaultEventLoopGroup.shutdownGracefully(100, 100, TimeUnit.MILLISECONDS).await();

    delegatedTaskExecutor.shutdown();
    //noinspection ResultOfMethodCallIgnored
    delegatedTaskExecutor.awaitTermination(1, TimeUnit.SECONDS);

    serverCallExecutor.shutdown();
    //noinspection ResultOfMethodCallIgnored
    serverCallExecutor.awaitTermination(1, TimeUnit.SECONDS);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void connectAuthenticated(final boolean includeProxyMessage) throws InterruptedException {
    try (final NoiseTunnelClient client = authenticated()
        .setProxyMessageSupplier(proxyMessageSupplier(includeProxyMessage))
        .build()) {
      final ManagedChannel channel = buildManagedChannel(client.getLocalAddress());

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
  void connectAuthenticatedBadServerKeySignature() throws InterruptedException, ExecutionException, TimeoutException {

    // Try to verify the server's public key with something other than the key with which it was signed
    try (final NoiseTunnelClient client = authenticated()
        .setServerPublicKey(Curve.generateKeyPair().getPublicKey())
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
  void connectAuthenticatedMismatchedClientPublicKey() throws InterruptedException, ExecutionException, TimeoutException {

    when(clientPublicKeysManager.findPublicKey(ACCOUNT_IDENTIFIER, DEVICE_ID))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(Curve.generateKeyPair().getPublicKey())));

    try (final NoiseTunnelClient client = authenticated().build()) {
      final ManagedChannel channel = buildManagedChannel(client.getLocalAddress());

      try {
        //noinspection ResultOfMethodCallIgnored
        GrpcTestUtils.assertStatusException(Status.UNAVAILABLE,
            () -> RequestAttributesGrpc.newBlockingStub(channel)
                .getRequestAttributes(GetRequestAttributesRequest.newBuilder().build()));
      } finally {
        channel.shutdown();
      }
      assertEquals(
          NoiseTunnelProtos.HandshakeResponse.Code.WRONG_PUBLIC_KEY,
          client.getHandshakeEventFuture().get(1, TimeUnit.SECONDS).handshakeResponse().getCode());
      assertClosedWith(client, CloseFrameEvent.CloseReason.NOISE_HANDSHAKE_ERROR);
    }
  }

  @Test
  void connectAuthenticatedUnrecognizedDevice() throws InterruptedException, ExecutionException, TimeoutException {
    when(clientPublicKeysManager.findPublicKey(ACCOUNT_IDENTIFIER, DEVICE_ID))
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    try (final NoiseTunnelClient client = authenticated().build()) {

      final ManagedChannel channel = buildManagedChannel(client.getLocalAddress());

      try {
        //noinspection ResultOfMethodCallIgnored
        GrpcTestUtils.assertStatusException(Status.UNAVAILABLE,
            () -> RequestAttributesGrpc.newBlockingStub(channel)
                .getRequestAttributes(GetRequestAttributesRequest.newBuilder().build()));
      } finally {
        channel.shutdown();
      }
      assertEquals(
          NoiseTunnelProtos.HandshakeResponse.Code.WRONG_PUBLIC_KEY,
          client.getHandshakeEventFuture().get(1, TimeUnit.SECONDS).handshakeResponse().getCode());
      assertClosedWith(client, CloseFrameEvent.CloseReason.NOISE_HANDSHAKE_ERROR);
    }

  }

  @Test
  void clientNormalClosure() throws InterruptedException {
    final NoiseTunnelClient client = anonymous().build();
    final ManagedChannel channel = buildManagedChannel(client.getLocalAddress());
    try {
      final GetAuthenticatedDeviceResponse response = RequestAttributesGrpc.newBlockingStub(channel)
          .getAuthenticatedDevice(GetAuthenticatedDeviceRequest.newBuilder().build());

      assertTrue(response.getAccountIdentifier().isEmpty());
      assertEquals(0, response.getDeviceId());
      client.close();

      // When we gracefully close the tunnel client, we should send an OK close frame
      final CloseFrameEvent closeFrame = client.closeFrameFuture().join();
      assertEquals(CloseFrameEvent.CloseInitiator.CLIENT, closeFrame.closeInitiator());
      assertEquals(CloseFrameEvent.CloseReason.OK, closeFrame.closeReason());
    } finally {
      channel.shutdown();
    }
  }

  @Test
  void connectAnonymous() throws InterruptedException {
    try (final NoiseTunnelClient client = anonymous().build()) {
      final ManagedChannel channel = buildManagedChannel(client.getLocalAddress());

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
  void connectAnonymousBadServerKeySignature() throws InterruptedException, ExecutionException, TimeoutException {

    // Try to verify the server's public key with something other than the key with which it was signed
    try (final NoiseTunnelClient client = anonymous()
        .setServerPublicKey(Curve.generateKeyPair().getPublicKey())
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

  protected ManagedChannel buildManagedChannel(final LocalAddress localAddress) {
    return NettyChannelBuilder.forAddress(localAddress)
        .channelType(LocalChannel.class)
        .eventLoopGroup(defaultEventLoopGroup)
        .usePlaintext()
        .build();
  }


  @Test
  void closeForReauthentication() throws InterruptedException, ExecutionException, TimeoutException {

    try (final NoiseTunnelClient client = authenticated().build()) {

      final ManagedChannel channel = buildManagedChannel(client.getLocalAddress());

      try {
        final GetAuthenticatedDeviceResponse response = RequestAttributesGrpc.newBlockingStub(channel)
            .getAuthenticatedDevice(GetAuthenticatedDeviceRequest.newBuilder().build());

        assertEquals(UUIDUtil.toByteString(ACCOUNT_IDENTIFIER), response.getAccountIdentifier());
        assertEquals(DEVICE_ID, response.getDeviceId());

        grpcClientConnectionManager.closeConnection(new AuthenticatedDevice(ACCOUNT_IDENTIFIER, DEVICE_ID));
        final CloseFrameEvent closeEvent = client.closeFrameFuture().get(2, TimeUnit.SECONDS);
        assertEquals(CloseFrameEvent.CloseReason.SERVER_CLOSED, closeEvent.closeReason());
        assertEquals(CloseFrameEvent.CloseInitiator.SERVER, closeEvent.closeInitiator());
      } finally {
        channel.shutdown();
      }
    }
  }

  @Test
  void waitForCallCompletion() throws InterruptedException, ExecutionException, TimeoutException {
    try (final NoiseTunnelClient client = authenticated().build()) {

      final ManagedChannel channel = buildManagedChannel(client.getLocalAddress());

      try {
        final GetAuthenticatedDeviceResponse response = RequestAttributesGrpc.newBlockingStub(channel)
            .getAuthenticatedDevice(GetAuthenticatedDeviceRequest.newBuilder().build());

        assertEquals(UUIDUtil.toByteString(ACCOUNT_IDENTIFIER), response.getAccountIdentifier());
        assertEquals(DEVICE_ID, response.getDeviceId());

        final CountDownLatch responseCountDownLatch = new CountDownLatch(1);

        // Start an open-ended server call and leave it in a non-complete state
        final StreamObserver<EchoRequest> echoRequestStreamObserver = EchoServiceGrpc.newStub(channel).echoStream(
            new StreamObserver<>() {
              @Override
              public void onNext(final EchoResponse echoResponse) {
                responseCountDownLatch.countDown();
              }

              @Override
              public void onError(final Throwable throwable) {
              }

              @Override
              public void onCompleted() {
              }
            });

        // Requests are transmitted asynchronously; it's possible that we'll issue the "close connection" request before
        // the request even starts. Make sure we've done at least one request/response pair to ensure that the call has
        // truly started before requesting connection closure.
        echoRequestStreamObserver.onNext(EchoRequest.newBuilder().setPayload(ByteString.copyFromUtf8("Test")).build());
        assertTrue(responseCountDownLatch.await(1, TimeUnit.SECONDS));

        grpcClientConnectionManager.closeConnection(new AuthenticatedDevice(ACCOUNT_IDENTIFIER, DEVICE_ID));
        try {
          client.closeFrameFuture().get(100, TimeUnit.MILLISECONDS);
          fail("Channel should not close until active requests have finished");
        } catch (TimeoutException e) {
        }

        //noinspection ResultOfMethodCallIgnored
        GrpcTestUtils.assertStatusException(Status.UNAVAILABLE, () -> EchoServiceGrpc.newBlockingStub(channel)
            .echo(EchoRequest.newBuilder().setPayload(ByteString.copyFromUtf8("Test")).build()));

        // Complete the open-ended server call
        echoRequestStreamObserver.onCompleted();

        final CloseFrameEvent closeFrameEvent = client.closeFrameFuture().get(1, TimeUnit.SECONDS);
        assertEquals(CloseFrameEvent.CloseInitiator.SERVER, closeFrameEvent.closeInitiator());
        assertEquals(CloseFrameEvent.CloseReason.SERVER_CLOSED, closeFrameEvent.closeReason());
      } finally {
        channel.shutdown();
      }
    }
  }

  protected NoiseTunnelClient.Builder anonymous() {
    return clientBuilder(nioEventLoopGroup, serverKeyPair.getPublicKey());
  }

  protected NoiseTunnelClient.Builder authenticated() {
    return clientBuilder(nioEventLoopGroup, serverKeyPair.getPublicKey())
        .setAuthenticated(clientKeyPair, ACCOUNT_IDENTIFIER, DEVICE_ID);
  }

  private static Supplier<HAProxyMessage> proxyMessageSupplier(boolean includeProxyMesage) {
    return includeProxyMesage
        ? () -> new HAProxyMessage(HAProxyProtocolVersion.V2, HAProxyCommand.PROXY, HAProxyProxiedProtocol.TCP4,
        "10.0.0.1", "10.0.0.2", 12345, 443)
        : null;
  }
}
