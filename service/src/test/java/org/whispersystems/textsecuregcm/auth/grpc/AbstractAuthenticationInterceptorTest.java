package org.whispersystems.textsecuregcm.auth.grpc;

import static org.mockito.Mockito.mock;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import java.io.IOException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.signal.chat.rpc.GetAuthenticatedDeviceRequest;
import org.signal.chat.rpc.GetAuthenticatedDeviceResponse;
import org.signal.chat.rpc.RequestAttributesGrpc;
import org.whispersystems.textsecuregcm.grpc.RequestAttributesServiceImpl;
import org.whispersystems.textsecuregcm.grpc.net.GrpcClientConnectionManager;

abstract class AbstractAuthenticationInterceptorTest {

  private static DefaultEventLoopGroup eventLoopGroup;

  private GrpcClientConnectionManager grpcClientConnectionManager;

  private Server server;
  private ManagedChannel managedChannel;

  @BeforeAll
  static void setUpBeforeAll() {
    eventLoopGroup = new DefaultEventLoopGroup();
  }

  @BeforeEach
  void setUp() throws IOException {
    final LocalAddress serverAddress = new LocalAddress("test-authentication-interceptor-server");

    grpcClientConnectionManager = mock(GrpcClientConnectionManager.class);

    // `RequestAttributesInterceptor` operates on `LocalAddresses`, so we need to do some slightly fancy plumbing to make
    // sure that we're using local channels and addresses
    server = NettyServerBuilder.forAddress(serverAddress)
        .channelType(LocalServerChannel.class)
        .bossEventLoopGroup(eventLoopGroup)
        .workerEventLoopGroup(eventLoopGroup)
        .intercept(getInterceptor())
        .addService(new RequestAttributesServiceImpl())
        .build()
        .start();

    managedChannel = NettyChannelBuilder.forAddress(serverAddress)
        .channelType(LocalChannel.class)
        .eventLoopGroup(eventLoopGroup)
        .usePlaintext()
        .build();
  }

  @AfterEach
  void tearDown() {
    managedChannel.shutdown();
    server.shutdown();
  }

  protected abstract AbstractAuthenticationInterceptor getInterceptor();

  protected GrpcClientConnectionManager getClientConnectionManager() {
    return grpcClientConnectionManager;
  }

  protected GetAuthenticatedDeviceResponse getAuthenticatedDevice() {
    return RequestAttributesGrpc.newBlockingStub(managedChannel)
        .getAuthenticatedDevice(GetAuthenticatedDeviceRequest.newBuilder().build());
  }
}
