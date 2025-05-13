package org.whispersystems.textsecuregcm.grpc.net.websocket;

import io.netty.channel.local.LocalAddress;
import io.netty.channel.nio.NioEventLoopGroup;
import java.util.concurrent.Executor;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.signal.libsignal.protocol.ecc.ECPublicKey;
import org.whispersystems.textsecuregcm.grpc.net.AbstractNoiseTunnelServerIntegrationTest;
import org.whispersystems.textsecuregcm.grpc.net.GrpcClientConnectionManager;
import org.whispersystems.textsecuregcm.grpc.net.client.NoiseTunnelClient;
import org.whispersystems.textsecuregcm.storage.ClientPublicKeysManager;

class WebSocketNoiseTunnelServerIntegrationTest extends AbstractNoiseTunnelServerIntegrationTest {
  private NoiseWebSocketTunnelServer plaintextNoiseWebSocketTunnelServer;

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
    plaintextNoiseWebSocketTunnelServer = new NoiseWebSocketTunnelServer(0,
        null,
        null,
        eventLoopGroup,
        delegatedTaskExecutor,
        grpcClientConnectionManager,
        clientPublicKeysManager,
        serverKeyPair,
        authenticatedGrpcServerAddress,
        anonymousGrpcServerAddress,
        recognizedProxySecret);
    plaintextNoiseWebSocketTunnelServer.start();
  }

  @Override
  protected void stop() throws InterruptedException {
    plaintextNoiseWebSocketTunnelServer.stop();
  }

  @Override
  protected NoiseTunnelClient.Builder clientBuilder(final NioEventLoopGroup eventLoopGroup, final ECPublicKey serverPublicKey) {
    return new NoiseTunnelClient
        .Builder(plaintextNoiseWebSocketTunnelServer.getLocalAddress(), eventLoopGroup, serverPublicKey);
  }
}
