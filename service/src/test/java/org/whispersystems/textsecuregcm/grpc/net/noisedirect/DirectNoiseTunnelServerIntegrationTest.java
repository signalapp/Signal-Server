package org.whispersystems.textsecuregcm.grpc.net.noisedirect;

import io.netty.channel.local.LocalAddress;
import io.netty.channel.nio.NioEventLoopGroup;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.signal.libsignal.protocol.ecc.ECPublicKey;
import org.whispersystems.textsecuregcm.grpc.net.FramingType;
import org.whispersystems.textsecuregcm.grpc.net.GrpcClientConnectionManager;
import org.whispersystems.textsecuregcm.grpc.net.client.NoiseTunnelClient;
import org.whispersystems.textsecuregcm.grpc.net.AbstractNoiseTunnelServerIntegrationTest;
import org.whispersystems.textsecuregcm.storage.ClientPublicKeysManager;

import java.util.concurrent.Executor;

class DirectNoiseTunnelServerIntegrationTest extends AbstractNoiseTunnelServerIntegrationTest {
  private NoiseDirectTunnelServer noiseDirectTunnelServer;

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

    noiseDirectTunnelServer = new NoiseDirectTunnelServer(0,
        eventLoopGroup,
        grpcClientConnectionManager,
        clientPublicKeysManager,
        serverKeyPair,
        authenticatedGrpcServerAddress,
        anonymousGrpcServerAddress);
    noiseDirectTunnelServer.start();
  }

  @Override
  protected void stop() throws InterruptedException {
    noiseDirectTunnelServer.stop();
  }

  @Override
  protected NoiseTunnelClient.Builder clientBuilder(final NioEventLoopGroup eventLoopGroup, final ECPublicKey serverPublicKey) {
    return new NoiseTunnelClient
        .Builder(noiseDirectTunnelServer.getLocalAddress(), eventLoopGroup, serverPublicKey)
        .setFramingType(FramingType.NOISE_DIRECT);
  }
}
