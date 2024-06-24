package org.whispersystems.textsecuregcm.grpc.net;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.signal.libsignal.protocol.ecc.ECKeyPair;

/**
 * A NoiseAnonymousHandler is a netty pipeline element that handles the responder side of an unauthenticated handshake
 * and noise encryption/decryption.
 * <p>
 * A noise NK handshake must be used for unauthenticated connections. Optionally, the initiator can also include an
 * initial request in their payload. If provided, this allows the server to begin processing the request without an
 * initial message delay (fast open).
 * <p>
 * Once the handler receives the handshake initiator message, it will fire a {@link NoiseIdentityDeterminedEvent}
 * indicating that initiator connected anonymously.
 */
class NoiseAnonymousHandler extends NoiseHandler {

  public NoiseAnonymousHandler(final ECKeyPair ecKeyPair) {
    super(new NoiseHandshakeHelper(HandshakePattern.NK, ecKeyPair));
  }

  @Override
  CompletableFuture<HandshakeResult> handleHandshakePayload(final ChannelHandlerContext context,
      final Optional<byte[]> initiatorPublicKey, final ByteBuf handshakePayload) {
    return CompletableFuture.completedFuture(new HandshakeResult(
        handshakePayload,
        Optional.empty()
    ));
  }
}
