package org.whispersystems.textsecuregcm.grpc.net;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.DefaultByteBufHolder;
import java.net.InetAddress;

/**
 * A message that includes the initiator's handshake message, connection metadata, and the handshake type. The metadata
 * and handshake type are extracted from the framing layer, so this allows receivers to be framing layer agnostic.
 */
public class NoiseHandshakeInit extends DefaultByteBufHolder {

  private final InetAddress remoteAddress;
  private final HandshakePattern handshakePattern;

  public NoiseHandshakeInit(
      final InetAddress remoteAddress,
      final HandshakePattern handshakePattern,
      final ByteBuf initiatorHandshakeMessage) {
    super(initiatorHandshakeMessage);
    this.remoteAddress = remoteAddress;
    this.handshakePattern = handshakePattern;
  }

  public InetAddress getRemoteAddress() {
    return remoteAddress;
  }

  public HandshakePattern getHandshakePattern() {
    return handshakePattern;
  }

}
