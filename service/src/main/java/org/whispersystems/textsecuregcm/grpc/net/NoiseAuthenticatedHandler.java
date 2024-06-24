package org.whispersystems.textsecuregcm.grpc.net;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.ReferenceCountUtil;
import java.security.MessageDigest;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.storage.ClientPublicKeysManager;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;

/**
 * A NoiseAuthenticatedHandler is a netty pipeline element that handles the responder side of an authenticated handshake
 * and noise encryption/decryption. Authenticated handshakes are noise IK handshakes where the initiator's static public
 * key is authenticated by the responder.
 * <p>
 * The authenticated handshake requires the initiator to provide a payload with their first handshake message that
 * includes their account identifier and device id in network byte-order. Optionally, the initiator can also include an
 * initial request in their payload. If provided, this allows the server to begin processing the request without an
 * initial message delay (fast open).
 * <pre>
 * +-----------------+----------------+------------------------+
 * |    UUID (16)    |  deviceId (1)  |     request bytes (N)  |
 * +-----------------+----------------+------------------------+
 * </pre>
 * <p>
 * For a successful handshake, the static key provided in the handshake message must match the server's stored public
 * key for the device identified by the provided ACI and deviceId.
 * <p>
 * As soon as the handler authenticates the caller, it will fire a {@link NoiseIdentityDeterminedEvent}.
 */
class NoiseAuthenticatedHandler extends NoiseHandler {

  private final ClientPublicKeysManager clientPublicKeysManager;

  NoiseAuthenticatedHandler(final ClientPublicKeysManager clientPublicKeysManager,
      final ECKeyPair ecKeyPair) {
    super(new NoiseHandshakeHelper(HandshakePattern.IK, ecKeyPair));
    this.clientPublicKeysManager = clientPublicKeysManager;
  }

  @Override
  CompletableFuture<HandshakeResult> handleHandshakePayload(
      final ChannelHandlerContext context,
      final Optional<byte[]> initiatorPublicKey,
      final ByteBuf handshakePayload) throws NoiseHandshakeException {
    if (handshakePayload.readableBytes() < 17) {
      throw new NoiseHandshakeException("Invalid handshake payload");
    }

    final byte[] publicKeyFromClient = initiatorPublicKey
        .orElseThrow(() -> new IllegalStateException("No remote public key"));

    // Advances the read index by 16 bytes
    final UUID accountIdentifier = parseUUID(handshakePayload);

    // Advances the read index by 1 byte
    final byte deviceId = handshakePayload.readByte();

    final ByteBuf fastOpenRequest = handshakePayload.slice();
    return clientPublicKeysManager
        .findPublicKey(accountIdentifier, deviceId)
        .handleAsync((storedPublicKey, throwable) -> {
          if (throwable != null) {
            ReferenceCountUtil.release(fastOpenRequest);
            throw ExceptionUtils.wrap(throwable);
          }
          final boolean valid = storedPublicKey
              .map(spk -> MessageDigest.isEqual(publicKeyFromClient, spk.getPublicKeyBytes()))
              .orElse(false);
          if (!valid) {
            throw ExceptionUtils.wrap(new ClientAuthenticationException());
          }
          return new HandshakeResult(
              fastOpenRequest,
              Optional.of(new AuthenticatedDevice(accountIdentifier, deviceId)));
        }, context.executor());
  }

  /**
   * Parse a {@link UUID} out of bytes, advancing the readerIdx by 16
   *
   * @param bytes The {@link ByteBuf} to read from
   * @return The parsed UUID
   * @throws NoiseHandshakeException If a UUID could not be parsed from bytes
   */
  private UUID parseUUID(final ByteBuf bytes) throws NoiseHandshakeException {
    if (bytes.readableBytes() < 16) {
      throw new NoiseHandshakeException("Could not parse account identifier");
    }
    return new UUID(bytes.readLong(), bytes.readLong());
  }
}
