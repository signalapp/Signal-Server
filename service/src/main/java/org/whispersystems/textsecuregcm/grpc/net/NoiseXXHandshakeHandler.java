package org.whispersystems.textsecuregcm.grpc.net;

import com.southernstorm.noise.protocol.HandshakeState;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.util.ReferenceCountUtil;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.crypto.BadPaddingException;
import javax.crypto.ShortBufferException;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.storage.ClientPublicKeysManager;
import org.whispersystems.textsecuregcm.util.UUIDUtil;

/**
 * A Noise XX handler handles the responder side of a Noise XX handshake. This implementation expects clients to send
 * identifying information (an account identifier and device ID) as an additional payload when sending its static key
 * material. It compares the static public key against the stored public key for the identified device asynchronously,
 * buffering traffic from the client until the authentication check completes.
 */
class NoiseXXHandshakeHandler extends AbstractNoiseHandshakeHandler {

  private final ClientPublicKeysManager clientPublicKeysManager;

  private AuthenticationState authenticationState = AuthenticationState.GET_EPHEMERAL_KEY;

  private final List<BinaryWebSocketFrame> pendingInboundFrames = new ArrayList<>();

  static final String NOISE_PROTOCOL_NAME = "Noise_XX_25519_ChaChaPoly_BLAKE2b";

  // When the client sends its static key message, we expect:
  //
  // - A 32-byte encrypted static public key
  // - A 16-byte AEAD tag for the static key
  // - 17 bytes of identity data in the message payload (a UUID and a one-byte device ID)
  // - A 16-byte AEAD tag for the identity payload
  private static final int EXPECTED_CLIENT_STATIC_KEY_MESSAGE_LENGTH = 81;

  private enum AuthenticationState {
    GET_EPHEMERAL_KEY,
    GET_STATIC_KEY,
    CHECK_PUBLIC_KEY,
    ERROR
  }

  public NoiseXXHandshakeHandler(final ClientPublicKeysManager clientPublicKeysManager,
      final ECKeyPair ecKeyPair,
      final byte[] publicKeySignature) {

    super(NOISE_PROTOCOL_NAME, ecKeyPair, publicKeySignature);

    this.clientPublicKeysManager = clientPublicKeysManager;
  }

  @Override
  public void channelRead(final ChannelHandlerContext context, final Object message) throws Exception {
    if (message instanceof BinaryWebSocketFrame frame) {
      try {
        switch (authenticationState) {
          case GET_EPHEMERAL_KEY -> {
            try {
              handleEphemeralKeyMessage(context, frame);
              authenticationState = AuthenticationState.GET_STATIC_KEY;
            } finally {
              frame.release();
            }
          }
          case GET_STATIC_KEY -> {
            try {
              handleStaticKey(context, frame);
              authenticationState = AuthenticationState.CHECK_PUBLIC_KEY;
            } finally {
              frame.release();
            }
          }
          case CHECK_PUBLIC_KEY -> {
            // Buffer any inbound traffic until we've finished checking the client's public key
            pendingInboundFrames.add(frame);
          }
          case ERROR -> {
            // If authentication has failed for any reason, just discard inbound traffic until the channel closes
            frame.release();
          }
        }
      } catch (final ShortBufferException e) {
        authenticationState = AuthenticationState.ERROR;
        throw new NoiseHandshakeException("Unexpected payload length");
      } catch (final BadPaddingException e) {
        authenticationState = AuthenticationState.ERROR;
        throw new ClientAuthenticationException();
      }
    } else {
      // Anything except binary WebSocket frames should have been filtered out of the pipeline by now; treat this as an
      // error
      ReferenceCountUtil.release(message);
      throw new IllegalArgumentException("Unexpected message in pipeline: " + message);
    }
  }

  private void handleStaticKey(final ChannelHandlerContext context, final BinaryWebSocketFrame frame)
      throws NoiseHandshakeException, ShortBufferException, BadPaddingException {

    if (frame.content().readableBytes() != EXPECTED_CLIENT_STATIC_KEY_MESSAGE_LENGTH) {
      throw new NoiseHandshakeException("Unexpected client static key message length");
    }

    final HandshakeState handshakeState = getHandshakeState();

    // The websocket frame will have come right off the wire, and so needs to be copied from a non-array-backed direct
    // buffer into a heap buffer.
    final byte[] staticKeyAndClientIdentityMessage = ByteBufUtil.getBytes(frame.content());

    // The payload from the client should be a UUID (16 bytes) followed by a device ID (1 byte)
    final byte[] payload = new byte[17];

    final UUID accountIdentifier;
    final byte deviceId;

    final int payloadBytesRead = handshakeState.readMessage(staticKeyAndClientIdentityMessage,
        0, staticKeyAndClientIdentityMessage.length, payload, 0);

    if (payloadBytesRead != 17) {
      throw new NoiseHandshakeException("Unexpected identity payload length");
    }

    try {
      accountIdentifier = UUIDUtil.fromBytes(payload, 0);
    } catch (final IllegalArgumentException e) {
      throw new NoiseHandshakeException("Could not parse account identifier");
    }

    deviceId = payload[16];

    // Verify the identity of the caller by comparing the submitted static public key against the stored public key for
    // the identified device
    clientPublicKeysManager.findPublicKey(accountIdentifier, deviceId)
        .whenCompleteAsync((maybePublicKey, throwable) -> maybePublicKey.ifPresentOrElse(storedPublicKey -> {
                  final byte[] publicKeyFromClient = new byte[handshakeState.getRemotePublicKey().getPublicKeyLength()];
                  handshakeState.getRemotePublicKey().getPublicKey(publicKeyFromClient, 0);

                  if (MessageDigest.isEqual(publicKeyFromClient, storedPublicKey.getPublicKeyBytes())) {
                    context.fireUserEventTriggered(new NoiseHandshakeCompleteEvent(
                        Optional.of(new AuthenticatedDevice(accountIdentifier, deviceId))));

                    context.pipeline().addAfter(context.name(), null, new NoiseStreamHandler(handshakeState.split()));

                    // Flush any buffered reads
                    pendingInboundFrames.forEach(context::fireChannelRead);
                    pendingInboundFrames.clear();

                    context.pipeline().remove(NoiseXXHandshakeHandler.this);
                  } else {
                    // We found a key, but it doesn't match what the caller submitted
                    context.fireExceptionCaught(new ClientAuthenticationException());
                    authenticationState = AuthenticationState.ERROR;
                  }
                },
                () -> {
                  // We couldn't find a key for the identified account/device
                  context.fireExceptionCaught(new ClientAuthenticationException());
                  authenticationState = AuthenticationState.ERROR;
                }),
            context.executor());
  }

  @Override
  public void handlerRemoved(final ChannelHandlerContext context) {
    super.handlerRemoved(context);

    pendingInboundFrames.forEach(BinaryWebSocketFrame::release);
    pendingInboundFrames.clear();
  }
}
