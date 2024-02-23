package org.whispersystems.textsecuregcm.grpc.net;

import com.southernstorm.noise.protocol.HandshakeState;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.util.internal.EmptyArrays;
import java.security.NoSuchAlgorithmException;
import javax.crypto.BadPaddingException;
import javax.crypto.ShortBufferException;
import org.signal.libsignal.protocol.ecc.ECKeyPair;

/**
 * An abstract base class for XX- and NX-patterned Noise responder handshake handlers.
 *
 * @see <a href="https://noiseprotocol.org/noise.html">The Noise Protocol Framework</a>
 */
abstract class AbstractNoiseHandshakeHandler extends ChannelInboundHandlerAdapter {

  private final ECKeyPair ecKeyPair;
  private final byte[] publicKeySignature;

  private final HandshakeState handshakeState;

  private static final int EXPECTED_EPHEMERAL_KEY_MESSAGE_LENGTH = 32;

  /**
   * Constructs a new Noise handler with the given static server keys and static public key signature. The static public
   * key must be signed by a trusted root private key whose public key is known to and trusted by authenticating
   * clients.
   *
   * @param noiseProtocolName the name of the Noise protocol implemented by this handshake handler
   * @param ecKeyPair the static key pair for this server
   * @param publicKeySignature an Ed25519 signature of the raw bytes of the static public key
   */
  AbstractNoiseHandshakeHandler(final String noiseProtocolName,
      final ECKeyPair ecKeyPair,
      final byte[] publicKeySignature) {

    this.ecKeyPair = ecKeyPair;
    this.publicKeySignature = publicKeySignature;

    try {
      this.handshakeState = new HandshakeState(noiseProtocolName, HandshakeState.RESPONDER);
    } catch (final NoSuchAlgorithmException e) {
      throw new AssertionError("Unsupported Noise algorithm: " + noiseProtocolName, e);
    }
  }

  protected HandshakeState getHandshakeState() {
    return handshakeState;
  }

  /**
   * Handles an initial ephemeral key message from a client, advancing the handshake state and sending the server's
   * static keys to the client. Both XX and NX patterns begin with a client sending its ephemeral key to the server.
   * Clients must not include an additional payload with their ephemeral key message. The server's reply contains its
   * static keys along with an Ed25519 signature of its public static key by a trusted root key.
   *
   * @param context the channel handler context for this message
   * @param frame the websocket frame containing the ephemeral key message
   *
   * @throws NoiseHandshakeException if the ephemeral key message from the client was not of the expected size or if a
   * general Noise encryption error occurred
   */
  protected void handleEphemeralKeyMessage(final ChannelHandlerContext context, final BinaryWebSocketFrame frame)
      throws NoiseHandshakeException {

    if (frame.content().readableBytes() != EXPECTED_EPHEMERAL_KEY_MESSAGE_LENGTH) {
      throw new NoiseHandshakeException("Unexpected ephemeral key message length");
    }

    // Cryptographically initializing a handshake is expensive, and so we defer it until we're confident the client is
    // making a good-faith effort to perform a handshake (i.e. now). Noise-java in particular will derive a public key
    // from the supplied private key (and will in fact overwrite any previously-set public key when setting a private
    // key), so we just set the private key here.
    handshakeState.getLocalKeyPair().setPrivateKey(ecKeyPair.getPrivateKey().serialize(), 0);
    handshakeState.start();

    // The initial message from the client should just include a plaintext ephemeral key with no payload. The frame is
    // coming off the wire and so will be in a direct buffer that doesn't have a backing array.
    final byte[] ephemeralKeyMessage = ByteBufUtil.getBytes(frame.content());
    frame.content().readBytes(ephemeralKeyMessage);

    try {
      handshakeState.readMessage(ephemeralKeyMessage, 0, ephemeralKeyMessage.length, EmptyArrays.EMPTY_BYTES, 0);
    } catch (final ShortBufferException e) {
      // This should never happen since we're checking the length of the frame up front
      throw new NoiseHandshakeException("Unexpected client payload");
    } catch (final BadPaddingException e) {
      // It turns out this should basically never happen because (a) we're not using padding and (b) the "bad AEAD tag"
      // subclass of a bad padding exception can only happen if we have some AD to check, which we don't for an
      // ephemeral-key-only message
      throw new NoiseHandshakeException("Invalid keys");
    }

    // Send our key material and public key signature back to the client; this buffer will include:
    //
    // - A 32-byte plaintext ephemeral key
    // - A 32-byte encrypted static key
    // - A 16-byte AEAD tag for the static key
    // - The public key signature payload
    // - A 16-byte AEAD tag for the payload
    final byte[] keyMaterial = new byte[32 + 32 + 16 + publicKeySignature.length + 16];

    try {
      handshakeState.writeMessage(keyMaterial, 0, publicKeySignature, 0, publicKeySignature.length);

      context.writeAndFlush(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(keyMaterial)))
          .addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
    } catch (final ShortBufferException e) {
      // This should never happen for messages of known length that we control
      throw new AssertionError("Key material buffer was too short for message", e);
    }
  }

  @Override
  public void handlerRemoved(final ChannelHandlerContext context) {
    handshakeState.destroy();
  }
}
