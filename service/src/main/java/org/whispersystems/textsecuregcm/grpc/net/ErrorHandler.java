package org.whispersystems.textsecuregcm.grpc.net;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import javax.crypto.BadPaddingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;

/**
 * An error handler serves as a general backstop for exceptions elsewhere in the pipeline. It translates exceptions
 * thrown in inbound handlers into {@link OutboundCloseErrorMessage}s.
 */
public class ErrorHandler extends ChannelInboundHandlerAdapter {
  private static final Logger log = LoggerFactory.getLogger(ErrorHandler.class);

  private static OutboundCloseErrorMessage NOISE_ENCRYPTION_ERROR_CLOSE = new OutboundCloseErrorMessage(
      OutboundCloseErrorMessage.Code.NOISE_ERROR,
      "Noise encryption error");

  @Override
  public void exceptionCaught(final ChannelHandlerContext context, final Throwable cause) {
      final OutboundCloseErrorMessage closeMessage = switch (ExceptionUtils.unwrap(cause)) {
        case NoiseHandshakeException e -> new OutboundCloseErrorMessage(
            OutboundCloseErrorMessage.Code.NOISE_HANDSHAKE_ERROR,
            e.getMessage());
        case BadPaddingException ignored -> NOISE_ENCRYPTION_ERROR_CLOSE;
        case NoiseException ignored -> NOISE_ENCRYPTION_ERROR_CLOSE;
        default -> {
          log.warn("An unexpected exception reached the end of the pipeline", cause);
          yield new OutboundCloseErrorMessage(
              OutboundCloseErrorMessage.Code.INTERNAL_SERVER_ERROR,
              cause.getMessage());
        }
      };

      context.writeAndFlush(closeMessage)
          .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
  }
}
