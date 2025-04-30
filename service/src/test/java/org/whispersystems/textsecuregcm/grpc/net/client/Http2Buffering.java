/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.ReferenceCountUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.stream.Stream;

/**
 * The noise tunnel streams bytes out of a gRPC client through noise and to a remote server. The server supports a "fast
 * open" optimization where the client can send a request along with the noise handshake. There's no direct way to
 * extract the request boundaries from the gRPC client's byte-stream, so {@link Http2Buffering#handler()} provides an
 * inbound pipeline handler that will parse the byte-stream back into HTTP/2 frames and buffer the first request.
 * <p>
 * Once an entire request has been buffered, the handler will remove itself from the pipeline and emit a
 * {@link FastOpenRequestBufferedEvent}
 */
public class Http2Buffering {

  /**
   * Create a pipeline handler that consumes serialized HTTP/2 ByteBufs and emits a fast-open request
   */
  public static ChannelInboundHandler handler() {
    return new Http2PrefaceHandler();
  }

  private Http2Buffering() {
  }

  private static class Http2PrefaceHandler extends ChannelInboundHandlerAdapter {

    // https://www.rfc-editor.org/rfc/rfc7540.html#section-3.5
    private static final byte[] HTTP2_PREFACE =
        HexFormat.of().parseHex("505249202a20485454502f322e300d0a0d0a534d0d0a0d0a");
    private final ByteBuf read = Unpooled.buffer(HTTP2_PREFACE.length, HTTP2_PREFACE.length);

    @Override
    public void channelRead(final ChannelHandlerContext context, final Object message) {
      if (message instanceof ByteBuf bb) {
        bb.readBytes(read);
        if (read.readableBytes() < HTTP2_PREFACE.length) {
          // Copied the message into the read buffer, but haven't yet got a full HTTP2 preface. Wait for more.
          return;
        }
        if (!Arrays.equals(read.array(), HTTP2_PREFACE)) {
          throw new IllegalStateException("HTTP/2 stream must start with HTTP/2 preface");
        }
        context.pipeline().replace(this, "http2frame1", new Http2LengthFieldFrameDecoder());
        context.pipeline().addAfter("http2frame1", "http2frame2", new Http2FrameDecoder());
        context.pipeline().addAfter("http2frame2", "http2frame3", new Http2FirstRequestHandler());
        context.fireChannelRead(bb);
      } else {
        throw new IllegalStateException("Unexpected message: " + message);
      }
    }

    @Override
    public void handlerRemoved(final ChannelHandlerContext context) {
      ReferenceCountUtil.release(read);
    }
  }


  private record Http2Frame(ByteBuf bytes, FrameType type, boolean endStream) {

    private static final byte FLAG_END_STREAM = 0x01;

    enum FrameType {
      SETTINGS,
      HEADERS,
      DATA,
      WINDOW_UPDATE,
      OTHER;

      static FrameType fromSerializedType(final byte type) {
        return switch (type) {
          case 0x00 -> Http2Frame.FrameType.DATA;
          case 0x01 -> Http2Frame.FrameType.HEADERS;
          case 0x04 -> Http2Frame.FrameType.SETTINGS;
          case 0x08 -> Http2Frame.FrameType.WINDOW_UPDATE;
          default -> Http2Frame.FrameType.OTHER;
        };
      }
    }
  }

  /**
   * Emit ByteBuf of entire HTTP/2 frame
   */
  private static class Http2LengthFieldFrameDecoder extends LengthFieldBasedFrameDecoder {

    public Http2LengthFieldFrameDecoder() {
      // Frames are 3 bytes of length, 6  bytes of other header, and then length bytes of payload
      super(16 * 1024 * 1024, 0, 3, 6, 0);
    }
  }

  /**
   * Parse the serialized Http/2 frames into {@link Http2Frame} objects
   */
  private static class Http2FrameDecoder extends ByteToMessageDecoder {

    @Override
    protected void decode(final ChannelHandlerContext ctx, final ByteBuf in, final List<Object> out) throws Exception {
      // https://www.rfc-editor.org/rfc/rfc7540.html#section-4.1
      final Http2Frame.FrameType frameType = Http2Frame.FrameType.fromSerializedType(in.getByte(in.readerIndex() + 3));
      final boolean endStream = endStream(frameType, in.getByte(in.readerIndex() + 4));
      out.add(new Http2Frame(in.readBytes(in.readableBytes()), frameType, endStream));
    }

    boolean endStream(Http2Frame.FrameType frameType, byte flags) {
      // A gRPC request are packed into HTTP/2 frames like:
      // HEADERS frame | DATA frame 1 (endStream=0) | ... | DATA frame N (endstream=1)
      //
      // Our goal is to get an entire request buffered, so as soon as we see a DATA frame with the end stream flag set
      // we have a whole request. Note that we could have pieces of multiple requests, but the only thing we care about
      // is having at least one complete request. In total, we can expect something like:
      // HTTP-preface | SETTINGS frame | Frames we don't care about ... | DATA (endstream=1)
      //
      // The connection isn't 'established' until the server has responded with their own SETTINGS frame with the ack
      // bit set, but HTTP/2 allows the client to send frames before getting the ACK.
      if (frameType == Http2Frame.FrameType.DATA) {
        return (flags & Http2Frame.FLAG_END_STREAM) == Http2Frame.FLAG_END_STREAM;
      }

      // In theory, at least. Unfortunately, the java gRPC client always waits for the HTTP/2 handshake to complete
      // (which requires the server sending back the ack) before it actually sends any requests. So if we waited for a
      // DATA frame, it would never come. The gRPC-java implementation always at least sends a WINDOW_UPDATE, so we
      // might as well pack that in.
      return frameType == Http2Frame.FrameType.WINDOW_UPDATE;
    }
  }

  /**
   * Collect HTTP/2 frames until we get an entire "request" to send
   */
  private static class Http2FirstRequestHandler extends ChannelInboundHandlerAdapter {

    final List<Http2Frame> pendingFrames = new ArrayList<>();

    @Override
    public void channelRead(final ChannelHandlerContext context, final Object message) {
      if (message instanceof Http2Frame http2Frame) {
        if (pendingFrames.isEmpty() && http2Frame.type != Http2Frame.FrameType.SETTINGS) {
          throw new IllegalStateException(
              "HTTP/2 stream must start with HTTP/2 SETTINGS frame, got " + http2Frame.type);
        }
        pendingFrames.add(http2Frame);
        if (http2Frame.endStream) {
          // We have a whole "request", emit the first request event and remove the http2 buffering handlers
          final ByteBuf request = Unpooled.wrappedBuffer(Stream.concat(
                  Stream.of(Unpooled.wrappedBuffer(Http2PrefaceHandler.HTTP2_PREFACE)),
                  pendingFrames.stream().map(Http2Frame::bytes))
              .toArray(ByteBuf[]::new));
          pendingFrames.clear();
          context.pipeline().remove(Http2LengthFieldFrameDecoder.class);
          context.pipeline().remove(Http2FrameDecoder.class);
          context.pipeline().remove(this);
          context.fireUserEventTriggered(new FastOpenRequestBufferedEvent(request));
        }
      } else {
        throw new IllegalStateException("Unexpected message: " + message);
      }
    }

    @Override
    public void handlerRemoved(final ChannelHandlerContext context) {
      pendingFrames.forEach(frame -> ReferenceCountUtil.release(frame.bytes()));
      pendingFrames.clear();
    }
  }
}
