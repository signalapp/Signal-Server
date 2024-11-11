/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.util;

import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.ext.WriterInterceptor;
import jakarta.ws.rs.ext.WriterInterceptorContext;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import org.glassfish.jersey.message.internal.CommittingOutputStream;


/**
 * This is an elaborate workaround to avoid doing blocking operations under synchronized blocks, which is currently a
 * suboptimal case for virtual threads.
 * <p>
 * Jersey's {@link CommittingOutputStream} has two modes: direct write and buffered writes. In buffered mode, if the
 * total amount written does not exceed the output stream's buffer size, CommittingOutputStream will compute the
 * content-length for us. However, when it passes through our write to its own underlying output stream it uses
 * {@link ByteArrayOutputStream#writeTo(OutputStream)} which performs the write under a synchronized block.
 * <p>
 * If we just disable buffering, we lose our content length. However, we can't really set content-length ourselves
 * without the same access to internal state that CommittingOutputStream has. Fortunately, the underlying OutputStream
 * wrapped by CommittingOutputStream ALSO has an internal buffer, and can compute the content-length from that if the
 * content fits. But to make use of that, we need to avoid flushing that output stream until calling close, so that the
 * underlying output stream can see that it has all the data. Unfortunately the runtime inserts manual flushes after
 * writes rather than letting the underlying output stream handle it.
 * <p>
 * So here we disable buffering on CommittingOutputStream, and buffer ourselves. We don't write anything to the
 * CommittingOutputStream until we are going to close, and we do nothing on flush.
 */
public class BufferingInterceptor implements WriterInterceptor {

  @Override
  public void aroundWriteTo(final WriterInterceptorContext ctx) throws IOException, WebApplicationException {
    final OutputStream orig = ctx.getOutputStream();
    if (Thread.currentThread().isVirtual() && orig instanceof CommittingOutputStream cos) {
      cos.enableBuffering(0);
      ctx.setOutputStream(new BufferingOutputStream(cos));
    }
    ctx.proceed();
  }

  private static class BufferingOutputStream extends ByteArrayOutputStream {

    private final CommittingOutputStream original;

    BufferingOutputStream(final CommittingOutputStream original) {
      this.original = original;
    }

    @Override
    public void close() throws IOException {
      original.write(buf, 0, count);
      original.close();
      super.close();
    }
  }
}
