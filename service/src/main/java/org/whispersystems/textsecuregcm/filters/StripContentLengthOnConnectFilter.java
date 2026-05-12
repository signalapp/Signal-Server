/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.filters;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.eclipse.jetty.ee10.servlet.ServletContextRequest;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.http.MetaData;
import org.eclipse.jetty.server.HttpStream;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.util.Callback;

/// Our current version of jetty (12.1.5) has a bug where it includes content-length:0 on
/// CONNECT websocket upgrade requests. Providing an HTTP/2 header frame with a
/// content-length that does not match the sum of the lengths of the data frames is technically
/// a malformed HTTP/2 stream and our netty-based reverse proxy implementation rejects it. This
/// filter strips out the superfluous content-length at stream-send time. It can be removed once
/// we update to a jetty version that fixes [jetty/jetty.project#15074](https://github.com/jetty/jetty.project/issues/15074)
public class StripContentLengthOnConnectFilter implements Filter {

  @Override
  public void doFilter(final ServletRequest request, final ServletResponse response, final FilterChain chain)
      throws IOException, ServletException {
    if (request instanceof HttpServletRequest hsr &&
        HttpVersion.HTTP_2.is(hsr.getProtocol()) &&
        HttpMethod.CONNECT.is(hsr.getMethod())) {
      final Request coreRequest = ServletContextRequest.getServletContextRequest(hsr);
      if (coreRequest != null) {
        coreRequest.addHttpStreamWrapper(StripContentLengthStream::new);
      }
    }
    chain.doFilter(request, response);
  }

  private static class StripContentLengthStream extends HttpStream.Wrapper {

    StripContentLengthStream(final HttpStream wrapped) {
      super(wrapped);
    }

    @Override
    public void send(MetaData.Request request, MetaData.Response response, boolean last, ByteBuffer content,
        Callback callback) {
      if (response != null && response.getStatus() == 200 && response.getHttpFields()
          .contains(HttpHeader.CONTENT_LENGTH)) {
        final HttpFields fieldsWithoutContentLengthHeader =
            HttpFields.build(response.getHttpFields()).remove(HttpHeader.CONTENT_LENGTH);
        response = new MetaData.Response(
            response.getStatus(),
            response.getReason(),
            response.getHttpVersion(),
            fieldsWithoutContentLengthHeader,
            -1,
            response.getTrailersSupplier());
      }
      super.send(request, response, last, content, callback);
    }
  }
}
