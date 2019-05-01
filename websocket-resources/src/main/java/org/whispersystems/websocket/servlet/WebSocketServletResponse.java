/**
 * Copyright (C) 2014 Open WhisperSystems
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.whispersystems.websocket.servlet;

import org.eclipse.jetty.websocket.api.RemoteEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.websocket.messages.WebSocketMessageFactory;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Locale;
import java.util.Optional;


public class WebSocketServletResponse implements HttpServletResponse {

  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory.getLogger(WebSocketServletResponse.class);

  private final RemoteEndpoint          endPoint;
  private final long                    requestId;
  private final WebSocketMessageFactory messageFactory;

  private ResponseBuilder       responseBuilder     = new ResponseBuilder();
  private ByteArrayOutputStream responseBody        = new ByteArrayOutputStream();
  private ServletOutputStream   servletOutputStream = new BufferingServletOutputStream(responseBody);
  private boolean               isCommitted         = false;

  public WebSocketServletResponse(RemoteEndpoint endPoint, long requestId,
                                  WebSocketMessageFactory messageFactory)
  {
    this.endPoint       = endPoint;
    this.requestId      = requestId;
    this.messageFactory = messageFactory;

    this.responseBuilder.setRequestId(requestId);
  }

  @Override
  public void addCookie(Cookie cookie) {}

  @Override
  public boolean containsHeader(String name) {
    return false;
  }

  @Override
  public String encodeURL(String url) {
    return url;
  }

  @Override
  public String encodeRedirectURL(String url) {
    return url;
  }

  @Override
  public String encodeUrl(String url) {
    return url;
  }

  @Override
  public String encodeRedirectUrl(String url) {
    return url;
  }

  @Override
  public void sendError(int sc, String msg) throws IOException {
    setStatus(sc, msg);
  }

  @Override
  public void sendError(int sc) throws IOException {
    setStatus(sc);
  }

  @Override
  public void sendRedirect(String location) throws IOException {
    throw new IOException("Not supported!");
  }

  @Override
  public void setDateHeader(String name, long date) {}

  @Override
  public void addDateHeader(String name, long date) {}

  @Override
  public void setHeader(String name, String value) {}

  @Override
  public void addHeader(String name, String value) {}

  @Override
  public void setIntHeader(String name, int value) {}

  @Override
  public void addIntHeader(String name, int value) {}

  @Override
  public void setStatus(int sc) {
    setStatus(sc, "");
  }

  @Override
  public void setStatus(int sc, String sm) {
    this.responseBuilder.setStatusCode(sc);
    this.responseBuilder.setMessage(sm);
  }

  @Override
  public int getStatus() {
    return this.responseBuilder.getStatusCode();
  }

  @Override
  public String getHeader(String name) {
    return null;
  }

  @Override
  public Collection<String> getHeaders(String name) {
    return new LinkedList<>();
  }

  @Override
  public Collection<String> getHeaderNames() {
    return new LinkedList<>();
  }

  @Override
  public String getCharacterEncoding() {
    return "UTF-8";
  }

  @Override
  public String getContentType() {
    return null;
  }

  @Override
  public ServletOutputStream getOutputStream() throws IOException {
    return servletOutputStream;
  }

  @Override
  public PrintWriter getWriter() throws IOException {
    return new PrintWriter(servletOutputStream);
  }

  @Override
  public void setCharacterEncoding(String charset) {}

  @Override
  public void setContentLength(int len) {}

  @Override
  public void setContentLengthLong(long len) {}

  @Override
  public void setContentType(String type) {}

  @Override
  public void setBufferSize(int size) {}

  @Override
  public int getBufferSize() {
    return 0;
  }

  @Override
  public void flushBuffer() throws IOException {
    if (!isCommitted) {
      byte[] body = responseBody.toByteArray();

      if (body.length <= 0) {
        body = null;
      }

      byte[] response = messageFactory.createResponse(responseBuilder.getRequestId(),
                                                      responseBuilder.getStatusCode(),
                                                      responseBuilder.getMessage(),
                                                      new LinkedList<>(),
                                                      Optional.ofNullable(body))
                                      .toByteArray();

      endPoint.sendBytesByFuture(ByteBuffer.wrap(response));
      isCommitted = true;
    }
  }

  @Override
  public void resetBuffer() {
    if (isCommitted) throw new IllegalStateException("Buffer already flushed!");
    responseBody.reset();
  }

  @Override
  public boolean isCommitted() {
    return isCommitted;
  }

  @Override
  public void reset() {
    if (isCommitted) throw new IllegalStateException("Buffer already flushed!");
    responseBuilder = new ResponseBuilder();
    responseBuilder.setRequestId(requestId);
    resetBuffer();
  }

  @Override
  public void setLocale(Locale loc) {}

  @Override
  public Locale getLocale() {
    return Locale.US;
  }

  private static class ResponseBuilder {
    private long   requestId;
    private int    statusCode;
    private String message = "";

    public long getRequestId() {
      return requestId;
    }

    public void setRequestId(long requestId) {
      this.requestId = requestId;
    }

    public int getStatusCode() {
      return statusCode;
    }

    public void setStatusCode(int statusCode) {
      this.statusCode = statusCode;
    }

    public String getMessage() {
      return message;
    }

    public void setMessage(String message) {
      this.message = message;
    }
  }
}
