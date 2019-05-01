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

import javax.servlet.ServletOutputStream;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Locale;

public class NullServletResponse implements HttpServletResponse {
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
  public void sendError(int sc, String msg) throws IOException {}

  @Override
  public void sendError(int sc) throws IOException {}

  @Override
  public void sendRedirect(String location) throws IOException {}

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
  public void setStatus(int sc) {}

  @Override
  public void setStatus(int sc, String sm) {}

  @Override
  public int getStatus() {
    return 200;
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
    return new NullServletOutputStream();
  }

  @Override
  public PrintWriter getWriter() throws IOException {
    return new PrintWriter(new NullServletOutputStream());
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
  public void flushBuffer() throws IOException {}

  @Override
  public void resetBuffer() {}

  @Override
  public boolean isCommitted() {
    return true;
  }

  @Override
  public void reset() {}

  @Override
  public void setLocale(Locale loc) {}

  @Override
  public Locale getLocale() {
    return Locale.US;
  }
}
