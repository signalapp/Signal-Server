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

import org.whispersystems.websocket.messages.WebSocketRequestMessage;
import org.whispersystems.websocket.session.WebSocketSessionContext;

import javax.servlet.AsyncContext;
import javax.servlet.DispatcherType;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpUpgradeHandler;
import javax.servlet.http.Part;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.security.Principal;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Locale;
import java.util.Map;
import java.util.Vector;


public class WebSocketServletRequest implements HttpServletRequest {

  private final Map<String, String> headers    = new HashMap<>();
  private final Map<String, Object> attributes = new HashMap<>();

  private final WebSocketRequestMessage requestMessage;
  private final ServletInputStream      inputStream;
  private final ServletContext          servletContext;
  private final WebSocketSessionContext sessionContext;

  public WebSocketServletRequest(WebSocketSessionContext sessionContext,
                                 WebSocketRequestMessage requestMessage,
                                 ServletContext          servletContext)
  {
    this.requestMessage = requestMessage;
    this.servletContext = servletContext;
    this.sessionContext = sessionContext;

    if (requestMessage.getBody().isPresent()) {
      inputStream = new BufferingServletInputStream(requestMessage.getBody().get());
    } else {
      inputStream = new BufferingServletInputStream(new byte[0]);
    }

    headers.putAll(requestMessage.getHeaders());
  }

  @Override
  public String getAuthType() {
    return BASIC_AUTH;
  }

  @Override
  public Cookie[] getCookies() {
    return new Cookie[0];
  }

  @Override
  public long getDateHeader(String name) {
    return -1;
  }

  @Override
  public String getHeader(String name) {
    return headers.get(name.toLowerCase());
  }

  @Override
  public Enumeration<String> getHeaders(String name) {
    String         header  = this.headers.get(name.toLowerCase());
    Vector<String> results = new Vector<>();

    if (header != null) {
      results.add(header);
    }

    return results.elements();
  }

  @Override
  public Enumeration<String> getHeaderNames() {
    return new Vector<>(headers.keySet()).elements();
  }

  @Override
  public int getIntHeader(String name) {
    return -1;
  }

  @Override
  public String getMethod() {
    return requestMessage.getVerb();
  }

  @Override
  public String getPathInfo() {
    return requestMessage.getPath();
  }

  @Override
  public String getPathTranslated() {
    return requestMessage.getPath();
  }

  @Override
  public String getContextPath() {
    return "";
  }

  @Override
  public String getQueryString() {
    if (requestMessage.getPath().contains("?")) {
      return requestMessage.getPath().substring(requestMessage.getPath().indexOf("?") + 1);
    }

    return null;
  }

  @Override
  public String getRemoteUser() {
    return null;
  }

  @Override
  public boolean isUserInRole(String role) {
    return false;
  }

  @Override
  public Principal getUserPrincipal() {
    return new ContextPrincipal(sessionContext);
  }

  @Override
  public String getRequestedSessionId() {
    return null;
  }

  @Override
  public String getRequestURI() {
    if (requestMessage.getPath().contains("?")) {
      return requestMessage.getPath().substring(0, requestMessage.getPath().indexOf("?"));
    } else {
      return requestMessage.getPath();
    }
  }

  @Override
  public StringBuffer getRequestURL() {
    StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append("http://websocket");
    stringBuffer.append(getRequestURI());

    return stringBuffer;
  }

  @Override
  public String getServletPath() {
    return "";
  }

  @Override
  public HttpSession getSession(boolean create) {
    return null;
  }

  @Override
  public HttpSession getSession() {
    return null;
  }

  @Override
  public String changeSessionId() {
    return null;
  }

  @Override
  public boolean isRequestedSessionIdValid() {
    return false;
  }

  @Override
  public boolean isRequestedSessionIdFromCookie() {
    return false;
  }

  @Override
  public boolean isRequestedSessionIdFromURL() {
    return false;
  }

  @Override
  public boolean isRequestedSessionIdFromUrl() {
    return false;
  }

  @Override
  public boolean authenticate(HttpServletResponse response) throws IOException, ServletException {
    return false;
  }

  @Override
  public void login(String username, String password) throws ServletException {

  }

  @Override
  public void logout() throws ServletException {

  }

  @Override
  public Collection<Part> getParts() throws IOException, ServletException {
    return new LinkedList<>();
  }

  @Override
  public Part getPart(String name) throws IOException, ServletException {
    return null;
  }

  @Override
  public <T extends HttpUpgradeHandler> T upgrade(Class<T> handlerClass) throws IOException, ServletException {
    return null;
  }

  @Override
  public Object getAttribute(String name) {
    return attributes.get(name);
  }

  @Override
  public Enumeration<String> getAttributeNames() {
    return new Vector<>(attributes.keySet()).elements();
  }

  @Override
  public String getCharacterEncoding() {
    return null;
  }

  @Override
  public void setCharacterEncoding(String env) throws UnsupportedEncodingException {}

  @Override
  public int getContentLength() {
    if (requestMessage.getBody().isPresent()) {
      return requestMessage.getBody().get().length;
    } else {
      return 0;
    }
  }

  @Override
  public long getContentLengthLong() {
    return getContentLength();
  }

  @Override
  public String getContentType() {
    if (requestMessage.getBody().isPresent()) {
      return "application/json";
    } else {
      return null;
    }
  }

  @Override
  public ServletInputStream getInputStream() throws IOException {
    return inputStream;
  }

  @Override
  public String getParameter(String name) {
    String[] result = getParameterMap().get(name);

    if (result != null && result.length > 0) {
      return result[0];
    }

    return null;
  }

  @Override
  public Enumeration<String> getParameterNames() {
    return new Vector<>(getParameterMap().keySet()).elements();
  }

  @Override
  public String[] getParameterValues(String name) {
    return getParameterMap().get(name);
  }

  @Override
  public Map<String, String[]> getParameterMap() {
    Map<String, String[]> parameterMap    = new HashMap<>();
    String                queryParameters = getQueryString();

    if (queryParameters == null) {
      return parameterMap;
    }

    String[] tokens = queryParameters.split("&");

    for (String token : tokens) {
      String[] parts = token.split("=");

      if (parts != null && parts.length > 1) {
        parameterMap.put(parts[0], new String[] {parts[1]});
      }
    }

    return parameterMap;
  }

  @Override
  public String getProtocol() {
    return "HTTP/1.0";
  }

  @Override
  public String getScheme() {
    return "http";
  }

  @Override
  public String getServerName() {
    return "websocket";
  }

  @Override
  public int getServerPort() {
    return 8080;
  }

  @Override
  public BufferedReader getReader() throws IOException {
    return new BufferedReader(new InputStreamReader(inputStream));
  }

  @Override
  public String getRemoteAddr() {
    return "127.0.0.1";
  }

  @Override
  public String getRemoteHost() {
    return "localhost";
  }

  @Override
  public void setAttribute(String name, Object o) {
    if (o != null) attributes.put(name, o);
    else           removeAttribute(name);
  }

  @Override
  public void removeAttribute(String name) {
    attributes.remove(name);
  }

  @Override
  public Locale getLocale() {
    return Locale.US;
  }

  @Override
  public Enumeration<Locale> getLocales() {
    Vector<Locale> results = new Vector<>();
    results.add(getLocale());
    return results.elements();
  }

  @Override
  public boolean isSecure() {
    return false;
  }

  @Override
  public RequestDispatcher getRequestDispatcher(String path) {
    return servletContext.getRequestDispatcher(path);
  }

  @Override
  public String getRealPath(String path) {
    return path;
  }

  @Override
  public int getRemotePort() {
    return 31337;
  }

  @Override
  public String getLocalName() {
    return "localhost";
  }

  @Override
  public String getLocalAddr() {
    return "127.0.0.1";
  }

  @Override
  public int getLocalPort() {
    return 8080;
  }

  @Override
  public ServletContext getServletContext() {
    return servletContext;
  }

  @Override
  public AsyncContext startAsync() throws IllegalStateException {
    throw new AssertionError("nyi");
  }

  @Override
  public AsyncContext startAsync(ServletRequest servletRequest, ServletResponse servletResponse) throws IllegalStateException {
    throw new AssertionError("nyi");
  }

  @Override
  public boolean isAsyncStarted() {
    return false;
  }

  @Override
  public boolean isAsyncSupported() {
    return false;
  }

  @Override
  public AsyncContext getAsyncContext() {
    return null;
  }

  @Override
  public DispatcherType getDispatcherType() {
    return DispatcherType.REQUEST;
  }

  public static class ContextPrincipal implements Principal {

    private final WebSocketSessionContext context;

    public ContextPrincipal(WebSocketSessionContext context) {
      this.context = context;
    }

    @Override
    public boolean equals(Object another) {
      return another instanceof ContextPrincipal &&
             context.equals(((ContextPrincipal) another).context);
    }

    @Override
    public String toString() {
      return super.toString();
    }

    @Override
    public int hashCode() {
      return context.hashCode();
    }

    @Override
    public String getName() {
      return "WebSocketSessionContext";
    }

    public WebSocketSessionContext getContext() {
      return context;
    }
  }
}
