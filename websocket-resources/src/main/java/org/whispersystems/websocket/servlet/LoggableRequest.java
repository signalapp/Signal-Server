package org.whispersystems.websocket.servlet;

import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpURI;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.server.Authentication;
import org.eclipse.jetty.server.HttpChannel;
import org.eclipse.jetty.server.HttpChannelState;
import org.eclipse.jetty.server.HttpInput;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.UserIdentity;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.util.Attributes;

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
import javax.servlet.http.Part;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.security.Principal;
import java.util.Collection;
import java.util.Enumeration;
import java.util.EventListener;
import java.util.Locale;
import java.util.Map;

public class LoggableRequest extends Request {

  private final HttpServletRequest request;

  public LoggableRequest(HttpServletRequest request) {
    super(null, null);
    this.request = request;
  }

  @Override
  public HttpFields getHttpFields() {
    throw new AssertionError();
  }

  @Override
  public HttpInput getHttpInput() {
    throw new AssertionError();
  }

  @Override
  public void addEventListener(EventListener listener) {
    throw new AssertionError();
  }

  @Override
  public AsyncContext getAsyncContext() {
    throw new AssertionError();
  }

  @Override
  public HttpChannelState getHttpChannelState() {
    throw new AssertionError();
  }

  @Override
  public Object getAttribute(String name) {
    return request.getAttribute(name);
  }

  @Override
  public Enumeration<String> getAttributeNames() {
    return request.getAttributeNames();
  }

  @Override
  public Attributes getAttributes() {
    throw new AssertionError();
  }

  @Override
  public Authentication getAuthentication() {
    return null;
  }

  @Override
  public String getAuthType() {
    return request.getAuthType();
  }

  @Override
  public String getCharacterEncoding() {
    return request.getCharacterEncoding();
  }

  @Override
  public HttpChannel getHttpChannel() {
    throw new AssertionError();
  }

  @Override
  public int getContentLength() {
    return request.getContentLength();
  }

  @Override
  public String getContentType() {
    return request.getContentType();
  }

  @Override
  public ContextHandler.Context getContext() {
    throw new AssertionError();
  }

  @Override
  public String getContextPath() {
    return request.getContextPath();
  }

  @Override
  public Cookie[] getCookies() {
    return request.getCookies();
  }

  @Override
  public long getDateHeader(String name) {
    return request.getDateHeader(name);
  }

  @Override
  public DispatcherType getDispatcherType() {
    return request.getDispatcherType();
  }

  @Override
  public String getHeader(String name) {
    return request.getHeader(name);
  }

  @Override
  public Enumeration<String> getHeaderNames() {
    return request.getHeaderNames();
  }

  @Override
  public Enumeration<String> getHeaders(String name) {
    return request.getHeaders(name);
  }

  @Override
  public int getInputState() {
    throw new AssertionError();
  }

  @Override
  public ServletInputStream getInputStream() throws IOException {
    return request.getInputStream();
  }

  @Override
  public int getIntHeader(String name) {
    return request.getIntHeader(name);
  }

  @Override
  public Locale getLocale() {
    return request.getLocale();
  }

  @Override
  public Enumeration<Locale> getLocales() {
    return request.getLocales();
  }

  @Override
  public String getLocalAddr() {
    return request.getLocalAddr();
  }

  @Override
  public String getLocalName() {
    return request.getLocalName();
  }

  @Override
  public int getLocalPort() {
    return request.getLocalPort();
  }

  @Override
  public String getMethod() {
    return request.getMethod();
  }

  @Override
  public String getParameter(String name) {
    return request.getParameter(name);
  }

  @Override
  public Map<String, String[]> getParameterMap() {
    return request.getParameterMap();
  }

  @Override
  public Enumeration<String> getParameterNames() {
    return request.getParameterNames();
  }

  @Override
  public String[] getParameterValues(String name) {
    return request.getParameterValues(name);
  }

  @Override
  public String getPathInfo() {
    return request.getPathInfo();
  }

  @Override
  public String getPathTranslated() {
    return request.getPathTranslated();
  }

  @Override
  public String getProtocol() {
    return request.getProtocol();
  }

  @Override
  public HttpVersion getHttpVersion() {
    throw new AssertionError();
  }

  @Override
  public String getQueryEncoding() {
    throw new AssertionError();
  }

  @Override
  public String getQueryString() {
    return request.getQueryString();
  }

  @Override
  public BufferedReader getReader() throws IOException {
    throw new AssertionError();
  }

  @Override
  public String getRealPath(String path) {
    return request.getRealPath(path);
  }

  @Override
  public String getRemoteAddr() {
    return request.getRemoteAddr();
  }

  @Override
  public String getRemoteHost() {
    return request.getRemoteHost();
  }

  @Override
  public int getRemotePort() {
    return request.getRemotePort();
  }

  @Override
  public String getRemoteUser() {
    return request.getRemoteUser();
  }

  @Override
  public RequestDispatcher getRequestDispatcher(String path) {
    return request.getRequestDispatcher(path);
  }

  @Override
  public String getRequestedSessionId() {
    return request.getRequestedSessionId();
  }

  @Override
  public String getRequestURI() {
    return request.getRequestURI();
  }

  @Override
  public StringBuffer getRequestURL() {
    return request.getRequestURL();
  }

  @Override
  public Response getResponse() {
    throw new AssertionError();
  }

  @Override
  public StringBuilder getRootURL() {
    throw new AssertionError();
  }

  @Override
  public String getScheme() {
    return request.getScheme();
  }

  @Override
  public String getServerName() {
    return request.getServerName();
  }

  @Override
  public int getServerPort() {
    return request.getServerPort();
  }

  @Override
  public ServletContext getServletContext() {
    return request.getServletContext();
  }

  @Override
  public String getServletName() {
    throw new AssertionError();
  }

  @Override
  public String getServletPath() {
    return request.getServletPath();
  }

  @Override
  public ServletResponse getServletResponse() {
    throw new AssertionError();
  }

  @Override
  public String changeSessionId() {
    throw new AssertionError();
  }

  @Override
  public HttpSession getSession() {
    return request.getSession();
  }

  @Override
  public HttpSession getSession(boolean create) {
    return request.getSession(create);
  }

  @Override
  public long getTimeStamp() {
    return System.currentTimeMillis();
  }

  @Override
  public HttpURI getHttpURI() {
    return new HttpURI(getRequestURI());
  }

  @Override
  public UserIdentity getUserIdentity() {
    throw new AssertionError();
  }

  @Override
  public UserIdentity getResolvedUserIdentity() {
    throw new AssertionError();
  }

  @Override
  public UserIdentity.Scope getUserIdentityScope() {
    throw new AssertionError();
  }

  @Override
  public Principal getUserPrincipal() {
    throw new AssertionError();
  }

  @Override
  public boolean isHandled() {
    throw new AssertionError();
  }

  @Override
  public boolean isAsyncStarted() {
    return request.isAsyncStarted();
  }

  @Override
  public boolean isAsyncSupported() {
    return request.isAsyncSupported();
  }

  @Override
  public boolean isRequestedSessionIdFromCookie() {
    return request.isRequestedSessionIdFromCookie();
  }

  @Override
  public boolean isRequestedSessionIdFromUrl() {
    return request.isRequestedSessionIdFromUrl();
  }

  @Override
  public boolean isRequestedSessionIdFromURL() {
    return request.isRequestedSessionIdFromURL();
  }

  @Override
  public boolean isRequestedSessionIdValid() {
    return request.isRequestedSessionIdValid();
  }

  @Override
  public boolean isSecure() {
    return request.isSecure();
  }

  @Override
  public void setSecure(boolean secure) {
    throw new AssertionError();
  }

  @Override
  public boolean isUserInRole(String role) {
    return request.isUserInRole(role);
  }

  @Override
  public void removeAttribute(String name) {
    request.removeAttribute(name);
  }

  @Override
  public void removeEventListener(EventListener listener) {
    throw new AssertionError();
  }

  @Override
  public void setAsyncSupported(boolean supported, String source) {
    throw new AssertionError();
  }

  @Override
  public void setAttribute(String name, Object value) {
    throw new AssertionError();
  }

  @Override
  public void setAttributes(Attributes attributes) {
    throw new AssertionError();
  }

  @Override
  public void setAuthentication(Authentication authentication) {
    throw new AssertionError();
  }

  @Override
  public void setCharacterEncoding(String encoding) throws UnsupportedEncodingException {
    throw new AssertionError();
  }

  @Override
  public void setCharacterEncodingUnchecked(String encoding) {
    throw new AssertionError();
  }

  @Override
  public void setContentType(String contentType) {
    throw new AssertionError();
  }

  @Override
  public void setContext(ContextHandler.Context context) {
    throw new AssertionError();
  }

  @Override
  public boolean takeNewContext() {
    throw new AssertionError();
  }

  @Override
  public void setContextPath(String contextPath) {
    throw new AssertionError();
  }

  @Override
  public void setCookies(Cookie[] cookies) {
    throw new AssertionError();
  }

  @Override
  public void setDispatcherType(DispatcherType type) {
    throw new AssertionError();
  }

  @Override
  public void setHandled(boolean h) {
    throw new AssertionError();
  }

  @Override
  public boolean isHead() {
    throw new AssertionError();
  }

  @Override
  public void setPathInfo(String pathInfo) {
    throw new AssertionError();
  }

  @Override
  public void setHttpVersion(HttpVersion version) {
    throw new AssertionError();
  }

  @Override
  public void setQueryEncoding(String queryEncoding) {
    throw new AssertionError();
  }

  @Override
  public void setQueryString(String queryString) {
    throw new AssertionError();
  }

  @Override
  public void setRemoteAddr(InetSocketAddress addr) {
    throw new AssertionError();
  }

  @Override
  public void setRequestedSessionId(String requestedSessionId) {
    throw new AssertionError();
  }

  @Override
  public void setRequestedSessionIdFromCookie(boolean requestedSessionIdCookie) {
    throw new AssertionError();
  }

  @Override
  public void setScheme(String scheme) {
    throw new AssertionError();
  }

  @Override
  public void setServletPath(String servletPath) {
    throw new AssertionError();
  }

  @Override
  public void setSession(HttpSession session) {
    throw new AssertionError();
  }

  @Override
  public void setTimeStamp(long ts) {
    throw new AssertionError();
  }

  @Override
  public void setHttpURI(HttpURI uri) {
    throw new AssertionError();
  }

  @Override
  public void setUserIdentityScope(UserIdentity.Scope scope) {
    throw new AssertionError();
  }

  @Override
  public AsyncContext startAsync() throws IllegalStateException {
    throw new AssertionError();
  }

  @Override
  public AsyncContext startAsync(ServletRequest servletRequest, ServletResponse servletResponse) throws IllegalStateException {
    throw new AssertionError();
  }

  @Override
  public String toString() {
    return request.toString();
  }

  @Override
  public boolean authenticate(HttpServletResponse response) throws IOException, ServletException {
    throw new AssertionError();
  }

  @Override
  public Part getPart(String name) throws IOException, ServletException {
    return request.getPart(name);
  }

  @Override
  public Collection<Part> getParts() throws IOException, ServletException {
    return request.getParts();
  }

  @Override
  public void login(String username, String password) throws ServletException {
    throw new AssertionError();
  }

  @Override
  public void logout() throws ServletException {
    throw new AssertionError();
  }

}
