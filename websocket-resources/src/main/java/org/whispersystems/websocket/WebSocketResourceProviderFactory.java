/*
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
package org.whispersystems.websocket;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.AttributesMap;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.eclipse.jetty.websocket.servlet.WebSocketCreator;
import org.eclipse.jetty.websocket.servlet.WebSocketServlet;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.websocket.auth.AuthenticationException;
import org.whispersystems.websocket.auth.WebSocketAuthenticator;
import org.whispersystems.websocket.auth.WebSocketAuthenticator.AuthenticationResult;
import org.whispersystems.websocket.auth.internal.WebSocketAuthValueFactoryProvider;
import org.whispersystems.websocket.session.WebSocketSessionContextValueFactoryProvider;
import org.whispersystems.websocket.setup.WebSocketEnvironment;

import javax.servlet.Filter;
import javax.servlet.FilterRegistration;
import javax.servlet.RequestDispatcher;
import javax.servlet.Servlet;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRegistration;
import javax.servlet.SessionCookieConfig;
import javax.servlet.SessionTrackingMode;
import javax.servlet.descriptor.JspConfigDescriptor;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.AccessController;
import java.util.Collections;
import java.util.Enumeration;
import java.util.EventListener;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.dropwizard.jersey.jackson.JacksonMessageBodyProvider;

public class WebSocketResourceProviderFactory extends WebSocketServlet implements WebSocketCreator {

  private static final Logger logger = LoggerFactory.getLogger(WebSocketResourceProviderFactory.class);

  private final WebSocketEnvironment environment;

  public WebSocketResourceProviderFactory(WebSocketEnvironment environment)
      throws ServletException
  {
    this.environment = environment;

    environment.jersey().register(new WebSocketSessionContextValueFactoryProvider.Binder());
    environment.jersey().register(new WebSocketAuthValueFactoryProvider.Binder());
    environment.jersey().register(new JacksonMessageBodyProvider(environment.getObjectMapper()));
  }

  public void start() throws ServletException {
    this.environment.getJerseyServletContainer().init(new WServletConfig());
  }

  @Override
  public Object createWebSocket(ServletUpgradeRequest request, ServletUpgradeResponse response) {
    try {
      Optional<WebSocketAuthenticator> authenticator = Optional.ofNullable(environment.getAuthenticator());
      Object                           authenticated = null;

      if (authenticator.isPresent()) {
        AuthenticationResult authenticationResult = authenticator.get().authenticate(request);

        if (!authenticationResult.getUser().isPresent() && authenticationResult.isRequired()) {
          response.sendForbidden("Unauthorized");
          return null;
        } else {
          authenticated = authenticationResult.getUser().orElse(null);
        }
      }

      return new WebSocketResourceProvider(this.environment.getJerseyServletContainer(),
                                           this.environment.getRequestLog(),
                                           authenticated,
                                           this.environment.getMessageFactory(),
                                           Optional.ofNullable(this.environment.getConnectListener()),
                                           this.environment.getIdleTimeoutMillis());
    } catch (AuthenticationException | IOException e) {
      logger.warn("Authentication failure", e);
      return null;
    }
  }

  @Override
  public void configure(WebSocketServletFactory factory) {
    factory.setCreator(this);
  }

  private static class WServletConfig implements ServletConfig {

    private final ServletContext context = new NoContext();

    @Override
    public String getServletName() {
      return "WebSocketResourceServlet";
    }

    @Override
    public ServletContext getServletContext() {
      return context;
    }

    @Override
    public String getInitParameter(String name) {
      return null;
    }

    @Override
    public Enumeration<String> getInitParameterNames() {
      return new Enumeration<String>() {
        @Override
        public boolean hasMoreElements() {
          return false;
        }

        @Override
        public String nextElement() {
          return null;
        }
      };
    }
  }

  public static class NoContext extends AttributesMap implements ServletContext
  {

    private int effectiveMajorVersion = 3;
    private int effectiveMinorVersion = 0;

    @Override
    public ServletContext getContext(String uripath)
    {
      return null;
    }

    @Override
    public int getMajorVersion()
    {
      return 3;
    }

    @Override
    public String getMimeType(String file)
    {
      return null;
    }

    @Override
    public int getMinorVersion()
    {
      return 0;
    }

    @Override
    public RequestDispatcher getNamedDispatcher(String name)
    {
      return null;
    }

    @Override
    public RequestDispatcher getRequestDispatcher(String uriInContext)
    {
      return null;
    }

    @Override
    public String getRealPath(String path)
    {
      return null;
    }

    @Override
    public URL getResource(String path) throws MalformedURLException
    {
      return null;
    }

    @Override
    public InputStream getResourceAsStream(String path)
    {
      return null;
    }

    @Override
    public Set<String> getResourcePaths(String path)
    {
      return null;
    }

    @Override
    public String getServerInfo()
    {
      return "websocketresources/" + Server.getVersion();
    }

    @Override
    @Deprecated
    public Servlet getServlet(String name) throws ServletException
    {
      return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    @Deprecated
    public Enumeration<String> getServletNames()
    {
      return Collections.enumeration(Collections.EMPTY_LIST);
    }

    @SuppressWarnings("unchecked")
    @Override
    @Deprecated
    public Enumeration<Servlet> getServlets()
    {
      return Collections.enumeration(Collections.EMPTY_LIST);
    }

    @Override
    public void log(Exception exception, String msg)
    {
      logger.warn(msg,exception);
    }

    @Override
    public void log(String msg)
    {
      logger.info(msg);
    }

    @Override
    public void log(String message, Throwable throwable)
    {
      logger.warn(message,throwable);
    }

    @Override
    public String getInitParameter(String name)
    {
      return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Enumeration<String> getInitParameterNames()
    {
      return Collections.enumeration(Collections.EMPTY_LIST);
    }


    @Override
    public String getServletContextName()
    {
      return "No Context";
    }

    @Override
    public String getContextPath()
    {
      return null;
    }


    @Override
    public boolean setInitParameter(String name, String value)
    {
      return false;
    }

    @Override
    public FilterRegistration.Dynamic addFilter(String filterName, Class<? extends Filter> filterClass)
    {
      return null;
    }

    @Override
    public FilterRegistration.Dynamic addFilter(String filterName, Filter filter)
    {
      return null;
    }

    @Override
    public FilterRegistration.Dynamic addFilter(String filterName, String className)
    {
      return null;
    }

    @Override
    public javax.servlet.ServletRegistration.Dynamic addServlet(String servletName, Class<? extends Servlet> servletClass)
    {
      return null;
    }

    @Override
    public javax.servlet.ServletRegistration.Dynamic addServlet(String servletName, Servlet servlet)
    {
      return null;
    }

    @Override
    public javax.servlet.ServletRegistration.Dynamic addServlet(String servletName, String className)
    {
      return null;
    }

    @Override
    public <T extends Filter> T createFilter(Class<T> c) throws ServletException
    {
      return null;
    }

    @Override
    public <T extends Servlet> T createServlet(Class<T> c) throws ServletException
    {
      return null;
    }

    @Override
    public Set<SessionTrackingMode> getDefaultSessionTrackingModes()
    {
      return null;
    }

    @Override
    public Set<SessionTrackingMode> getEffectiveSessionTrackingModes()
    {
      return null;
    }

    @Override
    public FilterRegistration getFilterRegistration(String filterName)
    {
      return null;
    }

    @Override
    public Map<String, ? extends FilterRegistration> getFilterRegistrations()
    {
      return null;
    }

    @Override
    public ServletRegistration getServletRegistration(String servletName)
    {
      return null;
    }

    @Override
    public Map<String, ? extends ServletRegistration> getServletRegistrations()
    {
      return null;
    }

    @Override
    public SessionCookieConfig getSessionCookieConfig()
    {
      return null;
    }

    @Override
    public void setSessionTrackingModes(Set<SessionTrackingMode> sessionTrackingModes)
    {
    }

    @Override
    public void addListener(String className)
    {
    }

    @Override
    public <T extends EventListener> void addListener(T t)
    {
    }

    @Override
    public void addListener(Class<? extends EventListener> listenerClass)
    {
    }

    @Override
    public <T extends EventListener> T createListener(Class<T> clazz) throws ServletException
    {
      try
      {
        return clazz.newInstance();
      }
      catch (InstantiationException e)
      {
        throw new ServletException(e);
      }
      catch (IllegalAccessException e)
      {
        throw new ServletException(e);
      }
    }

    @Override
    public ClassLoader getClassLoader()
    {
      AccessController.checkPermission(new RuntimePermission("getClassLoader"));
      return WebSocketResourceProviderFactory.class.getClassLoader();
    }

    @Override
    public int getEffectiveMajorVersion()
    {
      return effectiveMajorVersion;
    }

    @Override
    public int getEffectiveMinorVersion()
    {
      return effectiveMinorVersion;
    }

    public void setEffectiveMajorVersion (int v)
    {
      this.effectiveMajorVersion = v;
    }

    public void setEffectiveMinorVersion (int v)
    {
      this.effectiveMinorVersion = v;
    }

    @Override
    public JspConfigDescriptor getJspConfigDescriptor()
    {
      return null;
    }

    @Override
    public void declareRoles(String... roleNames)
    {
    }

    @Override
    public String getVirtualServerName() {
      return null;
    }
  }

}
