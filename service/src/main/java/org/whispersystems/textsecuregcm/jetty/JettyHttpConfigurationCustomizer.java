/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.jetty;

import org.eclipse.jetty.http2.server.HTTP2ServerConnectionFactory;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.util.component.Container;
import org.eclipse.jetty.util.component.LifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Uses {@link Container.Listener} to update {@link org.eclipse.jetty.server.HttpConfiguration}
 */
public class JettyHttpConfigurationCustomizer implements Container.Listener, LifeCycle.Listener {

  private static final Logger logger = LoggerFactory.getLogger(JettyHttpConfigurationCustomizer.class);

  @Override
  public void beanAdded(final Container parent, final Object child) {
    if (child instanceof Connector c) {

      for (ConnectionFactory cf : c.getConnectionFactories()) {
        final HttpConfiguration httpConfiguration = switch (cf) {
          case HTTP2ServerConnectionFactory h2cf -> h2cf.getHttpConfiguration();
          case HttpConnectionFactory hcf -> hcf.getHttpConfiguration();
          default -> null;
        };

        if (httpConfiguration != null) {
          // see https://github.com/jetty/jetty.project/issues/1891
          logger.info("setNotifyRemoteAsyncErrors(false) for {}", cf);
          httpConfiguration.setNotifyRemoteAsyncErrors(false);
        }
      }
    }
  }

  @Override
  public void beanRemoved(final Container parent, final Object child) {

  }
}
