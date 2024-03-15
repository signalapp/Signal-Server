/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.jetty;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.util.component.Container;
import org.eclipse.jetty.util.component.LifeCycle;

/**
 * Uses {@link Container.Listener} to update {@link org.eclipse.jetty.server.HttpConfiguration}
 */
public class JettyHttpConfigurationCustomizer implements Container.Listener, LifeCycle.Listener {

  @Override
  public void beanAdded(final Container parent, final Object child) {
    if (child instanceof Connector c) {
      final HttpConnectionFactory hcf = c.getConnectionFactory(HttpConnectionFactory.class);
      if (hcf != null) {
        // see https://github.com/jetty/jetty.project/issues/1891
        hcf.getHttpConfiguration().setNotifyRemoteAsyncErrors(false);
      }
    }
  }

  @Override
  public void beanRemoved(final Container parent, final Object child) {

  }
}
