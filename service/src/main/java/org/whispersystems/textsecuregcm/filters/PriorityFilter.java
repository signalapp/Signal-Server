/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.filters;

import jakarta.servlet.DispatcherType;
import jakarta.servlet.Filter;
import jakarta.servlet.ServletContext;
import java.util.EnumSet;
import java.util.Objects;
import org.eclipse.jetty.ee10.servlet.FilterHolder;
import org.eclipse.jetty.ee10.servlet.FilterMapping;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHandler;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.util.component.LifeCycle;

public class PriorityFilter {

  private PriorityFilter() {}

  private static FilterHolder getFilter(ServletContext servletContext, final Class<? extends Filter> filterClass) {
    final ContextHandler contextHandler = Objects.requireNonNull(ServletContextHandler.getServletContextHandler(servletContext));
    final ServletHandler servletHandler = contextHandler.getDescendant(ServletHandler.class);
    return servletHandler.getFilter(filterClass.getName());
  }

  /**
   * Ensure a filter is available on the provided ServletContext, a new filter will added if one does not already
   * exist.
   * <p>
   * If a new filter is added, it will be added before all other filters.
   * <p>
   * Modeled after {@link org.eclipse.jetty.ee10.websocket.servlet.WebSocketUpgradeFilter#ensureFilter(ServletContext)},
   * since its use of {@link org.eclipse.jetty.ee10.servlet.ServletHandler#prependFilter(FilterHolder)} is what makes
   * this necessary.
   */
  public static void ensureFilter(final ServletContext servletContext, final Filter filter) {
    FilterHolder existingFilter = getFilter(servletContext, filter.getClass());
    if (existingFilter != null) {
      return;
    }

    final ContextHandler contextHandler = ServletContextHandler.getServletContextHandler(servletContext);
    final ServletHandler servletHandler = contextHandler.getDescendant(ServletHandler.class);

    final String pathSpec = "/*";
    final FilterHolder holder = new FilterHolder(filter);
    holder.setName(filter.getClass().getName());
    holder.setAsyncSupported(true);

    final FilterMapping mapping = new FilterMapping();
    mapping.setFilterName(holder.getName());
    mapping.setPathSpec(pathSpec);
    mapping.setDispatcherTypes(EnumSet.of(DispatcherType.REQUEST));

    // Add as the first filter in the list.
    servletHandler.prependFilter(holder);
    servletHandler.prependFilterMapping(mapping);

    // If we create the filter we must also make sure it is removed if the context is stopped.
    contextHandler.addEventListener(new LifeCycle.Listener()
    {
      @Override
      public void lifeCycleStopping(LifeCycle event)
      {
        servletHandler.removeFilterHolder(holder);
        servletHandler.removeFilterMapping(mapping);
        contextHandler.removeEventListener(this);
      }

      @Override
      public String toString()
      {
        return String.format("%sCleanupListener", filter.getClass().getSimpleName());
      }
    });

  }
}
