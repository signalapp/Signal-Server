/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.abuse;

import io.dropwizard.lifecycle.Managed;
import javax.ws.rs.container.ContainerRequestFilter;
import java.io.IOException;

/**
 * An abusive message filter is a {@link ContainerRequestFilter} that filters requests to message-sending endpoints to
 * detect and respond to patterns of abusive behavior.
 * <p/>
 * Abusive message filters are managed components that are generally loaded dynamically via a
 * {@link java.util.ServiceLoader}. Their {@link #configure(String)} method will be called prior to be adding to the
 * server's pool of {@link Managed} objects.
 * <p/>
 * Abusive message filters must be annotated with {@link FilterAbusiveMessages}, a name binding annotation that
 * restricts the endpoints to which the filter may apply.
 */
public interface AbusiveMessageFilter extends ContainerRequestFilter, Managed {

  /**
   * Configures this abusive message filter. This method will be called before the filter is added to the server's pool
   * of managed objects and before the server processes any requests.
   *
   * @param environmentName the name of the environment in which this filter is running (e.g. "staging" or "production")
   * @throws IOException if the filter could not read its configuration source for any reason
   */
  void configure(String environmentName) throws IOException;
}
