/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import org.whispersystems.textsecuregcm.filters.RemoteAddressFilter;
import javax.annotation.Priority;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import java.io.IOException;

/**
 * Adds the request property set by {@link RemoteAddressFilter} for test scenarios that depend on it, but do not have
 * access to a full {@code HttpServletRequest} pipline
 */
@Priority(Integer.MIN_VALUE) // highest priority, since other filters might depend on it
public class TestRemoteAddressFilterProvider implements ContainerRequestFilter {

  private final String ip;

  public TestRemoteAddressFilterProvider(String ip) {
    this.ip = ip;
  }

  @Override
  public void filter(final ContainerRequestContext requestContext) throws IOException {
    requestContext.setProperty(RemoteAddressFilter.REMOTE_ADDRESS_ATTRIBUTE_NAME, ip);
  }
}
