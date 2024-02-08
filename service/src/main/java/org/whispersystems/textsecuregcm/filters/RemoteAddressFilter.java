/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.filters;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.HttpServletRequestUtil;

/**
 * Sets a {@link HttpServletRequest} attribute (that will also be available as a
 * {@link javax.ws.rs.container.ContainerRequestContext} property) with the remote address of the connection, using
 * either the {@link HttpServletRequest#getRemoteAddr()} or the {@code X-Forwarded-For} HTTP header value, depending on
 * whether {@link #preferRemoteAddress} is {@code true}.
 */
public class RemoteAddressFilter implements Filter {

  public static final String REMOTE_ADDRESS_ATTRIBUTE_NAME = RemoteAddressFilter.class.getName() + ".remoteAddress";
  private static final Logger logger = LoggerFactory.getLogger(RemoteAddressFilter.class);

  private final boolean preferRemoteAddress;


  public RemoteAddressFilter(boolean preferRemoteAddress) {
    this.preferRemoteAddress = preferRemoteAddress;
  }

  @Override
  public void doFilter(final ServletRequest request, final ServletResponse response, final FilterChain chain)
      throws ServletException, IOException {

    if (request instanceof HttpServletRequest httpServletRequest) {

      final String remoteAddress;

      if (preferRemoteAddress) {
        remoteAddress = HttpServletRequestUtil.getRemoteAddress(httpServletRequest);
      } else {
        final String forwardedFor = httpServletRequest.getHeader(com.google.common.net.HttpHeaders.X_FORWARDED_FOR);
        remoteAddress = getMostRecentProxy(forwardedFor)
            .orElseGet(() -> HttpServletRequestUtil.getRemoteAddress(httpServletRequest));
      }

      request.setAttribute(REMOTE_ADDRESS_ATTRIBUTE_NAME, remoteAddress);

    } else {
      logger.warn("request was of unexpected type: {}", request.getClass());
    }

    chain.doFilter(request, response);
  }

  /**
   * Returns the most recent proxy in a chain described by an {@code X-Forwarded-For} header.
   *
   * @param forwardedFor the value of an X-Forwarded-For header
   * @return the IP address of the most recent proxy in the forwarding chain, or empty if none was found or
   * {@code forwardedFor} was null
   * @see <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/X-Forwarded-For">X-Forwarded-For - HTTP |
   * MDN</a>
   */
  @VisibleForTesting
  static Optional<String> getMostRecentProxy(@Nullable final String forwardedFor) {
    return Optional.ofNullable(forwardedFor)
        .map(ff -> {
          final int idx = forwardedFor.lastIndexOf(',') + 1;
          return idx < forwardedFor.length()
              ? forwardedFor.substring(idx).trim()
              : null;
        })
        .filter(StringUtils::isNotBlank);
  }
}
