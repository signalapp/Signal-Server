/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.limits;

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import jakarta.ws.rs.ClientErrorException;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import org.glassfish.jersey.server.ExtendedUriInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.filters.RemoteAddressFilter;
import org.whispersystems.textsecuregcm.mappers.RateLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;

public class RateLimitByIpFilter implements ContainerRequestFilter {

  private static final Logger logger = LoggerFactory.getLogger(RateLimitByIpFilter.class);

  @VisibleForTesting
  static final RateLimitExceededException INVALID_HEADER_EXCEPTION = new RateLimitExceededException(Duration.ofHours(1)
  );

  private static final ExceptionMapper<RateLimitExceededException> EXCEPTION_MAPPER = new RateLimitExceededExceptionMapper();

  private static final String NO_IP_COUNTER_NAME = MetricsUtil.name(RateLimitByIpFilter.class, "noIpAddress");

  private final RateLimiters rateLimiters;

  public RateLimitByIpFilter(final RateLimiters rateLimiters) {
    this.rateLimiters = requireNonNull(rateLimiters);
  }

  @Override
  public void filter(final ContainerRequestContext requestContext) throws IOException {
    // requestContext.getUriInfo() should always be an instance of `ExtendedUriInfo`
    // in the Jersey client
    if (!(requestContext.getUriInfo() instanceof final ExtendedUriInfo uriInfo)) {
      return;
    }

    final RateLimitedByIp annotation = uriInfo.getMatchedResourceMethod()
        .getInvocable()
        .getHandlingMethod()
        .getAnnotation(RateLimitedByIp.class);

    if (annotation == null) {
      return;
    }

    final RateLimiters.For handle = annotation.value();

    try {
      final Optional<String> remoteAddress = Optional.ofNullable(
          (String) requestContext.getProperty(RemoteAddressFilter.REMOTE_ADDRESS_ATTRIBUTE_NAME));

      // checking if we failed to extract the most recent IP for any reason
      if (remoteAddress.isEmpty()) {
        Metrics.counter(
            NO_IP_COUNTER_NAME,
            Tags.of(
                Tag.of("limiter", handle.id()),
                Tag.of("fail", String.valueOf(annotation.failOnUnresolvedIp()))))
            .increment();

        // checking if annotation is configured to fail when the most recent IP is not resolved
        if (annotation.failOnUnresolvedIp()) {
          logger.error("Remote address was null");
          throw INVALID_HEADER_EXCEPTION;
        }
        // otherwise, allow request
        return;
      }

      final RateLimiter rateLimiter = rateLimiters.forDescriptor(handle);
      rateLimiter.validate(remoteAddress.get());
    } catch (RateLimitExceededException e) {
      final Response response = EXCEPTION_MAPPER.toResponse(e);
      throw new ClientErrorException(response);
    }
  }
}
