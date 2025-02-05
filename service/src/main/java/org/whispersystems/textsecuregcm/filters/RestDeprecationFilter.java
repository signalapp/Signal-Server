/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.filters;

import com.google.common.net.HttpHeaders;
import com.vdurmont.semver4j.Semver;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.core.SecurityContext;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.util.ua.ClientPlatform;
import org.whispersystems.textsecuregcm.util.ua.UnrecognizedUserAgentException;
import org.whispersystems.textsecuregcm.util.ua.UserAgent;
import org.whispersystems.textsecuregcm.util.ua.UserAgentUtil;

public class RestDeprecationFilter implements ContainerRequestFilter {

  private static final String EXPERIMENT_NAME = "restDeprecation";
  private static final String DEPRECATED_REST_COUNTER_NAME = MetricsUtil.name(RestDeprecationFilter.class, "blockedRestRequest");

  private static final Logger log = LoggerFactory.getLogger(RestDeprecationFilter.class);

  final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager;
  final ExperimentEnrollmentManager experimentEnrollmentManager;

  public RestDeprecationFilter(
      final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager,
      final ExperimentEnrollmentManager experimentEnrollmentManager) {
    this.dynamicConfigurationManager = dynamicConfigurationManager;
    this.experimentEnrollmentManager = experimentEnrollmentManager;
  }

  @Override
  public void filter(final ContainerRequestContext requestContext) throws IOException {

    final SecurityContext securityContext = requestContext.getSecurityContext();

    if (securityContext == null || securityContext.getUserPrincipal() == null) {
      // We can't check if an unauthenticated request is in the experiment
      return;
    }

    if (securityContext.getUserPrincipal() instanceof AuthenticatedDevice ad) {
      if (!experimentEnrollmentManager.isEnrolled(ad.getAccount().getUuid(), EXPERIMENT_NAME)) {
        return;
      }
    } else {
      log.error("Security context was not null but user principal was of type {}", securityContext.getUserPrincipal().getClass().getName());
      return;
    }

    final Map<ClientPlatform, Semver> minimumRestFreeVersion = dynamicConfigurationManager.getConfiguration().minimumRestFreeVersion();
    final String userAgentString = requestContext.getHeaderString(HttpHeaders.USER_AGENT);

    try {
      final UserAgent userAgent = UserAgentUtil.parseUserAgentString(userAgentString);
      final ClientPlatform platform = userAgent.getPlatform();
      final Semver version = userAgent.getVersion();
      if (!minimumRestFreeVersion.containsKey(platform)) {
        return;
      }
      if (version.isGreaterThanOrEqualTo(minimumRestFreeVersion.get(platform))) {
        Metrics.counter(
            DEPRECATED_REST_COUNTER_NAME, Tags.of("platform", platform.name().toLowerCase(), "version", version.toString()))
            .increment();
        throw new WebApplicationException("use websockets", 498);
      }
    } catch (final UnrecognizedUserAgentException e) {
      return;                   // at present we're only interested in experimenting on known clients
    }
  }
}
