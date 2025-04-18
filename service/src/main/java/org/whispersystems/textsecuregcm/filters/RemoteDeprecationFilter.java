/*
 * Copyright 2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.filters;

import static com.codahale.metrics.MetricRegistry.name;

import com.google.common.net.HttpHeaders;
import com.vdurmont.semver4j.Semver;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.micrometer.core.instrument.Metrics;
import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicRemoteDeprecationConfiguration;
import org.whispersystems.textsecuregcm.grpc.RequestAttributesUtil;
import org.whispersystems.textsecuregcm.grpc.StatusConstants;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.util.ua.ClientPlatform;
import org.whispersystems.textsecuregcm.util.ua.UnrecognizedUserAgentException;
import org.whispersystems.textsecuregcm.util.ua.UserAgent;
import org.whispersystems.textsecuregcm.util.ua.UserAgentUtil;

/**
 * The remote deprecation filter rejects traffic from clients older than a configured minimum
 * version. It may optionally also reject traffic from clients with unrecognized User-Agent strings.
 * If a client platform does not have a configured minimum version, all traffic from that client
 * platform is allowed.
 */
public class RemoteDeprecationFilter implements Filter, ServerInterceptor {

  private final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager;

  private static final String DEPRECATED_CLIENT_COUNTER_NAME = name(RemoteDeprecationFilter.class, "deprecated");
  private static final String PENDING_DEPRECATION_COUNTER_NAME = name(RemoteDeprecationFilter.class, "pendingDeprecation");
  private static final String PLATFORM_TAG = "platform";
  private static final String REASON_TAG_NAME = "reason";
  private static final String EXPIRED_CLIENT_REASON = "expired";
  private static final String BLOCKED_CLIENT_REASON = "blocked";
  private static final String UNRECOGNIZED_UA_REASON = "unrecognized_user_agent";

  public RemoteDeprecationFilter(final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager) {
    this.dynamicConfigurationManager = dynamicConfigurationManager;
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
    final String userAgentString = ((HttpServletRequest) request).getHeader(HttpHeaders.USER_AGENT);

    UserAgent userAgent;
    try {
      userAgent = UserAgentUtil.parseUserAgentString(userAgentString);
    } catch (final UnrecognizedUserAgentException e) {
      userAgent = null;
    }

    if (shouldBlock(userAgent)) {
      ((HttpServletResponse) response).sendError(499);
    } else {
      chain.doFilter(request, response);
    }
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      final ServerCall<ReqT, RespT> call,
      final Metadata headers,
      final ServerCallHandler<ReqT, RespT> next) {

    @Nullable final UserAgent userAgent = RequestAttributesUtil.getUserAgent()
        .map(userAgentString -> {
          try {
            return UserAgentUtil.parseUserAgentString(userAgentString);
          } catch (final UnrecognizedUserAgentException e) {
            return null;
          }
        }).orElse(null);

    if (shouldBlock(userAgent)) {
      call.close(StatusConstants.UPGRADE_NEEDED_STATUS, new Metadata());
      return new ServerCall.Listener<>() {};
    } else {
      return next.startCall(call, headers);
    }
  }

  private boolean shouldBlock(@Nullable final UserAgent userAgent) {
    final DynamicRemoteDeprecationConfiguration configuration = dynamicConfigurationManager
        .getConfiguration().getRemoteDeprecationConfiguration();
    final Map<ClientPlatform, Semver> minimumVersionsByPlatform = configuration.getMinimumVersions();
    final Map<ClientPlatform, Semver> versionsPendingDeprecationByPlatform = configuration
        .getVersionsPendingDeprecation();
    final Map<ClientPlatform, Set<Semver>> blockedVersionsByPlatform = configuration.getBlockedVersions();
    final Map<ClientPlatform, Set<Semver>> versionsPendingBlockByPlatform = configuration.getVersionsPendingBlock();

    boolean shouldBlock = false;

    if (userAgent == null) {
      if  (configuration.isUnrecognizedUserAgentAllowed()) {
        return false;
      }
      recordDeprecation(null, UNRECOGNIZED_UA_REASON);
      return true;
    }

    if (blockedVersionsByPlatform.containsKey(userAgent.platform())) {
      if (blockedVersionsByPlatform.get(userAgent.platform()).contains(userAgent.version())) {
        recordDeprecation(userAgent, BLOCKED_CLIENT_REASON);
        shouldBlock = true;
      }
    }

    if (minimumVersionsByPlatform.containsKey(userAgent.platform())) {
      if (userAgent.version().isLowerThan(minimumVersionsByPlatform.get(userAgent.platform()))) {
        recordDeprecation(userAgent, EXPIRED_CLIENT_REASON);
        shouldBlock = true;
      }
    }

    if (versionsPendingBlockByPlatform.containsKey(userAgent.platform())) {
      if (versionsPendingBlockByPlatform.get(userAgent.platform()).contains(userAgent.version())) {
        recordPendingDeprecation(userAgent, BLOCKED_CLIENT_REASON);
      }
    }

    if (versionsPendingDeprecationByPlatform.containsKey(userAgent.platform())) {
      if (userAgent.version().isLowerThan(versionsPendingDeprecationByPlatform.get(userAgent.platform()))) {
        recordPendingDeprecation(userAgent, EXPIRED_CLIENT_REASON);
      }
    }

    return shouldBlock;
  }

  private void recordDeprecation(final UserAgent userAgent, final String reason) {
    Metrics.counter(DEPRECATED_CLIENT_COUNTER_NAME,
        PLATFORM_TAG, userAgent != null ? userAgent.platform().name().toLowerCase() : "unrecognized",
        REASON_TAG_NAME, reason).increment();
  }

  private void recordPendingDeprecation(final UserAgent userAgent, final String reason) {
    Metrics.counter(PENDING_DEPRECATION_COUNTER_NAME,
        PLATFORM_TAG, userAgent.platform().name().toLowerCase(),
        REASON_TAG_NAME, reason).increment();
  }
}
