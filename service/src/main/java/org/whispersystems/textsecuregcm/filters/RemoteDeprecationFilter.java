/*
 * Copyright 2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.filters;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.google.common.annotations.VisibleForTesting;
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
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.auth.AccountAuthenticator;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.grpc.RequireAuthenticationInterceptor;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicRemoteDeprecationConfiguration;
import org.whispersystems.textsecuregcm.grpc.GrpcExceptions;
import org.whispersystems.textsecuregcm.grpc.RequestAttributesUtil;
import org.whispersystems.textsecuregcm.grpc.ServerInterceptorUtil;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.DeviceCapability;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.util.HeaderUtils;
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

  private final AccountsManager accountsManager;
  private final AccountAuthenticator accountAuthenticator;
  private final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager;

  private static final String DEPRECATED_CLIENT_COUNTER_NAME = name(RemoteDeprecationFilter.class, "deprecated");
  private static final String PENDING_DEPRECATION_COUNTER_NAME = name(RemoteDeprecationFilter.class, "pendingDeprecation");
  private static final String PLATFORM_TAG = "platform";
  private static final String REASON_TAG_NAME = "reason";
  private static final String EXPIRED_CLIENT_REASON = "expired";
  private static final String BLOCKED_CLIENT_REASON = "blocked";
  private static final String SPQR_NOT_SUPPORTED_REASON = "spqr";

  public RemoteDeprecationFilter(final AccountsManager accountsManager,
      final AccountAuthenticator accountAuthenticator,
      final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager) {

    this.accountsManager = accountsManager;
    this.accountAuthenticator = accountAuthenticator;
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

    if (shouldBlock(userAgent, ((HttpServletRequest) request).getHeader(HttpHeaders.AUTHORIZATION))) {
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

    if (shouldBlock(userAgent, headers.get(RequireAuthenticationInterceptor.AUTHORIZATION_METADATA_KEY))) {
      return ServerInterceptorUtil.closeWithStatusException(call, GrpcExceptions.upgradeRequired());
    } else {
      return next.startCall(call, headers);
    }
  }

  @VisibleForTesting
  boolean shouldBlock(@Nullable final UserAgent userAgent, @Nullable final String authHeader) {

    final DynamicRemoteDeprecationConfiguration configuration = dynamicConfigurationManager
        .getConfiguration().getRemoteDeprecationConfiguration();
    final Map<ClientPlatform, Semver> minimumVersionsByPlatform = configuration.minimumVersions();
    final Map<ClientPlatform, Semver> versionsPendingDeprecationByPlatform = configuration
        .versionsPendingDeprecation();
    final Map<ClientPlatform, Set<Semver>> blockedVersionsByPlatform = configuration.blockedVersions();
    final Map<ClientPlatform, Set<Semver>> versionsPendingBlockByPlatform = configuration.versionsPendingBlock();

    if (userAgent == null) {
      // In first-party clients, we can enforce SPQR presence via minimum client version. For third-party clients, we
      // need to do a more expensive check for the actual capability.
      if ((configuration.spqrEnforcementPending() || configuration.requireSpqr()) && isMissingSpqrCapability(authHeader)) {
        if (configuration.requireSpqr()) {
          recordDeprecation(null, SPQR_NOT_SUPPORTED_REASON);
          return true;
        }

        recordPendingDeprecation(null, SPQR_NOT_SUPPORTED_REASON);
      }

      return false;
    }

    boolean shouldBlock = false;

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

  /// Tests whether the device identified by the given authentication header (if any) is definitively missing the SPQR
  /// capability.
  ///
  /// @return `true` if the authenticated device is definitively missing the SPQR capability or `false` if the
  /// capability is present, no credentials are presented, or if authentication failed for any reason
  @VisibleForTesting
  boolean isMissingSpqrCapability(@Nullable final String authHeader) {
    if (authHeader == null) {
      return false;
    }

    final Optional<AuthenticatedDevice> maybeAuthenticatedDevice =
        HeaderUtils.basicCredentialsFromAuthHeader(authHeader)
            .flatMap(accountAuthenticator::authenticate);

    return maybeAuthenticatedDevice
        .flatMap(authenticatedDevice -> accountsManager.getByAccountIdentifier(authenticatedDevice.accountIdentifier()))
        .flatMap(account -> account.getDevice(maybeAuthenticatedDevice.get().deviceId()))
        .map(device -> !device.hasCapability(DeviceCapability.SPARSE_POST_QUANTUM_RATCHET))
        .orElse(false);
  }

  private void recordDeprecation(@Nullable final UserAgent userAgent, final String reason) {
    Metrics.counter(DEPRECATED_CLIENT_COUNTER_NAME,
        PLATFORM_TAG, userAgent != null ? userAgent.platform().name().toLowerCase() : "unrecognized",
        REASON_TAG_NAME, reason).increment();
  }

  private void recordPendingDeprecation(@Nullable final UserAgent userAgent, final String reason) {
    Metrics.counter(PENDING_DEPRECATION_COUNTER_NAME,
        PLATFORM_TAG, userAgent != null ? userAgent.platform().name().toLowerCase() : "unrecognized",
        REASON_TAG_NAME, reason).increment();
  }
}
