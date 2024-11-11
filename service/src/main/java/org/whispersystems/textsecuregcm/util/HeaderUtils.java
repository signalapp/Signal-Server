/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import static java.util.Objects.requireNonNull;

import com.google.common.net.HttpHeaders;
import io.dropwizard.auth.basic.BasicCredentials;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.container.ContainerRequestContext;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;

public final class HeaderUtils {

  private static final Logger logger = LoggerFactory.getLogger(HeaderUtils.class);

  public static final String X_SIGNAL_AGENT = "X-Signal-Agent";

  public static final String X_SIGNAL_KEY = "X-Signal-Key";

  public static final String TIMESTAMP_HEADER = "X-Signal-Timestamp";

  public static final String UNIDENTIFIED_ACCESS_KEY = "Unidentified-Access-Key";

  public static final String GROUP_SEND_TOKEN = "Group-Send-Token";

  private static final String INVALID_ACCEPT_LANGUAGE_COUNTER_NAME = MetricsUtil.name(HeaderUtils.class,
      "invalidAcceptLanguage");

  private HeaderUtils() {
    // utility class
  }

  public static String basicAuthHeader(final ExternalServiceCredentials credentials) {
    return basicAuthHeader(credentials.username(), credentials.password());
  }

  public static String basicAuthHeader(final String username, final String password) {
    requireNonNull(username);
    requireNonNull(password);
    return "Basic " + Base64.getEncoder().encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
  }

  @Nonnull
  public static String getTimestampHeader() {
    return TIMESTAMP_HEADER + ":" + System.currentTimeMillis();
  }

  /**
   * Parses a Base64-encoded value of the `Authorization` header in the form of `Basic dXNlcm5hbWU6cGFzc3dvcmQ=`. Note:
   * parsing logic is copied from {@link io.dropwizard.auth.basic.BasicCredentialAuthFilter#getCredentials(String)}.
   */
  public static Optional<BasicCredentials> basicCredentialsFromAuthHeader(final String authHeader) {
    final int space = authHeader.indexOf(' ');
    if (space <= 0) {
      return Optional.empty();
    }

    final String method = authHeader.substring(0, space);
    if (!"Basic".equalsIgnoreCase(method)) {
      return Optional.empty();
    }

    final String decoded;
    try {
      decoded = new String(Base64.getDecoder().decode(authHeader.substring(space + 1)), StandardCharsets.UTF_8);
    } catch (IllegalArgumentException e) {
      return Optional.empty();
    }

    // Decoded credentials is 'username:password'
    final int i = decoded.indexOf(':');
    if (i <= 0) {
      return Optional.empty();
    }

    final String username = decoded.substring(0, i);
    final String password = decoded.substring(i + 1);
    return Optional.of(new BasicCredentials(username, password));
  }

  public static List<Locale> getAcceptableLanguagesForRequest(ContainerRequestContext containerRequestContext) {
    try {
      return containerRequestContext.getAcceptableLanguages();
    } catch (final ProcessingException e) {
      final String userAgent = containerRequestContext.getHeaderString(HttpHeaders.USER_AGENT);
      Metrics.counter(INVALID_ACCEPT_LANGUAGE_COUNTER_NAME, Tags.of(
              UserAgentTagUtil.getPlatformTag(userAgent),
              Tag.of("path", containerRequestContext.getUriInfo().getPath())))
          .increment();
      logger.debug("Could not get acceptable languages; Accept-Language: {}; User-Agent: {}",
          containerRequestContext.getHeaderString(HttpHeaders.ACCEPT_LANGUAGE),
          userAgent,
          e);

      return List.of();
    }
  }


}
