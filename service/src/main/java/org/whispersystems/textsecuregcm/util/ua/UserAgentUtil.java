/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util.ua;

import com.google.common.annotations.VisibleForTesting;
import com.vdurmont.semver4j.Semver;
import io.grpc.Context;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;

public class UserAgentUtil {

  public static final Context.Key<UserAgent> USER_AGENT_CONTEXT_KEY = Context.key("x-signal-user-agent");

  private static final Pattern STANDARD_UA_PATTERN = Pattern.compile("^Signal-(Android|Desktop|iOS)/([^ ]+)( (.+))?$", Pattern.CASE_INSENSITIVE);

  public static UserAgent parseUserAgentString(final String userAgentString) throws UnrecognizedUserAgentException {
    if (StringUtils.isBlank(userAgentString)) {
      throw new UnrecognizedUserAgentException("User-Agent string is blank");
    }

    try {
      final UserAgent standardUserAgent = parseStandardUserAgentString(userAgentString);

      if (standardUserAgent != null) {
        return standardUserAgent;
      }
    } catch (final Exception e) {
      throw new UnrecognizedUserAgentException(e);
    }

    throw new UnrecognizedUserAgentException();
  }

  public static UserAgent userAgentFromGrpcContext() {
    return USER_AGENT_CONTEXT_KEY.get();
  }

  @VisibleForTesting
  static UserAgent parseStandardUserAgentString(final String userAgentString) {
    final Matcher matcher = STANDARD_UA_PATTERN.matcher(userAgentString);

    if (matcher.matches()) {
      return new UserAgent(ClientPlatform.valueOf(matcher.group(1).toUpperCase()), new Semver(matcher.group(2)), StringUtils.stripToNull(matcher.group(4)));
    }

    return null;
  }
}
