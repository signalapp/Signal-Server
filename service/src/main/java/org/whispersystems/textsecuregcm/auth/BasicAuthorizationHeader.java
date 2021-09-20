/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.auth;

import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.whispersystems.textsecuregcm.util.Pair;

public class BasicAuthorizationHeader {

  private final static Pattern AUTHORIZATION_HEADER_PATTERN = Pattern.compile("^ *([a-zA-Z]+) +([^ ]+) *$");

  private final String username;
  private final long deviceId;
  private final String password;

  private BasicAuthorizationHeader(final String username, final long deviceId, final String password) {
    this.username = username;
    this.deviceId = deviceId;
    this.password = password;
  }

  public static BasicAuthorizationHeader fromString(final String header) throws InvalidAuthorizationHeaderException {
    try {
      if (StringUtils.isBlank(header)) {
        throw new InvalidAuthorizationHeaderException("Blank header");
      }

      Matcher matcher = AUTHORIZATION_HEADER_PATTERN.matcher(header);

      if (!matcher.find()) {
        throw new InvalidAuthorizationHeaderException("Invalid authorization header: " + header);
      }

      final String authorizationType = matcher.group(1);

      if (!"Basic".equals(authorizationType)) {
        throw new InvalidAuthorizationHeaderException("Unsupported authorization method: " + authorizationType);
      }

      final String base64EncodedCredentials = matcher.group(2);

      final String credentials = new String(Base64.getDecoder().decode(base64EncodedCredentials));

      if (StringUtils.isEmpty(credentials)) {
        throw new InvalidAuthorizationHeaderException("Bad decoded value: " + credentials);
      }

      final int credentialSeparatorIndex = credentials.indexOf(':');

      if (credentialSeparatorIndex == -1) {
        throw new InvalidAuthorizationHeaderException("Badly-formatted credentials: " + credentials);
      }

      final String usernameComponent = credentials.substring(0, credentialSeparatorIndex);

      final String username;
      final long deviceId;
      {
        final Pair<String, Long> identifierAndDeviceId =
            BaseAccountAuthenticator.getIdentifierAndDeviceId(usernameComponent);

        username = identifierAndDeviceId.first();
        deviceId = identifierAndDeviceId.second();
      }

      final String password = credentials.substring(credentialSeparatorIndex + 1);

      if (StringUtils.isAnyBlank(username, password)) {
        throw new InvalidAuthorizationHeaderException("Username or password were blank");
      }

      return new BasicAuthorizationHeader(username, deviceId, password);
    } catch (final IllegalArgumentException | IndexOutOfBoundsException e) {
      throw new InvalidAuthorizationHeaderException(e);
    }
  }

  public String getUsername() {
    return username;
  }

  public long getDeviceId() {
    return deviceId;
  }

  public String getPassword() {
    return password;
  }
}
