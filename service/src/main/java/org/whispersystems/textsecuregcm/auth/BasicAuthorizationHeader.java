/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.auth;

import java.util.Base64;
import org.apache.commons.lang3.StringUtils;
import org.whispersystems.textsecuregcm.util.Pair;

public class BasicAuthorizationHeader {

  private final String username;
  private final byte deviceId;
  private final String password;

  private BasicAuthorizationHeader(final String username, final byte deviceId, final String password) {
    this.username = username;
    this.deviceId = deviceId;
    this.password = password;
  }

  public static BasicAuthorizationHeader fromString(final String header) throws InvalidAuthorizationHeaderException {
    try {
      if (StringUtils.isBlank(header)) {
        throw new InvalidAuthorizationHeaderException("Blank header");
      }

      final int spaceIndex = header.indexOf(' ');

      if (spaceIndex == -1) {
        throw new InvalidAuthorizationHeaderException("Invalid authorization header: " + header);
      }

      final String authorizationType = header.substring(0, spaceIndex);

      if (!"Basic".equals(authorizationType)) {
        throw new InvalidAuthorizationHeaderException("Unsupported authorization method: " + authorizationType);
      }

      final String credentials;

      try {
        credentials = new String(Base64.getDecoder().decode(header.substring(spaceIndex + 1)));
      } catch (final IndexOutOfBoundsException e) {
        throw new InvalidAuthorizationHeaderException("Missing credentials");
      }

      if (StringUtils.isEmpty(credentials)) {
        throw new InvalidAuthorizationHeaderException("Bad decoded value: " + credentials);
      }

      final int credentialSeparatorIndex = credentials.indexOf(':');

      if (credentialSeparatorIndex == -1) {
        throw new InvalidAuthorizationHeaderException("Badly-formatted credentials: " + credentials);
      }

      final String usernameComponent = credentials.substring(0, credentialSeparatorIndex);

      final String username;
      final byte deviceId;
      {
        final Pair<String, Byte> identifierAndDeviceId =
            AccountAuthenticator.getIdentifierAndDeviceId(usernameComponent);

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
