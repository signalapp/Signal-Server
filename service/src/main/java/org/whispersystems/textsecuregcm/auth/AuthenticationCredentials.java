/*
 * Copyright (C) 2013 Open WhisperSystems
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.whispersystems.textsecuregcm.auth;

import org.apache.commons.codec.binary.Hex;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

public class AuthenticationCredentials {

  private final String hashedAuthenticationToken;
  private final String salt;

  public AuthenticationCredentials(String hashedAuthenticationToken, String salt) {
    this.hashedAuthenticationToken = hashedAuthenticationToken;
    this.salt                      = salt;
  }

  public AuthenticationCredentials(String authenticationToken) {
    this.salt                      = String.valueOf(Math.abs(new SecureRandom().nextInt()));
    this.hashedAuthenticationToken = getHashedValue(salt, authenticationToken);
  }

  public String getHashedAuthenticationToken() {
    return hashedAuthenticationToken;
  }

  public String getSalt() {
    return salt;
  }

  public boolean verify(String authenticationToken) {
    String theirValue = getHashedValue(salt, authenticationToken);
    return MessageDigest.isEqual(theirValue.getBytes(StandardCharsets.UTF_8), this.hashedAuthenticationToken.getBytes(StandardCharsets.UTF_8));
  }

  private static String getHashedValue(String salt, String token) {
    try {
      return new String(Hex.encodeHex(MessageDigest.getInstance("SHA1").digest((salt + token).getBytes(StandardCharsets.UTF_8))));
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    }
  }

}
