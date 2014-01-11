/**
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

import com.google.common.base.Optional;
import com.yammer.dropwizard.auth.AuthenticationException;
import com.yammer.dropwizard.auth.Authenticator;
import com.yammer.dropwizard.auth.basic.BasicCredentials;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.AccountsManager;

import java.util.concurrent.TimeUnit;

public class DeviceAuthenticator implements Authenticator<BasicCredentials, Device> {

  private final Meter authenticationFailedMeter    = Metrics.newMeter(DeviceAuthenticator.class,
                                                                      "authentication", "failed",
                                                                      TimeUnit.MINUTES);

  private final Meter authenticationSucceededMeter = Metrics.newMeter(DeviceAuthenticator.class,
                                                                      "authentication", "succeeded",
                                                                      TimeUnit.MINUTES);

  private final Logger logger = LoggerFactory.getLogger(DeviceAuthenticator.class);

  private final AccountsManager accountsManager;

  public DeviceAuthenticator(AccountsManager accountsManager) {
    this.accountsManager = accountsManager;
  }

  @Override
  public Optional<Device> authenticate(BasicCredentials basicCredentials)
      throws AuthenticationException
  {
    AuthorizationHeader authorizationHeader;
    try {
      authorizationHeader = AuthorizationHeader.fromUserAndPassword(basicCredentials.getUsername(), basicCredentials.getPassword());
    } catch (InvalidAuthorizationHeaderException iahe) {
      return Optional.absent();
    }
    Optional<Device> device = accountsManager.get(authorizationHeader.getNumber(), authorizationHeader.getDeviceId());

    if (!device.isPresent()) {
      return Optional.absent();
    }

    if (device.get().getAuthenticationCredentials().verify(basicCredentials.getPassword())) {
      authenticationSucceededMeter.mark();
      return device;
    }

    authenticationFailedMeter.mark();
    return Optional.absent();
  }
}
