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
import org.whispersystems.textsecuregcm.configuration.FederationConfiguration;
import org.whispersystems.textsecuregcm.federation.FederatedPeer;

import java.util.List;
import java.util.concurrent.TimeUnit;


public class FederatedPeerAuthenticator implements Authenticator<BasicCredentials, FederatedPeer> {

  private final Meter authenticationFailedMeter    = Metrics.newMeter(FederatedPeerAuthenticator.class,
                                                                      "authentication", "failed",
                                                                      TimeUnit.MINUTES);

  private final Meter authenticationSucceededMeter = Metrics.newMeter(FederatedPeerAuthenticator.class,
                                                                      "authentication", "succeeded",
                                                                      TimeUnit.MINUTES);

  private final List<FederatedPeer> peers;

  public FederatedPeerAuthenticator(FederationConfiguration config) {
    this.peers = config.getPeers();
  }

  @Override
  public Optional<FederatedPeer> authenticate(BasicCredentials basicCredentials)
      throws AuthenticationException
  {

    if (peers == null) {
      authenticationFailedMeter.mark();
      return Optional.absent();
    }

    for (FederatedPeer peer : peers) {
      if (basicCredentials.getUsername().equals(peer.getName()) &&
          basicCredentials.getPassword().equals(peer.getAuthenticationToken()))
      {
        authenticationSucceededMeter.mark();
        return Optional.of(peer);
      }
    }

    authenticationFailedMeter.mark();
    return Optional.absent();
  }
}
