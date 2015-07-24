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

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.dropwizard.simpleauth.Authenticator;
import org.whispersystems.textsecuregcm.configuration.FederationConfiguration;
import org.whispersystems.textsecuregcm.federation.FederatedPeer;
import org.whispersystems.textsecuregcm.util.Constants;

import java.util.List;

import static com.codahale.metrics.MetricRegistry.name;
import io.dropwizard.auth.AuthenticationException;
import io.dropwizard.auth.basic.BasicCredentials;


public class FederatedPeerAuthenticator implements Authenticator<BasicCredentials, FederatedPeer> {

  private final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);

  private final Meter authenticationFailedMeter    = metricRegistry.meter(name(getClass(),
                                                                               "authentication",
                                                                               "failed"));

  private final Meter authenticationSucceededMeter = metricRegistry.meter(name(getClass(),
                                                                               "authentication",
                                                                               "succeeded"));

  private final Logger logger = LoggerFactory.getLogger(FederatedPeerAuthenticator.class);

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
