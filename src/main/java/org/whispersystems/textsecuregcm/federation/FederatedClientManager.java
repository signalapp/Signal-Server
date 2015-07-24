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
package org.whispersystems.textsecuregcm.federation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.FederationConfiguration;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import io.dropwizard.client.JerseyClientConfiguration;
import io.dropwizard.setup.Environment;

public class FederatedClientManager {

  private final Logger logger = LoggerFactory.getLogger(FederatedClientManager.class);

  private final HashMap<String, FederatedClient> clients = new HashMap<>();

  public FederatedClientManager(Environment environment,
                                JerseyClientConfiguration clientConfig,
                                FederationConfiguration federationConfig)
      throws IOException
  {
    List<FederatedPeer> peers    = federationConfig.getPeers();
    String              identity = federationConfig.getName();

    if (peers != null) {
      for (FederatedPeer peer : peers) {
        logger.info("Adding peer: " + peer.getName());
        clients.put(peer.getName(), new FederatedClient(environment, clientConfig, identity, peer));
      }
    }
  }

  public FederatedClient getClient(String name) throws NoSuchPeerException {
    FederatedClient client = clients.get(name);

    if (client == null) {
      throw new NoSuchPeerException(name);
    }

    return client;
  }

  public List<FederatedClient> getClients() {
    return new LinkedList<>(clients.values());
  }

}
