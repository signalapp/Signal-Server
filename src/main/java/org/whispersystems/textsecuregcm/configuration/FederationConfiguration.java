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
package org.whispersystems.textsecuregcm.configuration;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.whispersystems.textsecuregcm.federation.FederatedPeer;

import java.util.LinkedList;
import java.util.List;

public class FederationConfiguration {

  @JsonProperty
  private List<FederatedPeer> peers;

  @JsonProperty
  private String name;

  @JsonProperty
  private String herokuPeers;

  public List<FederatedPeer> getPeers() {
    if (peers != null) {
      return peers;
    }

    if (herokuPeers != null) {
      List<FederatedPeer> peers        = new LinkedList<>();
      JsonElement         root         = new JsonParser().parse(herokuPeers);
      JsonArray           peerElements = root.getAsJsonArray();

      for (JsonElement peer : peerElements) {
        String name                = peer.getAsJsonObject().get("name").getAsString();
        String url                 = peer.getAsJsonObject().get("url").getAsString();
        String authenticationToken = peer.getAsJsonObject().get("authenticationToken").getAsString();
        String certificate         = peer.getAsJsonObject().get("certificate").getAsString();

        peers.add(new FederatedPeer(name, url, authenticationToken, certificate));
      }

      return peers;
    }

    return peers;
  }

  public String getName() {
    return name;
  }
}
