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
package org.whispersystems.textsecuregcm.providers;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.auth.AuthDescriptor;
import net.spy.memcached.auth.PlainCallbackHandler;
import org.whispersystems.textsecuregcm.configuration.MemcacheConfiguration;
import org.whispersystems.textsecuregcm.util.Util;

import java.io.IOException;

public class MemcachedClientFactory {

  private final MemcachedClient client;

  public MemcachedClientFactory(MemcacheConfiguration config) throws IOException {
    ConnectionFactoryBuilder builder = new ConnectionFactoryBuilder();
    builder.setProtocol(ConnectionFactoryBuilder.Protocol.BINARY);

    if (!Util.isEmpty(config.getUser())) {
      AuthDescriptor ad = new AuthDescriptor(new String[] { "PLAIN" },
                                             new PlainCallbackHandler(config.getUser(),
                                                                      config.getPassword()));

      builder.setAuthDescriptor(ad);
    }


    this.client = new MemcachedClient(builder.build(),
                                      AddrUtil.getAddresses(config.getServers()));
  }


  public MemcachedClient getClient() {
    return client;
  }
}
