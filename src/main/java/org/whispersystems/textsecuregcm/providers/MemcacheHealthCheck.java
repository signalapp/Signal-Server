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

import com.codahale.metrics.health.HealthCheck;
import net.spy.memcached.MemcachedClient;

import java.security.SecureRandom;

public class MemcacheHealthCheck extends HealthCheck {

  private final MemcachedClient client;

  public MemcacheHealthCheck(MemcachedClient client) {
    this.client = client;
  }

  @Override
  protected Result check() throws Exception {
    if (client == null) {
      return Result.unhealthy("not configured");
    }

    int random = SecureRandom.getInstance("SHA1PRNG").nextInt();
    int value  = SecureRandom.getInstance("SHA1PRNG").nextInt();

    this.client.set("HEALTH" + random, 2000, String.valueOf(value));
    String result = (String)this.client.get("HEALTH" + random);

    if (result == null || Integer.parseInt(result) != value) {
      return Result.unhealthy("Fetch failed");
    }

    this.client.delete("HEALTH" + random);

    return Result.healthy();
  }

}
