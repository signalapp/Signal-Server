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
package org.whispersystems.textsecuregcm.limits;

import java.io.Serializable;

public class LeakyBucket implements Serializable {

  private final int    bucketSize;
  private final double leakRatePerMillis;

  private int spaceRemaining;
  private long lastUpdateTimeMillis;

  public LeakyBucket(int bucketSize, double leakRatePerMillis) {
    this.bucketSize           = bucketSize;
    this.leakRatePerMillis    = leakRatePerMillis;
    this.spaceRemaining       = bucketSize;
    this.lastUpdateTimeMillis = System.currentTimeMillis();
  }

  public boolean add(int amount) {
    this.spaceRemaining = getUpdatedSpaceRemaining();

    if (this.spaceRemaining >= amount) {
      this.spaceRemaining -= amount;
      return true;
    } else {
      return false;
    }
  }

  private int getUpdatedSpaceRemaining() {
    long elapsedTime = System.currentTimeMillis() - this.lastUpdateTimeMillis;

    return Math.min(this.bucketSize,
                    (int)Math.floor(this.spaceRemaining + (elapsedTime * this.leakRatePerMillis)));
  }
}