/**
 * Copyright (C) 2018 Open WhisperSystems
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
package org.whispersystems.textsecuregcm.storage;

public class ActiveUser {
    private long id;
    private long lastActiveMs;
    private int deviceId;
    private int platformId;

    public ActiveUser() { }

    public ActiveUser(long id, long lastActiveMs, int deviceId, int platformId) {
      this.id           = id;
      this.lastActiveMs = lastActiveMs;
      this.deviceId     = deviceId;
      this.platformId   = platformId;
    }

    public long getId()           { return id; }
    public long getLastActiveMs() { return lastActiveMs; }
    public int  getDeviceId()     { return deviceId;     }
    public int  getPlatformId()   { return platformId;     }
  }

