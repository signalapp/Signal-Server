package org.whispersystems.textsecuregcm.storage;
public class ActiveUser {
    private long id;
    private long lastActiveMs;
    private int deviceId;
    private int platform;

    public ActiveUser() { }

    public ActiveUser(long id, long lastActiveMs, int deviceId, int platform) {
      this.id = id;
      this.lastActiveMs = lastActiveMs;
      this.deviceId = deviceId;
      this.platform = platform;
    }

    public long getId()           { return id; }
    public long getLastActiveMs() { return lastActiveMs; }
    public int  getDeviceId()     { return deviceId;     }
    public int  getPlatform()     { return platform;     }
  }

