/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.limits;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class LeakyBucket {

  private final int    bucketSize;
  private final double leakRatePerMillis;

  private int spaceRemaining;
  private long lastUpdateTimeMillis;

  public LeakyBucket(int bucketSize, double leakRatePerMillis) {
    this(bucketSize, leakRatePerMillis, bucketSize, System.currentTimeMillis());
  }

  private LeakyBucket(int bucketSize, double leakRatePerMillis, int spaceRemaining, long lastUpdateTimeMillis) {
    this.bucketSize           = bucketSize;
    this.leakRatePerMillis    = leakRatePerMillis;
    this.spaceRemaining       = spaceRemaining;
    this.lastUpdateTimeMillis = lastUpdateTimeMillis;
  }

  public boolean add(int amount) {
    this.spaceRemaining       = getUpdatedSpaceRemaining();
    this.lastUpdateTimeMillis = System.currentTimeMillis();

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

  public String serialize(ObjectMapper mapper) throws JsonProcessingException {
    return mapper.writeValueAsString(new LeakyBucketEntity(bucketSize, leakRatePerMillis, spaceRemaining, lastUpdateTimeMillis));
  }

  public static LeakyBucket fromSerialized(ObjectMapper mapper, String serialized) throws IOException {
    LeakyBucketEntity entity = mapper.readValue(serialized, LeakyBucketEntity.class);

    return new LeakyBucket(entity.bucketSize, entity.leakRatePerMillis,
                           entity.spaceRemaining, entity.lastUpdateTimeMillis);
  }

  private static class LeakyBucketEntity {
    @JsonProperty
    private int    bucketSize;

    @JsonProperty
    private double leakRatePerMillis;

    @JsonProperty
    private int    spaceRemaining;

    @JsonProperty
    private long   lastUpdateTimeMillis;

    public LeakyBucketEntity() {}

    private LeakyBucketEntity(int bucketSize, double leakRatePerMillis,
                              int spaceRemaining, long lastUpdateTimeMillis)
    {
      this.bucketSize           = bucketSize;
      this.leakRatePerMillis    = leakRatePerMillis;
      this.spaceRemaining       = spaceRemaining;
      this.lastUpdateTimeMillis = lastUpdateTimeMillis;
    }
  }
}
