/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.StoredVerificationCode;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.util.SystemMapper;

import java.io.IOException;
import java.util.Optional;

public class PendingDevicesManager {

  private final Logger logger = LoggerFactory.getLogger(PendingDevicesManager.class);

  private static final String CACHE_PREFIX = "pending_devices2::";

  private final PendingDevices            pendingDevices;
  private final FaultTolerantRedisCluster cacheCluster;
  private final ObjectMapper              mapper;

  public PendingDevicesManager(PendingDevices pendingDevices, FaultTolerantRedisCluster cacheCluster) {
    this.pendingDevices = pendingDevices;
    this.cacheCluster   = cacheCluster;
    this.mapper         = SystemMapper.getMapper();
  }

  public void store(String number, StoredVerificationCode code) {
    memcacheSet(number, code);
    pendingDevices.insert(number, code.getCode(), code.getTimestamp());
  }

  public void remove(String number) {
    memcacheDelete(number);
    pendingDevices.remove(number);
  }

  public Optional<StoredVerificationCode> getCodeForNumber(String number) {
    Optional<StoredVerificationCode> code = memcacheGet(number);

    if (!code.isPresent()) {
      code = pendingDevices.getCodeForNumber(number);
      code.ifPresent(storedVerificationCode -> memcacheSet(number, storedVerificationCode));
    }

    return code;
  }

  private void memcacheSet(String number, StoredVerificationCode code) {
    try {
      final String verificationCodeJson = mapper.writeValueAsString(code);

      cacheCluster.useCluster(connection -> connection.sync().set(CACHE_PREFIX + number, verificationCodeJson));
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private Optional<StoredVerificationCode> memcacheGet(String number) {
    try {
      final String json = cacheCluster.withCluster(connection -> connection.sync().get(CACHE_PREFIX + number));

      if (json == null) return Optional.empty();
      else              return Optional.of(mapper.readValue(json, StoredVerificationCode.class));
    } catch (IOException e) {
      logger.warn("Could not parse pending device stored verification json");
      return Optional.empty();
    }
  }

  private void memcacheDelete(String number) {
    cacheCluster.useCluster(connection -> connection.sync().del(CACHE_PREFIX + number));
  }

}
