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

public class PendingAccountsManager {

  private final Logger logger = LoggerFactory.getLogger(PendingAccountsManager.class);

  private static final String CACHE_PREFIX = "pending_account2::";

  private final PendingAccounts           pendingAccounts;
  private final FaultTolerantRedisCluster cacheCluster;
  private final ObjectMapper              mapper;

  public PendingAccountsManager(PendingAccounts pendingAccounts, FaultTolerantRedisCluster cacheCluster)
  {
    this.pendingAccounts = pendingAccounts;
    this.cacheCluster    = cacheCluster;
    this.mapper          = SystemMapper.getMapper();
  }

  public void store(String number, StoredVerificationCode code) {
    memcacheSet(number, code);
    pendingAccounts.insert(number, code.getCode(), code.getTimestamp(), code.getPushCode());
  }

  public void remove(String number) {
    memcacheDelete(number);
    pendingAccounts.remove(number);
  }

  public Optional<StoredVerificationCode> getCodeForNumber(String number) {
    Optional<StoredVerificationCode> code = memcacheGet(number);

    if (!code.isPresent()) {
      code = pendingAccounts.getCodeForNumber(number);
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
      logger.warn("Error deserializing value...", e);
      return Optional.empty();
    }
  }

  private void memcacheDelete(String number) {
    cacheCluster.useCluster(connection -> connection.sync().del(CACHE_PREFIX + number));
  }
}
