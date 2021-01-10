/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.List;

public class DirectoryConfiguration {

  @JsonProperty
  @NotNull
  @Valid
  private RedisConfiguration redis;
    
  @JsonProperty
  @NotNull
  @Valid
  private SqsConfiguration sqs;
    
  @JsonProperty
  @NotNull
  @Valid
  private DirectoryClientConfiguration client;

  @JsonProperty
  @NotNull
  @Valid
  private List<DirectoryServerConfiguration> server;

  public RedisConfiguration getRedisConfiguration() {
    return redis;
  }

  public SqsConfiguration getSqsConfiguration() {
    return sqs;
  }

  public DirectoryClientConfiguration getDirectoryClientConfiguration() {
    return client;
  }

  public List<DirectoryServerConfiguration> getDirectoryServerConfiguration() {
    return server;
  }
}
