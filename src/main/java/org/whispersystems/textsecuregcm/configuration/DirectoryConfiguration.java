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
package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

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
  private DirectoryServerConfiguration server;

  public RedisConfiguration getRedisConfiguration() {
    return redis;
  }

  public SqsConfiguration getSqsConfiguration() {
    return sqs;
  }

  public DirectoryClientConfiguration getDirectoryClientConfiguration() {
    return client;
  }

  public DirectoryServerConfiguration getDirectoryServerConfiguration() {
    return server;
  }

}
