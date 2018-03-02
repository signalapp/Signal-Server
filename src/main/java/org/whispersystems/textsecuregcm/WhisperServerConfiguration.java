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
package org.whispersystems.textsecuregcm;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.whispersystems.textsecuregcm.configuration.ApnConfiguration;
import org.whispersystems.textsecuregcm.configuration.FederationConfiguration;
import org.whispersystems.textsecuregcm.configuration.GcmConfiguration;
import org.whispersystems.textsecuregcm.configuration.MaxDeviceConfiguration;
import org.whispersystems.textsecuregcm.configuration.ProfilesConfiguration;
import org.whispersystems.textsecuregcm.configuration.PushConfiguration;
import org.whispersystems.textsecuregcm.configuration.RateLimitsConfiguration;
import org.whispersystems.textsecuregcm.configuration.RedPhoneConfiguration;
import org.whispersystems.textsecuregcm.configuration.RedisConfiguration;
import org.whispersystems.textsecuregcm.configuration.AttachmentsConfiguration;
import org.whispersystems.textsecuregcm.configuration.TestDeviceConfiguration;
import org.whispersystems.textsecuregcm.configuration.TurnConfiguration;
import org.whispersystems.textsecuregcm.configuration.TwilioConfiguration;
import org.whispersystems.websocket.configuration.WebSocketConfiguration;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import io.dropwizard.Configuration;
import io.dropwizard.client.JerseyClientConfiguration;
import io.dropwizard.db.DataSourceFactory;

public class WhisperServerConfiguration extends Configuration {

  @NotNull
  @Valid
  @JsonProperty
  private TwilioConfiguration twilio;

  @NotNull
  @Valid
  @JsonProperty
  private PushConfiguration push;

  @NotNull
  @Valid
  @JsonProperty
  private AttachmentsConfiguration attachments;

  @NotNull
  @Valid
  @JsonProperty
  private ProfilesConfiguration profiles;

  @NotNull
  @Valid
  @JsonProperty
  private RedisConfiguration cache;

  @NotNull
  @Valid
  @JsonProperty
  private RedisConfiguration directory;

  @Valid
  @NotNull
  @JsonProperty
  private DataSourceFactory messageStore;

  @Valid
  @NotNull
  @JsonProperty
  private List<TestDeviceConfiguration> testDevices = new LinkedList<>();

  @Valid
  @NotNull
  @JsonProperty
  private List<MaxDeviceConfiguration> maxDevices = new LinkedList<>();

  @Valid
  @JsonProperty
  private FederationConfiguration federation = new FederationConfiguration();

  @Valid
  @NotNull
  @JsonProperty
  private DataSourceFactory database = new DataSourceFactory();

  @JsonProperty
  private DataSourceFactory read_database;

  @Valid
  @NotNull
  @JsonProperty
  private RateLimitsConfiguration limits = new RateLimitsConfiguration();

  @Valid
  @NotNull
  @JsonProperty
  private JerseyClientConfiguration httpClient = new JerseyClientConfiguration();

  @Valid
  @NotNull
  @JsonProperty
  private WebSocketConfiguration webSocket = new WebSocketConfiguration();

  @Valid
  @NotNull
  @JsonProperty
  private TurnConfiguration turn;

  @Valid
  @NotNull
  @JsonProperty
  private GcmConfiguration gcm;

  @Valid
  @NotNull
  @JsonProperty
  private ApnConfiguration apn;

  public WebSocketConfiguration getWebSocketConfiguration() {
    return webSocket;
  }

  public TwilioConfiguration getTwilioConfiguration() {
    return twilio;
  }

  public PushConfiguration getPushConfiguration() {
    return push;
  }

  public JerseyClientConfiguration getJerseyClientConfiguration() {
    return httpClient;
  }

  public AttachmentsConfiguration getAttachmentsConfiguration() {
    return attachments;
  }

  public RedisConfiguration getCacheConfiguration() {
    return cache;
  }

  public RedisConfiguration getDirectoryConfiguration() {
    return directory;
  }

  public DataSourceFactory getMessageStoreConfiguration() {
    return messageStore;
  }

  public DataSourceFactory getDataSourceFactory() {
    return database;
  }

  public DataSourceFactory getReadDataSourceFactory() {
    return read_database;
  }

  public RateLimitsConfiguration getLimitsConfiguration() {
    return limits;
  }

  public FederationConfiguration getFederationConfiguration() {
    return federation;
  }

  public TurnConfiguration getTurnConfiguration() {
    return turn;
  }

  public GcmConfiguration getGcmConfiguration() {
    return gcm;
  }

  public ApnConfiguration getApnConfiguration() {
    return apn;
  }

  public ProfilesConfiguration getProfilesConfiguration() {
    return profiles;
  }

  public Map<String, Integer> getTestDevices() {
    Map<String, Integer> results = new HashMap<>();

    for (TestDeviceConfiguration testDeviceConfiguration : testDevices) {
      results.put(testDeviceConfiguration.getNumber(),
                  testDeviceConfiguration.getCode());
    }

    return results;
  }

  public Map<String, Integer> getMaxDevices() {
    Map<String, Integer> results = new HashMap<>();

    for (MaxDeviceConfiguration maxDeviceConfiguration : maxDevices) {
      results.put(maxDeviceConfiguration.getNumber(),
                  maxDeviceConfiguration.getCount());
    }

    return results;
  }

}
