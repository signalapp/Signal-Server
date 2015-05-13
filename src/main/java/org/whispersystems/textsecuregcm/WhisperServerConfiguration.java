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
import org.whispersystems.textsecuregcm.configuration.FederationConfiguration;
import org.whispersystems.textsecuregcm.configuration.GraphiteConfiguration;
import org.whispersystems.textsecuregcm.configuration.NexmoConfiguration;
import org.whispersystems.textsecuregcm.configuration.PushConfiguration;
import org.whispersystems.textsecuregcm.configuration.RateLimitsConfiguration;
import org.whispersystems.textsecuregcm.configuration.RedPhoneConfiguration;
import org.whispersystems.textsecuregcm.configuration.RedisConfiguration;
import org.whispersystems.textsecuregcm.configuration.S3Configuration;
import org.whispersystems.textsecuregcm.configuration.TestDeviceConfiguration;
import org.whispersystems.textsecuregcm.configuration.TwilioConfiguration;
import org.whispersystems.textsecuregcm.configuration.WebsocketConfiguration;

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

  @JsonProperty
  private NexmoConfiguration nexmo;

  @NotNull
  @Valid
  @JsonProperty
  private PushConfiguration push;

  @NotNull
  @Valid
  @JsonProperty
  private S3Configuration s3;

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
  @JsonProperty
  private FederationConfiguration federation = new FederationConfiguration();

  @Valid
  @NotNull
  @JsonProperty
  private DataSourceFactory database = new DataSourceFactory();

  @Valid
  @NotNull
  @JsonProperty
  private RateLimitsConfiguration limits = new RateLimitsConfiguration();

  @Valid
  @JsonProperty
  private GraphiteConfiguration graphite = new GraphiteConfiguration();

  @Valid
  @JsonProperty
  private WebsocketConfiguration websocket = new WebsocketConfiguration();

  @JsonProperty
  private RedPhoneConfiguration redphone = new RedPhoneConfiguration();

  @Valid
  @NotNull
  @JsonProperty
  private JerseyClientConfiguration httpClient = new JerseyClientConfiguration();


  public WebsocketConfiguration getWebsocketConfiguration() {
    return websocket;
  }

  public TwilioConfiguration getTwilioConfiguration() {
    return twilio;
  }

  public NexmoConfiguration getNexmoConfiguration() {
    return nexmo;
  }

  public PushConfiguration getPushConfiguration() {
    return push;
  }

  public JerseyClientConfiguration getJerseyClientConfiguration() {
    return httpClient;
  }

  public S3Configuration getS3Configuration() {
    return s3;
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

  public RateLimitsConfiguration getLimitsConfiguration() {
    return limits;
  }

  public FederationConfiguration getFederationConfiguration() {
    return federation;
  }

  public GraphiteConfiguration getGraphiteConfiguration() {
    return graphite;
  }

  public RedPhoneConfiguration getRedphoneConfiguration() {
    return redphone;
  }

  public Map<String, Integer> getTestDevices() {
    Map<String, Integer> results = new HashMap<>();

    for (TestDeviceConfiguration testDeviceConfiguration : testDevices) {
      results.put(testDeviceConfiguration.getNumber(),
                  testDeviceConfiguration.getCode());
    }

    return results;
  }
}
