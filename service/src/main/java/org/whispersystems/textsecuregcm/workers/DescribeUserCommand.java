/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import static com.codahale.metrics.MetricRegistry.name;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jsr310.JSR310Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.dropwizard.Application;
import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.cli.EnvironmentCommand;
import io.dropwizard.jdbi3.JdbiFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.lettuce.core.resource.ClientResources;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.jdbi.v3.core.Jdbi;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.auth.AmbiguousIdentifier;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialGenerator;
import org.whispersystems.textsecuregcm.metrics.PushLatencyManager;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.securestorage.SecureStorageClient;
import org.whispersystems.textsecuregcm.sqs.DirectoryQueue;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Accounts;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.Device.DeviceCapabilities;
import org.whispersystems.textsecuregcm.storage.FaultTolerantDatabase;
import org.whispersystems.textsecuregcm.storage.KeysDynamoDb;
import org.whispersystems.textsecuregcm.storage.MessagesCache;
import org.whispersystems.textsecuregcm.storage.MessagesDynamoDb;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.storage.Profiles;
import org.whispersystems.textsecuregcm.storage.ProfilesManager;
import org.whispersystems.textsecuregcm.storage.ReservedUsernames;
import org.whispersystems.textsecuregcm.storage.Usernames;
import org.whispersystems.textsecuregcm.storage.UsernamesManager;

public class DescribeUserCommand extends EnvironmentCommand<WhisperServerConfiguration> {

  private static class AccountSummary {
    @JsonProperty
    private final UUID uuid;

    @JsonProperty
    private final String number;

    @JsonProperty
    private final String currentProfileVersion;

    @JsonProperty
    private final boolean discoverableByPhoneNumber;

    @JsonProperty
    private final List<DeviceSummary> devices;

    @JsonProperty
    private final boolean hasSecureStorageManifest;

    public AccountSummary(final Account account, final boolean hasSecureStorageManifest) {
      this.uuid = account.getUuid();
      this.number = account.getNumber();
      this.currentProfileVersion = account.getCurrentProfileVersion().orElse(null);
      this.discoverableByPhoneNumber = account.isDiscoverableByPhoneNumber();
      this.devices = account.getDevices().stream().map(DeviceSummary::new).collect(Collectors.toList());
      this.hasSecureStorageManifest = hasSecureStorageManifest;
    }
  }

  private static class DeviceSummary {
    @JsonProperty
    private final long id;

    @JsonProperty
    private final String name;

    @JsonProperty
    private final String gcmToken;

    @JsonProperty
    private final String apnToken;

    @JsonProperty
    private final String voipApnToken;

    @JsonProperty
    private final boolean fetchesMessages;

    @JsonProperty
    private final LocalDate created;

    @JsonProperty
    private final LocalDate lastSeen;

    @JsonProperty
    private final DeviceCapabilities capabilities;

    public DeviceSummary(final Device device) {
      this.id = device.getId();
      this.name = device.getName();
      this.gcmToken = device.getGcmId();
      this.apnToken = device.getApnId();
      this.voipApnToken = device.getVoipApnId();
      this.fetchesMessages = device.getFetchesMessages();
      this.created = LocalDate.ofInstant(Instant.ofEpochMilli(device.getCreated()), ZoneId.systemDefault());
      this.lastSeen = LocalDate.ofInstant(Instant.ofEpochMilli(device.getLastSeen()), ZoneId.systemDefault());

      this.capabilities = device.getCapabilities();
    }
  }

  public DescribeUserCommand() {
    super(new Application<>() {
      @Override
      public void run(WhisperServerConfiguration configuration, Environment environment) {
      }
    }, "describe-user", "Print a summary of a user's account for debugging purposes");
  }

  @Override
  public void configure(final Subparser subparser) {
    super.configure(subparser);

    subparser.addArgument("-u", "--user")
        .dest("user")
        .type(String.class)
        .required(true)
        .help("The user to describe");
  }

  @Override
  protected void run(Environment environment, Namespace namespace, WhisperServerConfiguration configuration) throws Exception {

    environment.getObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    JdbiFactory jdbiFactory = new JdbiFactory();
    Jdbi accountJdbi = jdbiFactory.build(environment, configuration.getAccountsDatabaseConfiguration(), "accountdb");
    FaultTolerantDatabase accountDatabase             = new FaultTolerantDatabase("account_database_delete_user", accountJdbi, configuration.getAccountsDatabaseConfiguration().getCircuitBreakerConfiguration());
    ClientResources redisClusterClientResources = ClientResources.builder().build();

    AmazonDynamoDBClientBuilder clientBuilder = AmazonDynamoDBClientBuilder
        .standard()
        .withRegion(configuration.getMessageDynamoDbConfiguration().getRegion())
        .withClientConfiguration(new ClientConfiguration().withClientExecutionTimeout(((int) configuration.getMessageDynamoDbConfiguration().getClientExecutionTimeout().toMillis()))
            .withRequestTimeout((int) configuration.getMessageDynamoDbConfiguration().getClientRequestTimeout().toMillis()))
        .withCredentials(InstanceProfileCredentialsProvider.getInstance());

    AmazonDynamoDBClientBuilder keysDynamoDbClientBuilder = AmazonDynamoDBClientBuilder
        .standard()
        .withRegion(configuration.getKeysDynamoDbConfiguration().getRegion())
        .withClientConfiguration(new ClientConfiguration().withClientExecutionTimeout(((int) configuration.getKeysDynamoDbConfiguration().getClientExecutionTimeout().toMillis()))
            .withRequestTimeout((int) configuration.getKeysDynamoDbConfiguration().getClientRequestTimeout().toMillis()))
        .withCredentials(InstanceProfileCredentialsProvider.getInstance());

    DynamoDB messageDynamoDb = new DynamoDB(clientBuilder.build());
    DynamoDB preKeysDynamoDb = new DynamoDB(keysDynamoDbClientBuilder.build());

    FaultTolerantRedisCluster cacheCluster = new FaultTolerantRedisCluster("main_cache_cluster", configuration.getCacheClusterConfiguration(), redisClusterClientResources);

    ExecutorService keyspaceNotificationDispatchExecutor = environment.lifecycle().executorService(name(getClass(), "keyspaceNotification-%d")).maxThreads(4).build();
    ExecutorService storageServiceExecutor = environment.lifecycle().executorService(name(getClass(), "storageService-%d")).maxThreads(8).minThreads(1).build();

    ExternalServiceCredentialGenerator storageCredentialsGenerator   = new ExternalServiceCredentialGenerator(configuration.getSecureStorageServiceConfiguration().getUserAuthenticationTokenSharedSecret(), new byte[0], false);

    SecureStorageClient storageClient = new SecureStorageClient(storageCredentialsGenerator, storageServiceExecutor, configuration.getSecureStorageServiceConfiguration());
    Accounts accounts = new Accounts(accountDatabase);
    Usernames usernames = new Usernames(accountDatabase);
    Profiles profiles = new Profiles(accountDatabase);
    ReservedUsernames reservedUsernames = new ReservedUsernames(accountDatabase);
    KeysDynamoDb keysDynamoDb = new KeysDynamoDb(preKeysDynamoDb, configuration.getKeysDynamoDbConfiguration().getTableName());
    MessagesDynamoDb messagesDynamoDb = new MessagesDynamoDb(messageDynamoDb, configuration.getMessageDynamoDbConfiguration().getTableName(), configuration.getMessageDynamoDbConfiguration().getTimeToLive());
    FaultTolerantRedisCluster messageInsertCacheCluster = new FaultTolerantRedisCluster("message_insert_cluster", configuration.getMessageCacheConfiguration().getRedisClusterConfiguration(), redisClusterClientResources);
    FaultTolerantRedisCluster messageReadDeleteCluster = new FaultTolerantRedisCluster("message_read_delete_cluster", configuration.getMessageCacheConfiguration().getRedisClusterConfiguration(), redisClusterClientResources);
    FaultTolerantRedisCluster metricsCluster = new FaultTolerantRedisCluster("metrics_cluster", configuration.getMetricsClusterConfiguration(), redisClusterClientResources);
    MessagesCache messagesCache = new MessagesCache(messageInsertCacheCluster, messageReadDeleteCluster, keyspaceNotificationDispatchExecutor);
    PushLatencyManager pushLatencyManager = new PushLatencyManager(metricsCluster);
    DirectoryQueue directoryQueue = new DirectoryQueue  (configuration.getDirectoryConfiguration().getSqsConfiguration());
    UsernamesManager usernamesManager = new UsernamesManager(usernames, reservedUsernames, cacheCluster);
    ProfilesManager profilesManager = new ProfilesManager(profiles, cacheCluster);
    MessagesManager messagesManager = new MessagesManager(messagesDynamoDb, messagesCache, pushLatencyManager);
    AccountsManager accountsManager = new AccountsManager(accounts, cacheCluster, directoryQueue, keysDynamoDb, messagesManager, usernamesManager, profilesManager);

    accountsManager.get(new AmbiguousIdentifier(namespace.getString("user"))).ifPresentOrElse(account -> {
          final boolean hasStoredManifest = storageClient.hasStoredData(account.getUuid()).join();

          try {
            final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory())
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

            System.out.println(objectMapper.writeValueAsString(new AccountSummary(account, hasStoredManifest)));
          } catch (final JsonProcessingException e) {
            // This should never happen if we're going straight to a string
            throw new RuntimeException(e);
          }
        },
        () -> System.err.println("User not found: " + namespace.getString("user")));
  }
}
