/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.whispersystems.textsecuregcm.tests.util.JsonHelpers.asJson;
import static org.whispersystems.textsecuregcm.tests.util.JsonHelpers.jsonFixture;
import static org.whispersystems.textsecuregcm.util.MockUtils.exactly;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junitpioneer.jupiter.cartesian.ArgumentSets;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.mockito.ArgumentCaptor;
import org.signal.libsignal.protocol.SealedSenderMultiRecipientMessage;
import org.signal.libsignal.zkgroup.ServerSecretParams;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.UnidentifiedAccessUtil;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicInboundMessageByteLimitConfiguration;
import org.whispersystems.textsecuregcm.entities.AccountMismatchedDevices;
import org.whispersystems.textsecuregcm.entities.AccountStaleDevices;
import org.whispersystems.textsecuregcm.entities.IncomingMessage;
import org.whispersystems.textsecuregcm.entities.IncomingMessageList;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.entities.MismatchedDevices;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntity;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntityList;
import org.whispersystems.textsecuregcm.entities.SendMultiRecipientMessageResponse;
import org.whispersystems.textsecuregcm.entities.SpamReport;
import org.whispersystems.textsecuregcm.entities.StaleDevices;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.PniServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.limits.CardinalityEstimator;
import org.whispersystems.textsecuregcm.limits.MessageDeliveryLoopMonitor;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.mappers.RateLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.metrics.MessageMetrics;
import org.whispersystems.textsecuregcm.providers.MultiRecipientMessageProvider;
import org.whispersystems.textsecuregcm.push.MessageSender;
import org.whispersystems.textsecuregcm.push.PushNotificationManager;
import org.whispersystems.textsecuregcm.push.PushNotificationScheduler;
import org.whispersystems.textsecuregcm.push.ReceiptSender;
import org.whispersystems.textsecuregcm.spam.SpamChecker;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.ClientReleaseManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.storage.RemovedMessage;
import org.whispersystems.textsecuregcm.storage.ReportMessageManager;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.CompletableFutureTestUtil;
import org.whispersystems.textsecuregcm.util.HeaderUtils;
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.TestClock;
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import org.whispersystems.websocket.Stories;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

@ExtendWith(DropwizardExtensionsSupport.class)
class MessageControllerTest {

  private static final String SINGLE_DEVICE_RECIPIENT = "+14151111111";
  private static final UUID SINGLE_DEVICE_UUID = UUID.randomUUID();
  private static final AciServiceIdentifier SINGLE_DEVICE_ACI_ID = new AciServiceIdentifier(SINGLE_DEVICE_UUID);
  private static final UUID SINGLE_DEVICE_PNI = UUID.randomUUID();
  private static final PniServiceIdentifier SINGLE_DEVICE_PNI_ID = new PniServiceIdentifier(SINGLE_DEVICE_PNI);
  private static final byte SINGLE_DEVICE_ID1 = 1;
  private static final int SINGLE_DEVICE_REG_ID1 = 111;
  private static final int SINGLE_DEVICE_PNI_REG_ID1 = 1111;

  private static final String MULTI_DEVICE_RECIPIENT = "+14152222222";
  private static final UUID MULTI_DEVICE_UUID = UUID.randomUUID();
  private static final AciServiceIdentifier MULTI_DEVICE_ACI_ID = new AciServiceIdentifier(MULTI_DEVICE_UUID);
  private static final UUID MULTI_DEVICE_PNI = UUID.randomUUID();
  private static final PniServiceIdentifier MULTI_DEVICE_PNI_ID = new PniServiceIdentifier(MULTI_DEVICE_PNI);
  private static final byte MULTI_DEVICE_ID1 = 1;
  private static final byte MULTI_DEVICE_ID2 = 2;
  private static final byte MULTI_DEVICE_ID3 = 3;
  private static final int MULTI_DEVICE_REG_ID1 = 222;
  private static final int MULTI_DEVICE_REG_ID2 = 333;
  private static final int MULTI_DEVICE_REG_ID3 = 444;
  private static final int MULTI_DEVICE_PNI_REG_ID1 = 2222;
  private static final int MULTI_DEVICE_PNI_REG_ID2 = 3333;
  private static final int MULTI_DEVICE_PNI_REG_ID3 = 4444;

  private static final UUID NONEXISTENT_UUID = UUID.randomUUID();
  private static final AciServiceIdentifier NONEXISTENT_ACI_ID = new AciServiceIdentifier(NONEXISTENT_UUID);
  private static final PniServiceIdentifier NONEXISTENT_PNI_ID = new PniServiceIdentifier(NONEXISTENT_UUID);

  private static final byte[] UNIDENTIFIED_ACCESS_BYTES = "0123456789abcdef".getBytes();

  private static final String INTERNATIONAL_RECIPIENT = "+61123456789";
  private static final UUID INTERNATIONAL_UUID = UUID.randomUUID();

  @SuppressWarnings("unchecked")
  private static final RedisAdvancedClusterCommands<String, String> redisCommands  = mock(RedisAdvancedClusterCommands.class);

  private static final MessageSender messageSender = mock(MessageSender.class);
  private static final ReceiptSender receiptSender = mock(ReceiptSender.class);
  private static final AccountsManager accountsManager = mock(AccountsManager.class);
  private static final MessagesManager messagesManager = mock(MessagesManager.class);
  private static final RateLimiters rateLimiters = mock(RateLimiters.class);
  private static final CardinalityEstimator cardinalityEstimator = mock(CardinalityEstimator.class);
  private static final RateLimiter rateLimiter = mock(RateLimiter.class);
  private static final PushNotificationManager pushNotificationManager = mock(PushNotificationManager.class);
  private static final PushNotificationScheduler pushNotificationScheduler = mock(PushNotificationScheduler.class);
  private static final ReportMessageManager reportMessageManager = mock(ReportMessageManager.class);
  private static final ExecutorService multiRecipientMessageExecutor = MoreExecutors.newDirectExecutorService();
  private static final Scheduler messageDeliveryScheduler = Schedulers.newBoundedElastic(10, 10_000, "messageDelivery");

  @SuppressWarnings("unchecked")
  private static final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager =
      mock(DynamicConfigurationManager.class);

  private static final ServerSecretParams serverSecretParams = ServerSecretParams.generate();

  private static final TestClock clock = TestClock.now();

  private static final ResourceExtension resources = ResourceExtension.builder()
      .addProperty(ServerProperties.UNWRAP_COMPLETION_STAGE_IN_WRITER_ENABLE, Boolean.TRUE)
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(new AuthValueFactoryProvider.Binder<>(AuthenticatedDevice.class))
      .addProvider(RateLimitExceededExceptionMapper.class)
      .addProvider(MultiRecipientMessageProvider.class)
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(
          new MessageController(rateLimiters, cardinalityEstimator, messageSender, receiptSender, accountsManager,
              messagesManager, pushNotificationManager, pushNotificationScheduler, reportMessageManager, multiRecipientMessageExecutor,
              messageDeliveryScheduler, mock(ClientReleaseManager.class), dynamicConfigurationManager,
              serverSecretParams, SpamChecker.noop(), new MessageMetrics(), mock(MessageDeliveryLoopMonitor.class),
              clock))
      .build();

  @BeforeEach
  void setup() {
    reset(pushNotificationScheduler);

    final List<Device> singleDeviceList = List.of(
        generateTestDevice(SINGLE_DEVICE_ID1, SINGLE_DEVICE_REG_ID1, SINGLE_DEVICE_PNI_REG_ID1, true)
    );

    final List<Device> multiDeviceList = List.of(
        generateTestDevice(MULTI_DEVICE_ID1, MULTI_DEVICE_REG_ID1, MULTI_DEVICE_PNI_REG_ID1, true),
        generateTestDevice(MULTI_DEVICE_ID2, MULTI_DEVICE_REG_ID2, MULTI_DEVICE_PNI_REG_ID2, true),
        generateTestDevice(MULTI_DEVICE_ID3, MULTI_DEVICE_REG_ID3, MULTI_DEVICE_PNI_REG_ID3, false)
    );

    Account singleDeviceAccount  = AccountsHelper.generateTestAccount(SINGLE_DEVICE_RECIPIENT, SINGLE_DEVICE_UUID, SINGLE_DEVICE_PNI, singleDeviceList, UNIDENTIFIED_ACCESS_BYTES);
    Account multiDeviceAccount   = AccountsHelper.generateTestAccount(MULTI_DEVICE_RECIPIENT, MULTI_DEVICE_UUID, MULTI_DEVICE_PNI, multiDeviceList, UNIDENTIFIED_ACCESS_BYTES);
    Account internationalAccount = AccountsHelper.generateTestAccount(INTERNATIONAL_RECIPIENT, INTERNATIONAL_UUID,
        UUID.randomUUID(), singleDeviceList, UNIDENTIFIED_ACCESS_BYTES);

    when(accountsManager.getByServiceIdentifier(SINGLE_DEVICE_ACI_ID)).thenReturn(Optional.of(singleDeviceAccount));
    when(accountsManager.getByServiceIdentifier(SINGLE_DEVICE_PNI_ID)).thenReturn(Optional.of(singleDeviceAccount));
    when(accountsManager.getByServiceIdentifier(MULTI_DEVICE_ACI_ID)).thenReturn(Optional.of(multiDeviceAccount));
    when(accountsManager.getByServiceIdentifier(MULTI_DEVICE_PNI_ID)).thenReturn(Optional.of(multiDeviceAccount));
    when(accountsManager.getByServiceIdentifier(new AciServiceIdentifier(INTERNATIONAL_UUID))).thenReturn(Optional.of(internationalAccount));
    when(accountsManager.getByServiceIdentifier(NONEXISTENT_ACI_ID)).thenReturn(Optional.empty());
    when(accountsManager.getByServiceIdentifier(NONEXISTENT_PNI_ID)).thenReturn(Optional.empty());

    when(accountsManager.getByServiceIdentifierAsync(any())).thenReturn(CompletableFuture.completedFuture(Optional.empty()));
    when(accountsManager.getByServiceIdentifierAsync(SINGLE_DEVICE_ACI_ID)).thenReturn(CompletableFuture.completedFuture(Optional.of(singleDeviceAccount)));
    when(accountsManager.getByServiceIdentifierAsync(SINGLE_DEVICE_PNI_ID)).thenReturn(CompletableFuture.completedFuture(Optional.of(singleDeviceAccount)));
    when(accountsManager.getByServiceIdentifierAsync(MULTI_DEVICE_ACI_ID)).thenReturn(CompletableFuture.completedFuture(Optional.of(multiDeviceAccount)));
    when(accountsManager.getByServiceIdentifierAsync(MULTI_DEVICE_PNI_ID)).thenReturn(CompletableFuture.completedFuture(Optional.of(multiDeviceAccount)));
    when(accountsManager.getByServiceIdentifierAsync(new AciServiceIdentifier(INTERNATIONAL_UUID))).thenReturn(CompletableFuture.completedFuture(Optional.of(internationalAccount)));

    final DynamicInboundMessageByteLimitConfiguration inboundMessageByteLimitConfiguration =
        mock(DynamicInboundMessageByteLimitConfiguration.class);

    when(inboundMessageByteLimitConfiguration.enforceInboundLimit()).thenReturn(false);

    final DynamicConfiguration dynamicConfiguration = mock(DynamicConfiguration.class);
    when(dynamicConfiguration.getInboundMessageByteLimitConfiguration()).thenReturn(inboundMessageByteLimitConfiguration);

    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);

    when(rateLimiters.getMessagesLimiter()).thenReturn(rateLimiter);
    when(rateLimiters.getStoriesLimiter()).thenReturn(rateLimiter);
    when(rateLimiters.getInboundMessageBytes()).thenReturn(rateLimiter);

    when(rateLimiter.validateAsync(any(UUID.class))).thenReturn(CompletableFuture.completedFuture(null));

    clock.unpin();
  }

  private static Device generateTestDevice(final byte id, final int registrationId, final int pniRegistrationId,
      final boolean enabled) {
    final Device device = new Device();
    device.setId(id);
    device.setRegistrationId(registrationId);
    device.setPhoneNumberIdentityRegistrationId(pniRegistrationId);
    device.setFetchesMessages(enabled);

    return device;
  }

  @AfterEach
  void teardown() {
    reset(
        redisCommands,
        messageSender,
        receiptSender,
        accountsManager,
        messagesManager,
        rateLimiters,
        rateLimiter,
        cardinalityEstimator,
        pushNotificationManager,
        reportMessageManager
    );
  }

  @AfterAll
  static void teardownAll() {
    messageDeliveryScheduler.dispose();
  }

  @Test
  void testSingleDeviceCurrent() throws Exception {
    try (final Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/%s", SINGLE_DEVICE_UUID))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(SystemMapper.jsonMapper().readValue(jsonFixture("fixtures/current_message_single_device.json"),
                    IncomingMessageList.class),
                MediaType.APPLICATION_JSON_TYPE))) {

      assertThat("Good Response", response.getStatus(), is(equalTo(200)));

      ArgumentCaptor<Envelope> captor = ArgumentCaptor.forClass(Envelope.class);
      verify(messageSender, times(1)).sendMessage(any(Account.class), any(Device.class), captor.capture(), eq(false));

      assertTrue(captor.getValue().hasSourceServiceId());
      assertTrue(captor.getValue().hasSourceDevice());
      assertTrue(captor.getValue().getUrgent());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testSingleDeviceSync(final boolean sendToPni) throws Exception {
    final ServiceIdentifier serviceIdentifier = sendToPni
        ? new PniServiceIdentifier(AuthHelper.VALID_PNI_3)
        : new AciServiceIdentifier(AuthHelper.VALID_UUID_3);

    try (final Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/%s", serviceIdentifier.toServiceIdentifierString()))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_3, AuthHelper.VALID_PASSWORD_3_PRIMARY))
            .put(Entity.entity(SystemMapper.jsonMapper().readValue(jsonFixture("fixtures/current_message_sync.json"),
                    IncomingMessageList.class),
                MediaType.APPLICATION_JSON_TYPE))) {

      assertThat(response.getStatus(), is(equalTo(sendToPni ? 403 : 200)));
    }
  }

  @Test
  void testSingleDeviceCurrentNotUrgent() throws Exception {
    try (final Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/%s", SINGLE_DEVICE_UUID))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(SystemMapper.jsonMapper().readValue(jsonFixture("fixtures/current_message_single_device_not_urgent.json"),
                    IncomingMessageList.class),
                MediaType.APPLICATION_JSON_TYPE))) {

      assertThat("Good Response", response.getStatus(), is(equalTo(200)));

      ArgumentCaptor<Envelope> captor = ArgumentCaptor.forClass(Envelope.class);
      verify(messageSender, times(1)).sendMessage(any(Account.class), any(Device.class), captor.capture(), eq(false));

      assertTrue(captor.getValue().hasSourceServiceId());
      assertTrue(captor.getValue().hasSourceDevice());
      assertFalse(captor.getValue().getUrgent());
    }
  }

  @Test
  void testSingleDeviceCurrentByPni() throws Exception {
    try (final Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/PNI:%s", SINGLE_DEVICE_PNI))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(SystemMapper.jsonMapper().readValue(jsonFixture("fixtures/current_message_single_device.json"),
                    IncomingMessageList.class),
                MediaType.APPLICATION_JSON_TYPE))) {

      assertThat("Good Response", response.getStatus(), is(equalTo(200)));

      ArgumentCaptor<Envelope> captor = ArgumentCaptor.forClass(Envelope.class);
      verify(messageSender, times(1)).sendMessage(any(Account.class), any(Device.class), captor.capture(), eq(false));

      assertTrue(captor.getValue().hasSourceServiceId());
      assertTrue(captor.getValue().hasSourceDevice());
    }
  }

  @Test
  void testNullMessageInList() throws Exception {
    try (final Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/%s", SINGLE_DEVICE_UUID))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(SystemMapper.jsonMapper().readValue(jsonFixture("fixtures/current_message_null_message_in_list.json"), IncomingMessageList.class),
                MediaType.APPLICATION_JSON_TYPE))) {

      assertThat("Bad request", response.getStatus(), is(equalTo(422)));
    }
  }

  @Test
  void testSingleDeviceCurrentUnidentified() throws Exception {
    try (final Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/%s", SINGLE_DEVICE_UUID))
            .request()
            .header(HeaderUtils.UNIDENTIFIED_ACCESS_KEY, Base64.getEncoder().encodeToString(UNIDENTIFIED_ACCESS_BYTES))
            .put(Entity.entity(SystemMapper.jsonMapper().readValue(jsonFixture("fixtures/current_message_single_device.json"),
                    IncomingMessageList.class),
                MediaType.APPLICATION_JSON_TYPE))) {

      assertThat("Good Response", response.getStatus(), is(equalTo(200)));

      ArgumentCaptor<Envelope> captor = ArgumentCaptor.forClass(Envelope.class);
      verify(messageSender, times(1)).sendMessage(any(Account.class), any(Device.class), captor.capture(), eq(false));

      assertFalse(captor.getValue().hasSourceServiceId());
      assertFalse(captor.getValue().hasSourceDevice());
    }
  }

  @ParameterizedTest
  @MethodSource
  void testSingleDeviceCurrentGroupSendEndorsement(
      ServiceIdentifier recipient, ServiceIdentifier authorizedRecipient,
      Duration timeLeft, boolean includeUak, boolean story, int expectedResponse) throws Exception {
    final Instant expiration = Instant.now().truncatedTo(ChronoUnit.DAYS); // expiration times must be UTC midnight or libsignal will reject the endorsement
    clock.pin(expiration.minus(timeLeft));

    Invocation.Builder builder =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/%s", recipient.toServiceIdentifierString()))
            .queryParam("story", story)
            .request()
            .header(HeaderUtils.GROUP_SEND_TOKEN,
                AuthHelper.validGroupSendTokenHeader(serverSecretParams, List.of(authorizedRecipient), expiration));

    if (includeUak) {
      builder = builder.header(HeaderUtils.UNIDENTIFIED_ACCESS_KEY, Base64.getEncoder().encodeToString(UNIDENTIFIED_ACCESS_BYTES));
    }

    try (final Response response = builder
        .put(Entity.entity(
                SystemMapper.jsonMapper().readValue(jsonFixture("fixtures/current_message_single_device.json"),
                    IncomingMessageList.class),
                MediaType.APPLICATION_JSON_TYPE))) {

      assertThat("Good Response", response.getStatus(), is(equalTo(expectedResponse)));
      if (expectedResponse == 200) {
        verify(messageSender).sendMessage(
            any(Account.class), any(Device.class), argThat(env -> !env.hasSourceServiceId() && !env.hasSourceDevice()),
            eq(false));
      } else {
        verifyNoMoreInteractions(messageSender);
      }
    }
  }

  private static Stream<Arguments> testSingleDeviceCurrentGroupSendEndorsement() {
    return Stream.of(
        // valid endorsement
        Arguments.of(SINGLE_DEVICE_ACI_ID, SINGLE_DEVICE_ACI_ID, Duration.ofHours(1), false, false, 200),

        // expired endorsement, not authorized
        Arguments.of(SINGLE_DEVICE_ACI_ID, SINGLE_DEVICE_ACI_ID, Duration.ofHours(-1), false, false, 401),

        // endorsement for the wrong recipient, not authorized
        Arguments.of(SINGLE_DEVICE_ACI_ID, NONEXISTENT_ACI_ID, Duration.ofHours(1), false, false, 401),

        // expired endorsement for the wrong recipient, not authorized
        Arguments.of(SINGLE_DEVICE_ACI_ID, NONEXISTENT_ACI_ID, Duration.ofHours(-1), false, false, 401),

        // valid endorsement for the right recipient but they aren't registered, not found
        Arguments.of(NONEXISTENT_ACI_ID, NONEXISTENT_ACI_ID, Duration.ofHours(1), false, false, 404),

        // expired endorsement for the right recipient but they aren't registered, not authorized (NOT not found)
        Arguments.of(NONEXISTENT_ACI_ID, NONEXISTENT_ACI_ID, Duration.ofHours(-1), false, false, 401),

        // valid endorsement but also a UAK, bad request
        Arguments.of(SINGLE_DEVICE_ACI_ID, SINGLE_DEVICE_ACI_ID, Duration.ofHours(1), true, false, 400),

        // valid endorsement on a story, bad request
        Arguments.of(SINGLE_DEVICE_ACI_ID, SINGLE_DEVICE_ACI_ID, Duration.ofHours(1), false, true, 400),

        // valid endorsement on a story with a UAK, bad request
        Arguments.of(SINGLE_DEVICE_ACI_ID, SINGLE_DEVICE_ACI_ID, Duration.ofHours(1), true, true, 400));
  }

  @Test
  void testSendBadAuth() throws Exception {
    try (final Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/%s", SINGLE_DEVICE_UUID))
            .request()
            .put(Entity.entity(SystemMapper.jsonMapper().readValue(jsonFixture("fixtures/current_message_single_device.json"),
                    IncomingMessageList.class),
                MediaType.APPLICATION_JSON_TYPE))) {

      assertThat("Good Response", response.getStatus(), is(equalTo(401)));
    }
  }

  @Test
  void testMultiDeviceMissing() throws Exception {
    try (final Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/%s", MULTI_DEVICE_UUID))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(SystemMapper.jsonMapper().readValue(jsonFixture("fixtures/current_message_single_device.json"),
                    IncomingMessageList.class),
                MediaType.APPLICATION_JSON_TYPE))) {

      assertThat("Good Response Code", response.getStatus(), is(equalTo(409)));

      assertThat("Good Response Body",
          asJson(response.readEntity(MismatchedDevices.class)),
          is(equalTo(jsonFixture("fixtures/missing_device_response.json"))));

      verifyNoMoreInteractions(messageSender);
    }
  }

  @Test
  void testMultiDeviceExtra() throws Exception {
    try (final Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/%s", MULTI_DEVICE_UUID))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(SystemMapper.jsonMapper().readValue(jsonFixture("fixtures/current_message_extra_device.json"),
                    IncomingMessageList.class),
                MediaType.APPLICATION_JSON_TYPE))) {

      assertThat("Good Response Code", response.getStatus(), is(equalTo(409)));

      assertThat("Good Response Body",
          asJson(response.readEntity(MismatchedDevices.class)),
          is(equalTo(jsonFixture("fixtures/missing_device_response2.json"))));

      verifyNoMoreInteractions(messageSender);
    }
  }

  @Test
  void testMultiDeviceDuplicate() throws Exception {
    try (final Response response = resources.getJerseyTest()
        .target(String.format("/v1/messages/%s", MULTI_DEVICE_UUID))
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .put(Entity.entity(SystemMapper.jsonMapper().readValue(jsonFixture("fixtures/current_message_duplicate_device.json"),
                    IncomingMessageList.class),
                MediaType.APPLICATION_JSON_TYPE))) {

      assertThat("Good Response Code", response.getStatus(), is(equalTo(422)));

      verifyNoMoreInteractions(messageSender);
    }
  }

  @Test
  void testMultiDevice() throws Exception {
    try (final Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/%s", MULTI_DEVICE_UUID))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(SystemMapper.jsonMapper().readValue(jsonFixture("fixtures/current_message_multi_device.json"),
                    IncomingMessageList.class),
                MediaType.APPLICATION_JSON_TYPE))) {

      assertThat("Good Response Code", response.getStatus(), is(equalTo(200)));

      final ArgumentCaptor<Envelope> envelopeCaptor = ArgumentCaptor.forClass(Envelope.class);

      verify(messageSender, times(3))
          .sendMessage(any(Account.class), any(Device.class), envelopeCaptor.capture(), eq(false));

      envelopeCaptor.getAllValues().forEach(envelope -> assertTrue(envelope.getUrgent()));
    }
  }

  @Test
  void testMultiDeviceNotUrgent() throws Exception {
    try (final Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/%s", MULTI_DEVICE_UUID))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(SystemMapper.jsonMapper().readValue(jsonFixture("fixtures/current_message_multi_device_not_urgent.json"),
                    IncomingMessageList.class),
                MediaType.APPLICATION_JSON_TYPE))) {

      assertThat("Good Response Code", response.getStatus(), is(equalTo(200)));

      final ArgumentCaptor<Envelope> envelopeCaptor = ArgumentCaptor.forClass(Envelope.class);

      verify(messageSender, times(3))
          .sendMessage(any(Account.class), any(Device.class), envelopeCaptor.capture(), eq(false));

      envelopeCaptor.getAllValues().forEach(envelope -> assertFalse(envelope.getUrgent()));
    }
  }

  @Test
  void testMultiDeviceByPni() throws Exception {
    try (final Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/PNI:%s", MULTI_DEVICE_PNI))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(SystemMapper.jsonMapper().readValue(jsonFixture("fixtures/current_message_multi_device_pni.json"),
                    IncomingMessageList.class),
                MediaType.APPLICATION_JSON_TYPE))) {

      assertThat("Good Response Code", response.getStatus(), is(equalTo(200)));

      verify(messageSender, times(3))
          .sendMessage(any(Account.class), any(Device.class), any(Envelope.class), eq(false));
    }
  }

  @Test
  void testRegistrationIdMismatch() throws Exception {
    try (final Response response =
        resources.getJerseyTest().target(String.format("/v1/messages/%s", MULTI_DEVICE_UUID))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(SystemMapper.jsonMapper().readValue(jsonFixture("fixtures/current_message_registration_id.json"),
                    IncomingMessageList.class),
                MediaType.APPLICATION_JSON_TYPE))) {

      assertThat("Good Response Code", response.getStatus(), is(equalTo(410)));

      assertThat("Good Response Body",
          asJson(response.readEntity(StaleDevices.class)),
          is(equalTo(jsonFixture("fixtures/mismatched_registration_id.json"))));

      verifyNoMoreInteractions(messageSender);
    }
  }

  @ParameterizedTest
  @CsvSource({
      "false, false",
      "false, true",
      "true, false",
      "true, true"
  })
  void testGetMessages(final boolean receiveStories, final boolean hasMore) {

    final long timestampOne = 313377;
    final long timestampTwo = 313388;

    final UUID messageGuidOne = UUID.randomUUID();
    final UUID messageGuidTwo = UUID.randomUUID();
    final UUID sourceUuid = UUID.randomUUID();

    final UUID updatedPniOne = UUID.randomUUID();

    List<Envelope> envelopes = List.of(
        generateEnvelope(messageGuidOne, Envelope.Type.CIPHERTEXT_VALUE, timestampOne, sourceUuid, (byte) 2,
            AuthHelper.VALID_UUID, updatedPniOne, "hi there".getBytes(), 0, false),
        generateEnvelope(messageGuidTwo, Envelope.Type.SERVER_DELIVERY_RECEIPT_VALUE, timestampTwo, sourceUuid,
            (byte) 2,
            AuthHelper.VALID_UUID, null, null, 0, true)
    );

    when(messagesManager.getMessagesForDevice(eq(AuthHelper.VALID_UUID), eq(AuthHelper.VALID_DEVICE), anyBoolean()))
        .thenReturn(Mono.just(new Pair<>(envelopes, hasMore)));

    final String userAgent = "Test-UA";

    OutgoingMessageEntityList response =
        resources.getJerseyTest().target("/v1/messages/")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .header(Stories.X_SIGNAL_RECEIVE_STORIES, receiveStories ? "true" : "false")
            .header(HttpHeaders.USER_AGENT, userAgent)
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .get(OutgoingMessageEntityList.class);

    List<OutgoingMessageEntity> messages = response.messages();
    int expectedSize = receiveStories ? 2 : 1;
    assertEquals(expectedSize, messages.size());

    OutgoingMessageEntity first = messages.getFirst();
    assertEquals(first.timestamp(), timestampOne);
    assertEquals(first.guid(), messageGuidOne);
    assertNotNull(first.sourceUuid());
    assertEquals(first.sourceUuid().uuid(), sourceUuid);
    assertEquals(updatedPniOne, first.updatedPni());

    if (receiveStories) {
      OutgoingMessageEntity second = messages.get(1);
      assertEquals(second.timestamp(), timestampTwo);
      assertEquals(second.guid(), messageGuidTwo);
      assertNotNull(second.sourceUuid());
      assertEquals(second.sourceUuid().uuid(), sourceUuid);
      assertNull(second.updatedPni());
    }

    verify(pushNotificationManager).handleMessagesRetrieved(AuthHelper.VALID_ACCOUNT, AuthHelper.VALID_DEVICE, userAgent);

    if (hasMore) {
      verify(pushNotificationScheduler).scheduleDelayedNotification(eq(AuthHelper.VALID_ACCOUNT), eq(AuthHelper.VALID_DEVICE), any());
    } else {
      verify(pushNotificationScheduler, never()).scheduleDelayedNotification(any(), any(), any());
    }
  }

  @Test
  void testGetMessagesBadAuth() {
    final long timestampOne = 313377;
    final long timestampTwo = 313388;

    final List<Envelope> messages = List.of(
        generateEnvelope(UUID.randomUUID(), Envelope.Type.CIPHERTEXT_VALUE, timestampOne, UUID.randomUUID(), (byte) 2,
            AuthHelper.VALID_UUID, null, "hi there".getBytes(), 0),
        generateEnvelope(UUID.randomUUID(), Envelope.Type.SERVER_DELIVERY_RECEIPT_VALUE, timestampTwo,
            UUID.randomUUID(), (byte) 2, AuthHelper.VALID_UUID, null, null, 0)
    );

    when(messagesManager.getMessagesForDevice(eq(AuthHelper.VALID_UUID), eq(AuthHelper.VALID_DEVICE), anyBoolean()))
        .thenReturn(Mono.just(new Pair<>(messages, false)));

    Response response =
        resources.getJerseyTest().target("/v1/messages/")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.INVALID_PASSWORD))
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .get();

    assertThat("Unauthorized response", response.getStatus(), is(equalTo(401)));
  }

  @Test
  void testDeleteMessages() {
    long clientTimestamp = System.currentTimeMillis();

    UUID sourceUuid = UUID.randomUUID();

    UUID uuid1 = UUID.randomUUID();

    final long serverTimestamp = 0;
    when(messagesManager.delete(AuthHelper.VALID_UUID, AuthHelper.VALID_DEVICE, uuid1, null))
        .thenReturn(
            CompletableFutureTestUtil.almostCompletedFuture(Optional.of(
                new RemovedMessage(Optional.of(new AciServiceIdentifier(sourceUuid)),
                    new AciServiceIdentifier(AuthHelper.VALID_UUID), uuid1, serverTimestamp, clientTimestamp,
                    Envelope.Type.CIPHERTEXT))));

    UUID uuid2 = UUID.randomUUID();
    when(messagesManager.delete(AuthHelper.VALID_UUID, AuthHelper.VALID_DEVICE, uuid2, null))
        .thenReturn(
            CompletableFutureTestUtil.almostCompletedFuture(Optional.of(
                new RemovedMessage(Optional.of(new AciServiceIdentifier(sourceUuid)),
                    new AciServiceIdentifier(AuthHelper.VALID_UUID), uuid2, serverTimestamp, clientTimestamp,
                    Envelope.Type.SERVER_DELIVERY_RECEIPT))));

    UUID uuid3 = UUID.randomUUID();
    when(messagesManager.delete(AuthHelper.VALID_UUID, AuthHelper.VALID_DEVICE, uuid3, null))
        .thenReturn(CompletableFutureTestUtil.almostCompletedFuture(Optional.empty()));

    UUID uuid4 = UUID.randomUUID();
    when(messagesManager.delete(AuthHelper.VALID_UUID, AuthHelper.VALID_DEVICE, uuid4, null))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Oh No")));

    try (final Response response = resources.getJerseyTest()
        .target(String.format("/v1/messages/uuid/%s", uuid1))
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .delete()) {

      assertThat("Good Response Code", response.getStatus(), is(equalTo(204)));
      verify(receiptSender).sendReceipt(eq(new AciServiceIdentifier(AuthHelper.VALID_UUID)), eq((byte) 1),
          eq(new AciServiceIdentifier(sourceUuid)), eq(clientTimestamp));
    }

    try (final Response response = resources.getJerseyTest()
        .target(String.format("/v1/messages/uuid/%s", uuid2))
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .delete()) {

      assertThat("Good Response Code", response.getStatus(), is(equalTo(204)));
      verifyNoMoreInteractions(receiptSender);
    }

    try (final Response response = resources.getJerseyTest()
        .target(String.format("/v1/messages/uuid/%s", uuid3))
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .delete()) {

      assertThat("Good Response Code", response.getStatus(), is(equalTo(204)));
      verifyNoMoreInteractions(receiptSender);
    }

    try (final Response response = resources.getJerseyTest()
        .target(String.format("/v1/messages/uuid/%s", uuid4))
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .delete()) {

      assertThat("Bad Response Code", response.getStatus(), is(equalTo(500)));
      verifyNoMoreInteractions(receiptSender);
    }
  }

  @Test
  void testReportMessageByE164() {

    final String senderNumber = "+12125550001";
    final UUID senderAci = UUID.randomUUID();
    final UUID senderPni = UUID.randomUUID();
    final String userAgent = "user-agent";
    UUID messageGuid = UUID.randomUUID();

    final Account account = mock(Account.class);
    when(account.getUuid()).thenReturn(senderAci);
    when(account.getNumber()).thenReturn(senderNumber);
    when(account.getPhoneNumberIdentifier()).thenReturn(senderPni);

    when(accountsManager.getByE164(senderNumber)).thenReturn(Optional.of(account));
    when(accountsManager.findRecentlyDeletedAccountIdentifier(senderNumber)).thenReturn(Optional.of(senderAci));
    when(accountsManager.getPhoneNumberIdentifier(senderNumber)).thenReturn(senderPni);

    try (final Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/report/%s/%s", senderNumber, messageGuid))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .header(HttpHeaders.USER_AGENT, userAgent)
            .post(null)) {

      assertThat(response.getStatus(), is(equalTo(202)));

      verify(reportMessageManager).report(Optional.of(senderNumber), Optional.of(senderAci), Optional.of(senderPni),
          messageGuid, AuthHelper.VALID_UUID, Optional.empty(), userAgent);
      verify(accountsManager, never()).findRecentlyDeletedE164(any(UUID.class));
      verify(accountsManager, never()).getPhoneNumberIdentifier(anyString());
    }

    when(accountsManager.getByE164(senderNumber)).thenReturn(Optional.empty());
    messageGuid = UUID.randomUUID();

    try (final Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/report/%s/%s", senderNumber, messageGuid))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .header(HttpHeaders.USER_AGENT, userAgent)
            .post(null)) {

      assertThat(response.getStatus(), is(equalTo(202)));

      verify(reportMessageManager).report(Optional.of(senderNumber), Optional.of(senderAci), Optional.of(senderPni),
          messageGuid, AuthHelper.VALID_UUID, Optional.empty(), userAgent);
    }
  }

  @Test
  void testReportMessageByAci() {

    final String senderNumber = "+12125550001";
    final UUID senderAci = UUID.randomUUID();
    final UUID senderPni = UUID.randomUUID();
    final String userAgent = "user-agent";
    UUID messageGuid = UUID.randomUUID();

    final Account account = mock(Account.class);
    when(account.getUuid()).thenReturn(senderAci);
    when(account.getNumber()).thenReturn(senderNumber);
    when(account.getPhoneNumberIdentifier()).thenReturn(senderPni);

    when(accountsManager.getByAccountIdentifier(senderAci)).thenReturn(Optional.of(account));
    when(accountsManager.findRecentlyDeletedE164(senderAci)).thenReturn(Optional.of(senderNumber));
    when(accountsManager.getPhoneNumberIdentifier(senderNumber)).thenReturn(senderPni);

    try (final Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/report/%s/%s", senderAci, messageGuid))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .header(HttpHeaders.USER_AGENT, userAgent)
            .post(null)) {

      assertThat(response.getStatus(), is(equalTo(202)));

      verify(reportMessageManager).report(Optional.of(senderNumber), Optional.of(senderAci), Optional.of(senderPni),
          messageGuid, AuthHelper.VALID_UUID, Optional.empty(), userAgent);
      verify(accountsManager, never()).findRecentlyDeletedE164(any(UUID.class));
      verify(accountsManager, never()).getPhoneNumberIdentifier(anyString());
    }

    when(accountsManager.getByAccountIdentifier(senderAci)).thenReturn(Optional.empty());

    messageGuid = UUID.randomUUID();

    try (final Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/report/%s/%s", senderAci, messageGuid))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .header(HttpHeaders.USER_AGENT, userAgent)
            .post(null)) {

      assertThat(response.getStatus(), is(equalTo(202)));

      verify(reportMessageManager).report(Optional.of(senderNumber), Optional.of(senderAci), Optional.of(senderPni),
          messageGuid, AuthHelper.VALID_UUID, Optional.empty(), userAgent);
    }
  }

  @Test
  void testReportMessageByAciWithSpamReportToken() {

    final String senderNumber = "+12125550001";
    final UUID senderAci = UUID.randomUUID();
    final UUID senderPni = UUID.randomUUID();
    UUID messageGuid = UUID.randomUUID();

    final Account account = mock(Account.class);
    when(account.getUuid()).thenReturn(senderAci);
    when(account.getNumber()).thenReturn(senderNumber);
    when(account.getPhoneNumberIdentifier()).thenReturn(senderPni);

    when(accountsManager.getByAccountIdentifier(senderAci)).thenReturn(Optional.of(account));
    when(accountsManager.findRecentlyDeletedE164(senderAci)).thenReturn(Optional.of(senderNumber));
    when(accountsManager.getPhoneNumberIdentifier(senderNumber)).thenReturn(senderPni);

    Entity<SpamReport> entity = Entity.entity(new SpamReport(new byte[3]), "application/json");

    try (final Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/report/%s/%s", senderAci, messageGuid))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .post(entity)) {

      assertThat(response.getStatus(), is(equalTo(202)));
      verify(reportMessageManager).report(eq(Optional.of(senderNumber)),
          eq(Optional.of(senderAci)),
          eq(Optional.of(senderPni)),
          eq(messageGuid),
          eq(AuthHelper.VALID_UUID),
          argThat(maybeBytes -> maybeBytes.map(bytes -> Arrays.equals(bytes, new byte[3])).orElse(false)),
          any());
      verify(accountsManager, never()).findRecentlyDeletedE164(any(UUID.class));
      verify(accountsManager, never()).getPhoneNumberIdentifier(anyString());
    }

    when(accountsManager.getByAccountIdentifier(senderAci)).thenReturn(Optional.empty());

    messageGuid = UUID.randomUUID();

    entity = Entity.entity(new SpamReport(new byte[5]), "application/json");

    try (final Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/report/%s/%s", senderAci, messageGuid))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .post(entity)) {

      assertThat(response.getStatus(), is(equalTo(202)));
      verify(reportMessageManager).report(eq(Optional.of(senderNumber)),
          eq(Optional.of(senderAci)),
          eq(Optional.of(senderPni)),
          eq(messageGuid),
          eq(AuthHelper.VALID_UUID),
          argThat(maybeBytes -> maybeBytes.map(bytes -> Arrays.equals(bytes, new byte[5])).orElse(false)),
          any());
    }
  }

  @ParameterizedTest
  @MethodSource
  void testReportMessageByAciWithNullSpamReportToken(Entity<?> entity, boolean expectOk) {

    final String senderNumber = "+12125550001";
    final UUID senderAci = UUID.randomUUID();
    final UUID senderPni = UUID.randomUUID();
    UUID messageGuid = UUID.randomUUID();

    final Account account = mock(Account.class);
    when(account.getUuid()).thenReturn(senderAci);
    when(account.getNumber()).thenReturn(senderNumber);
    when(account.getPhoneNumberIdentifier()).thenReturn(senderPni);

    when(accountsManager.getByAccountIdentifier(senderAci)).thenReturn(Optional.of(account));
    when(accountsManager.findRecentlyDeletedE164(senderAci)).thenReturn(Optional.of(senderNumber));
    when(accountsManager.getPhoneNumberIdentifier(senderNumber)).thenReturn(senderPni);

    try (final Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/report/%s/%s", senderAci, messageGuid))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .post(entity)) {

      Matcher<Integer> matcher = expectOk ? is(equalTo(202)) : not(equalTo(202));
      assertThat(response.getStatus(), matcher);
    }
  }

  private static Stream<Arguments> testReportMessageByAciWithNullSpamReportToken() {
    return Stream.of(
        Arguments.of(Entity.json(new SpamReport(new byte[5])), true),
        Arguments.of(Entity.json("{\"token\":\"AAAAAAA\"}"), true),
        Arguments.of(Entity.json(new SpamReport(new byte[0])), true),
        Arguments.of(Entity.json(new SpamReport(null)), true),
        Arguments.of(Entity.json("{\"token\": \"\"}"), true),
        Arguments.of(Entity.json("{\"token\": null}"), true),
        Arguments.of(Entity.json("null"), true),
        Arguments.of(Entity.json("{\"weird\": 123}"), true),
        Arguments.of(Entity.json("\"weirder\""), false),
        Arguments.of(Entity.json("weirdest"), false),
        Arguments.of(Entity.json("{\"token\":\"InvalidBase64[][][][]\"}"), false)
    );
  }

  @Test
  void testValidateContentLength() throws Exception {
    final int contentLength = Math.toIntExact(MessageController.MAX_MESSAGE_SIZE + 1);
    final byte[] contentBytes = new byte[contentLength];
    Arrays.fill(contentBytes, (byte) 1);

    try (final Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/%s", SINGLE_DEVICE_UUID))
            .request()
            .header(HeaderUtils.UNIDENTIFIED_ACCESS_KEY, Base64.getEncoder().encodeToString(UNIDENTIFIED_ACCESS_BYTES))
            .put(Entity.entity(new IncomingMessageList(
                    List.of(new IncomingMessage(1, (byte) 1, 1, new String(contentBytes))), false, true,
                    System.currentTimeMillis()),
                MediaType.APPLICATION_JSON_TYPE))) {

      assertThat("Bad response", response.getStatus(), is(equalTo(413)));

      verify(messageSender, never()).sendMessage(any(Account.class), any(Device.class), any(Envelope.class),
          anyBoolean());
    }
  }

  @ParameterizedTest
  @MethodSource
  void testValidateEnvelopeType(String payloadFilename, boolean expectOk) throws Exception {
    try (final Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/%s", SINGLE_DEVICE_UUID))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .header(HttpHeaders.USER_AGENT, "Test-UA")
            .put(Entity.entity(SystemMapper.jsonMapper().readValue(jsonFixture(payloadFilename), IncomingMessageList.class),
                MediaType.APPLICATION_JSON_TYPE))) {

      if (expectOk) {
        assertEquals(200, response.getStatus());

        final ArgumentCaptor<Envelope> captor = ArgumentCaptor.forClass(Envelope.class);
        verify(messageSender).sendMessage(any(Account.class), any(Device.class), captor.capture(), eq(false));
      } else {
        assertEquals(400, response.getStatus());
        verify(messageSender, never()).sendMessage(any(), any(), any(), anyBoolean());
      }
    }
  }

  private static Stream<Arguments> testValidateEnvelopeType() {
    return Stream.of(
        Arguments.of("fixtures/current_message_single_device.json", true),
        Arguments.of("fixtures/current_message_single_device_server_receipt_type.json", false)
    );
  }

  private record Recipient(ServiceIdentifier uuid,
                           Byte[] deviceId,
                           Integer[] registrationId,
                           byte[] perRecipientKeyMaterial) {

    Recipient(ServiceIdentifier uuid,
        byte deviceId,
        int registrationId,
        byte[] perRecipientKeyMaterial) {
      this(uuid, new Byte[]{deviceId}, new Integer[]{registrationId}, perRecipientKeyMaterial);
    }
  }

  private static void writeMultiPayloadRecipient(final ByteBuffer bb, final Recipient r,
      final boolean useExplicitIdentifier) {
    if (useExplicitIdentifier) {
      bb.put(r.uuid().toFixedWidthByteArray());
    } else {
      bb.put(UUIDUtil.toBytes(r.uuid().uuid()));
    }

    assert (r.deviceId.length == r.registrationId.length);

    for (int i = 0; i < r.deviceId.length; i++) {
      final int hasMore = i == r.deviceId.length - 1 ? 0 : 0x8000;
      bb.put(r.deviceId()[i]); // device id (1 byte)
      bb.putShort((short) (r.registrationId()[i] | hasMore)); // registration id (2 bytes)
    }
    bb.put(r.perRecipientKeyMaterial()); // key material (48 bytes)
  }

  private static InputStream initializeMultiPayload(final List<Recipient> recipients, final byte[] buffer, final boolean explicitIdentifiers) {
    return initializeMultiPayload(recipients, buffer, explicitIdentifiers, 39);
  }

  private static InputStream initializeMultiPayload(final List<Recipient> recipients, final byte[] buffer, final boolean explicitIdentifiers, final int payloadSize) {
    // initialize a binary payload according to our wire format
    ByteBuffer bb = ByteBuffer.wrap(buffer);
    bb.order(ByteOrder.BIG_ENDIAN);

    // first write the header
    bb.put(explicitIdentifiers ? (byte) 0x23 : (byte) 0x22);  // version byte

    // count varint
    writeVarint(bb, recipients.size());

    recipients.forEach(recipient -> writeMultiPayloadRecipient(bb, recipient, explicitIdentifiers));

    // now write the actual message body (empty for now)
    assert(payloadSize >= 32);
    writeVarint(bb, payloadSize);
    bb.put(new byte[payloadSize]);

    // return the input stream
    return new ByteArrayInputStream(buffer, 0, bb.position());
  }

  private static void writeVarint(ByteBuffer bb, long n) {
    while (n >= 0x80) {
      bb.put ((byte) (n & 0x7F | 0x80));
      n = n >> 7;
    }
    bb.put((byte) (n & 0x7F));
  }

  @Test
  void testManyRecipientMessage() throws Exception {

    when(messagesManager.insertSharedMultiRecipientMessagePayload(any(SealedSenderMultiRecipientMessage.class)))
        .thenReturn(new byte[]{1});

    final int nRecipients = 999;
    final int devicesPerRecipient = 5;
    final List<Recipient> recipients = new ArrayList<>();

    for (int i = 0; i < nRecipients; i++) {
      final List<Device> devices =
          IntStream.range(1, devicesPerRecipient + 1)
          .mapToObj(
              d -> generateTestDevice(
                  (byte) d, 100 + d, 10 * d, true))
          .collect(Collectors.toList());
      final UUID aci = new UUID(0L, i);
      final UUID pni = new UUID(1L, i);
      final String e164 = String.format("+1408555%04d", i);
      final Account account = AccountsHelper.generateTestAccount(e164, aci, pni, devices, UNIDENTIFIED_ACCESS_BYTES);

      when(accountsManager.getByServiceIdentifierAsync(new AciServiceIdentifier(aci)))
          .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

      when(accountsManager.getByServiceIdentifierAsync(new PniServiceIdentifier(pni)))
          .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

      devices.forEach(d -> recipients.add(new Recipient(new AciServiceIdentifier(aci), d.getId(), d.getRegistrationId(), new byte[48])));
    }

    byte[] buffer = new byte[1048576];
    InputStream stream = initializeMultiPayload(recipients, buffer, true);
    Entity<InputStream> entity = Entity.entity(stream, MultiRecipientMessageProvider.MEDIA_TYPE);
    try (final Response response = resources
        .getJerseyTest()
        .target("/v1/messages/multi_recipient")
        .queryParam("online", true)
        .queryParam("story", true)
        .queryParam("urgent", false)
        .request()
        .header(HttpHeaders.USER_AGENT, "test")
        .put(entity)) {

      assertThat(response.readEntity(String.class), response.getStatus(), is(equalTo(200)));
      verify(messageSender, times(nRecipients * devicesPerRecipient)).sendMessage(any(), any(), any(), eq(true));
    }
  }

  // see testMultiRecipientMessageNoPni and testMultiRecipientMessagePni below for actual invocations
  private void testMultiRecipientMessage(
      Map<ServiceIdentifier, Map<Byte, Integer>> destinations, boolean authorize, boolean isStory, boolean urgent,
      boolean explicitIdentifier, int expectedStatus, int expectedMessagesSent) throws Exception {

    when(messagesManager.insertSharedMultiRecipientMessagePayload(any(SealedSenderMultiRecipientMessage.class)))
        .thenReturn(new byte[]{1});

    final List<Recipient> recipients = new ArrayList<>();
    destinations.forEach(
        (serviceIdentifier, deviceToRegistrationId) ->
            deviceToRegistrationId.forEach(
                (deviceId, registrationId) ->
                    recipients.add(new Recipient(serviceIdentifier, deviceId, registrationId, new byte[48]))));

    // initialize our binary payload and create an input stream
    byte[] buffer = new byte[2048];
    InputStream stream = initializeMultiPayload(recipients, buffer, explicitIdentifier);

    // set up the entity to use in our PUT request
    Entity<InputStream> entity = Entity.entity(stream, MultiRecipientMessageProvider.MEDIA_TYPE);

    // build correct or incorrect access header
    final String accessHeader;
    if (authorize) {
      final long count = destinations.keySet().stream().map(accountsManager::getByServiceIdentifier).filter(Optional::isPresent).count();
      accessHeader = Base64.getEncoder().encodeToString(count % 2 == 1 ? UNIDENTIFIED_ACCESS_BYTES : new byte[16]);
    } else {
      accessHeader = "BBBBBBBBBBBBBBBBBBBBBB==";
    }

    // make the PUT request
    try (final Response response = resources
        .getJerseyTest()
        .target("/v1/messages/multi_recipient")
        .queryParam("online", true)
        .queryParam("ts", 1663798405641L)
        .queryParam("story", isStory)
        .queryParam("urgent", urgent)
        .request()
        .header(HttpHeaders.USER_AGENT, "test")
        .header(HeaderUtils.UNIDENTIFIED_ACCESS_KEY, accessHeader)
        .put(entity)) {

      assertThat("Unexpected response", response.getStatus(), is(equalTo(expectedStatus)));
      verify(messageSender,
          exactly(expectedMessagesSent))
          .sendMessage(
              any(),
              any(),
              argThat(env -> env.getUrgent() == urgent && !env.hasSourceServiceId() && !env.hasSourceDevice()),
              eq(true));
      if (expectedStatus == 200) {
        SendMultiRecipientMessageResponse smrmr = response.readEntity(SendMultiRecipientMessageResponse.class);
        assertThat(smrmr.uuids404(), is(empty()));
      }
    }
  }

  @SafeVarargs
  private static <K, V> Map<K, V> submap(Map<K, V> map, K... keys) {
    return Arrays.stream(keys).collect(Collectors.toMap(Function.identity(), map::get));
  }

  private static Map<ServiceIdentifier, Map<Byte, Integer>> multiRecipientTargetMap() {
    return Map.of(
        SINGLE_DEVICE_ACI_ID, Map.of(SINGLE_DEVICE_ID1, SINGLE_DEVICE_REG_ID1),
        SINGLE_DEVICE_PNI_ID, Map.of(SINGLE_DEVICE_ID1, SINGLE_DEVICE_PNI_REG_ID1),
        MULTI_DEVICE_ACI_ID,
        Map.of(
            MULTI_DEVICE_ID1, MULTI_DEVICE_REG_ID1,
            MULTI_DEVICE_ID2, MULTI_DEVICE_REG_ID2,
            MULTI_DEVICE_ID3, MULTI_DEVICE_REG_ID3),
        MULTI_DEVICE_PNI_ID,
        Map.of(
            MULTI_DEVICE_ID1, MULTI_DEVICE_PNI_REG_ID1,
            MULTI_DEVICE_ID2, MULTI_DEVICE_PNI_REG_ID2,
            MULTI_DEVICE_ID3, MULTI_DEVICE_PNI_REG_ID3),
        NONEXISTENT_ACI_ID, Map.of(SINGLE_DEVICE_ID1, SINGLE_DEVICE_REG_ID1),
        NONEXISTENT_PNI_ID, Map.of(SINGLE_DEVICE_ID1, SINGLE_DEVICE_PNI_REG_ID1)
    );
  }

  private record MultiRecipientMessageTestCase(
      Map<ServiceIdentifier, Map<Byte, Integer>> destinations,
      boolean authenticated,
      boolean story,
      int expectedStatus,
      int expectedSentMessages) {
  }

  @CartesianTest
  @CartesianTest.MethodFactory("testMultiRecipientMessageNoPni")
  void testMultiRecipientMessageNoPni(MultiRecipientMessageTestCase testCase, boolean urgent , boolean explicitIdentifier) throws Exception {
    testMultiRecipientMessage(testCase.destinations(), testCase.authenticated(), testCase.story(), urgent, explicitIdentifier, testCase.expectedStatus(), testCase.expectedSentMessages());
  }

  @SuppressWarnings("unused")
  private static ArgumentSets testMultiRecipientMessageNoPni() {
    final Map<ServiceIdentifier, Map<Byte, Integer>> targets = multiRecipientTargetMap();
    final Map<ServiceIdentifier, Map<Byte, Integer>> singleDeviceAci = submap(targets, SINGLE_DEVICE_ACI_ID);
    final Map<ServiceIdentifier, Map<Byte, Integer>> multiDeviceAci = submap(targets, MULTI_DEVICE_ACI_ID);
    final Map<ServiceIdentifier, Map<Byte, Integer>> bothAccountsAci =
        submap(targets, SINGLE_DEVICE_ACI_ID, MULTI_DEVICE_ACI_ID);
    final Map<ServiceIdentifier, Map<Byte, Integer>> realAndFakeAci =
        submap(
            targets,
            SINGLE_DEVICE_ACI_ID,
            MULTI_DEVICE_ACI_ID,
            NONEXISTENT_ACI_ID);

    final boolean auth = true;
    final boolean unauth = false;
    final boolean story = true;
    final boolean notStory = false;

    return ArgumentSets
        .argumentsForFirstParameter(
            new MultiRecipientMessageTestCase(singleDeviceAci, unauth, story, 200, 1),
            new MultiRecipientMessageTestCase(multiDeviceAci, unauth, story, 200, 3),
            new MultiRecipientMessageTestCase(bothAccountsAci, unauth, story, 200, 4),
            new MultiRecipientMessageTestCase(realAndFakeAci, unauth, story, 200, 4),

            new MultiRecipientMessageTestCase(singleDeviceAci, unauth, notStory, 401, 0),
            new MultiRecipientMessageTestCase(multiDeviceAci, unauth, notStory, 401, 0),
            new MultiRecipientMessageTestCase(bothAccountsAci, unauth, notStory, 401, 0),
            new MultiRecipientMessageTestCase(realAndFakeAci, unauth, notStory, 404, 0),

            new MultiRecipientMessageTestCase(singleDeviceAci, auth, story, 200, 1),
            new MultiRecipientMessageTestCase(multiDeviceAci, auth, story, 200, 3),
            new MultiRecipientMessageTestCase(bothAccountsAci, auth, story, 200, 4),
            new MultiRecipientMessageTestCase(realAndFakeAci, auth, story, 200, 4),

            new MultiRecipientMessageTestCase(singleDeviceAci, auth, notStory, 200, 1),
            new MultiRecipientMessageTestCase(multiDeviceAci, auth, notStory, 200, 3),
            new MultiRecipientMessageTestCase(bothAccountsAci, auth, notStory, 200, 4),
            new MultiRecipientMessageTestCase(realAndFakeAci, auth, notStory, 404, 0))
        .argumentsForNextParameter(false, true) // urgent
        .argumentsForNextParameter(false, true); // explicitIdentifiers
  }

  @CartesianTest
  @CartesianTest.MethodFactory("testMultiRecipientMessagePni")
  void testMultiRecipientMessagePni(MultiRecipientMessageTestCase testCase, boolean urgent) throws Exception {
    testMultiRecipientMessage(testCase.destinations(), testCase.authenticated(), testCase.story(), urgent, true, testCase.expectedStatus(), testCase.expectedSentMessages());
  }

  @SuppressWarnings("unused")
  private static ArgumentSets testMultiRecipientMessagePni() {
    final Map<ServiceIdentifier, Map<Byte, Integer>> targets = multiRecipientTargetMap();
    final Map<ServiceIdentifier, Map<Byte, Integer>> singleDevicePni = submap(targets, SINGLE_DEVICE_PNI_ID);
    final Map<ServiceIdentifier, Map<Byte, Integer>> singleDeviceAciAndPni = submap(
        targets, SINGLE_DEVICE_ACI_ID, SINGLE_DEVICE_PNI_ID);
    final Map<ServiceIdentifier, Map<Byte, Integer>> multiDevicePni = submap(targets, MULTI_DEVICE_PNI_ID);
    final Map<ServiceIdentifier, Map<Byte, Integer>> bothAccountsMixed =
        submap(targets, SINGLE_DEVICE_ACI_ID, MULTI_DEVICE_PNI_ID);
    final Map<ServiceIdentifier, Map<Byte, Integer>> realAndFakeMixed =
        submap(
            targets,
            SINGLE_DEVICE_PNI_ID,
            MULTI_DEVICE_ACI_ID,
            NONEXISTENT_PNI_ID);

    final boolean auth = true;
    final boolean unauth = false;
    final boolean story = true;
    final boolean notStory = false;

    return ArgumentSets
        .argumentsForFirstParameter(
            new MultiRecipientMessageTestCase(singleDevicePni, unauth, story, 200, 1),
            new MultiRecipientMessageTestCase(singleDeviceAciAndPni, unauth, story, 200, 2),
            new MultiRecipientMessageTestCase(multiDevicePni, unauth, story, 200, 3),
            new MultiRecipientMessageTestCase(bothAccountsMixed, unauth, story, 200, 4),
            new MultiRecipientMessageTestCase(realAndFakeMixed, unauth, story, 200, 4),

            new MultiRecipientMessageTestCase(singleDevicePni, unauth, notStory, 401, 0),
            new MultiRecipientMessageTestCase(singleDeviceAciAndPni, unauth, notStory, 401, 0),
            new MultiRecipientMessageTestCase(multiDevicePni, unauth, notStory, 401, 0),
            new MultiRecipientMessageTestCase(bothAccountsMixed, unauth, notStory, 401, 0),
            new MultiRecipientMessageTestCase(realAndFakeMixed, unauth, notStory, 404, 0),

            new MultiRecipientMessageTestCase(singleDevicePni, auth, story, 200, 1),
            new MultiRecipientMessageTestCase(singleDeviceAciAndPni, auth, story, 200, 2),
            new MultiRecipientMessageTestCase(multiDevicePni, auth, story, 200, 3),
            new MultiRecipientMessageTestCase(bothAccountsMixed, auth, story, 200, 4),
            new MultiRecipientMessageTestCase(realAndFakeMixed, auth, story, 200, 4),

            new MultiRecipientMessageTestCase(singleDevicePni, auth, notStory, 401, 0),
            new MultiRecipientMessageTestCase(singleDeviceAciAndPni, auth, notStory, 401, 0),
            new MultiRecipientMessageTestCase(multiDevicePni, auth, notStory, 401, 0),
            new MultiRecipientMessageTestCase(bothAccountsMixed, auth, notStory, 401, 0),
            new MultiRecipientMessageTestCase(realAndFakeMixed, auth, notStory, 404, 0))
        .argumentsForNextParameter(false, true); // urgent
  }

  @Test
  void testMultiRecipientMessageWithGroupSendEndorsements() throws Exception {

    when(messagesManager.insertSharedMultiRecipientMessagePayload(any(SealedSenderMultiRecipientMessage.class)))
        .thenReturn(new byte[]{1});

    final List<Recipient> recipients = List.of(
        new Recipient(SINGLE_DEVICE_ACI_ID, SINGLE_DEVICE_ID1, SINGLE_DEVICE_REG_ID1, new byte[48]),
        new Recipient(MULTI_DEVICE_ACI_ID, MULTI_DEVICE_ID1, MULTI_DEVICE_REG_ID1, new byte[48]),
        new Recipient(MULTI_DEVICE_ACI_ID, MULTI_DEVICE_ID2, MULTI_DEVICE_REG_ID2, new byte[48]),
        new Recipient(MULTI_DEVICE_ACI_ID, MULTI_DEVICE_ID3, MULTI_DEVICE_REG_ID3, new byte[48]));

    // initialize our binary payload and create an input stream
    byte[] buffer = new byte[2048];
    InputStream stream = initializeMultiPayload(recipients, buffer, true);

    clock.pin(Instant.parse("2024-04-09T12:00:00.00Z"));

    try (final Response response =  resources
        .getJerseyTest()
        .target("/v1/messages/multi_recipient")
        .queryParam("online", true)
        .queryParam("ts", 1663798405641L)
        .queryParam("story", false)
        .queryParam("urgent", false)
        .request()
        .header(HttpHeaders.USER_AGENT, "test")
        .header(HeaderUtils.GROUP_SEND_TOKEN, AuthHelper.validGroupSendTokenHeader(
                serverSecretParams, List.of(SINGLE_DEVICE_ACI_ID, MULTI_DEVICE_ACI_ID), Instant.parse("2024-04-10T00:00:00.00Z")))
        .put(Entity.entity(stream, MultiRecipientMessageProvider.MEDIA_TYPE))) {

      assertThat("Unexpected response", response.getStatus(), is(equalTo(200)));
      verify(messageSender,
          exactly(4))
          .sendMessage(
              any(),
              any(),
              argThat(env -> !env.hasSourceServiceId() && !env.hasSourceDevice()),
              eq(true));
      SendMultiRecipientMessageResponse smrmr = response.readEntity(SendMultiRecipientMessageResponse.class);
      assertThat(smrmr.uuids404(), is(empty()));
    }
  }

  @Test
  void testMultiRecipientMessageWithInvalidGroupSendEndorsements() throws Exception {
    final List<Recipient> recipients = List.of(
        new Recipient(NONEXISTENT_ACI_ID, SINGLE_DEVICE_ID1, SINGLE_DEVICE_REG_ID1, new byte[48]),
        new Recipient(MULTI_DEVICE_ACI_ID, MULTI_DEVICE_ID1, MULTI_DEVICE_REG_ID1, new byte[48]),
        new Recipient(MULTI_DEVICE_ACI_ID, MULTI_DEVICE_ID2, MULTI_DEVICE_REG_ID2, new byte[48]));

    // initialize our binary payload and create an input stream
    byte[] buffer = new byte[2048];
    InputStream stream = initializeMultiPayload(recipients, buffer, true);

    clock.pin(Instant.parse("2024-04-09T12:00:00.00Z"));

    try (final Response response = resources
        .getJerseyTest()
        .target("/v1/messages/multi_recipient")
        .queryParam("online", true)
        .queryParam("ts", 1663798405641L)
        .queryParam("story", false)
        .queryParam("urgent", false)
        .request()
        .header(HttpHeaders.USER_AGENT, "test")
        .header(HeaderUtils.GROUP_SEND_TOKEN, AuthHelper.validGroupSendTokenHeader(
                serverSecretParams, List.of(MULTI_DEVICE_ACI_ID), Instant.parse("2024-04-10T00:00:00.00Z")))
        .put(Entity.entity(stream, MultiRecipientMessageProvider.MEDIA_TYPE))) {

      assertThat("Unexpected response", response.getStatus(), is(equalTo(401)));
      verifyNoMoreInteractions(messageSender);
    }
  }

  @Test
  void testMultiRecipientMessageWithExpiredGroupSendEndorsements() throws Exception {
    final List<Recipient> recipients = List.of(
        new Recipient(SINGLE_DEVICE_ACI_ID, SINGLE_DEVICE_ID1, SINGLE_DEVICE_REG_ID1, new byte[48]),
        new Recipient(MULTI_DEVICE_ACI_ID, MULTI_DEVICE_ID1, MULTI_DEVICE_REG_ID1, new byte[48]),
        new Recipient(MULTI_DEVICE_ACI_ID, MULTI_DEVICE_ID2, MULTI_DEVICE_REG_ID2, new byte[48]));

    // initialize our binary payload and create an input stream
    byte[] buffer = new byte[2048];
    InputStream stream = initializeMultiPayload(recipients, buffer, true);

    clock.pin(Instant.parse("2024-04-10T12:00:00.00Z"));

    try (final Response response = resources
        .getJerseyTest()
        .target("/v1/messages/multi_recipient")
        .queryParam("online", true)
        .queryParam("ts", 1663798405641L)
        .queryParam("story", false)
        .queryParam("urgent", false)
        .request()
        .header(HttpHeaders.USER_AGENT, "test")
        .header(HeaderUtils.GROUP_SEND_TOKEN, AuthHelper.validGroupSendTokenHeader(
                serverSecretParams, List.of(SINGLE_DEVICE_ACI_ID, MULTI_DEVICE_ACI_ID), Instant.parse("2024-04-10T00:00:00.00Z")))
        .put(Entity.entity(stream, MultiRecipientMessageProvider.MEDIA_TYPE))) {

      assertThat("Unexpected response", response.getStatus(), is(equalTo(401)));
      verifyNoMoreInteractions(messageSender);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testMultiRecipientRedisBombProtection(final boolean useExplicitIdentifier) throws Exception {
    final List<Recipient> recipients = List.of(
        new Recipient(MULTI_DEVICE_ACI_ID, MULTI_DEVICE_ID1, MULTI_DEVICE_REG_ID1, new byte[48]),
        new Recipient(MULTI_DEVICE_ACI_ID, MULTI_DEVICE_ID2, MULTI_DEVICE_REG_ID2, new byte[48]),
        new Recipient(MULTI_DEVICE_ACI_ID, MULTI_DEVICE_ID1, MULTI_DEVICE_REG_ID1, new byte[48]));

    Response response = resources
        .getJerseyTest()
        .target("/v1/messages/multi_recipient")
        .queryParam("online", true)
        .queryParam("ts", 1663798405641L)
        .queryParam("story", false)
        .queryParam("urgent", false)
        .request()
        .header(HttpHeaders.USER_AGENT, "cluck cluck, i'm a parrot")
        .header(HeaderUtils.UNIDENTIFIED_ACCESS_KEY, Base64.getEncoder().encodeToString(UNIDENTIFIED_ACCESS_BYTES))
        .put(Entity.entity(initializeMultiPayload(recipients, new byte[2048], useExplicitIdentifier), MultiRecipientMessageProvider.MEDIA_TYPE));

    checkBadMultiRecipientResponse(response, 400);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testMultiRecipientSizeLimit() throws Exception {
    final List<Recipient> recipients = List.of(
        new Recipient(SINGLE_DEVICE_ACI_ID, SINGLE_DEVICE_ID1, SINGLE_DEVICE_REG_ID1, new byte[48]));

    Response response = resources
        .getJerseyTest()
        .target("/v1/messages/multi_recipient")
        .queryParam("online", true)
        .queryParam("ts", 1663798405641L)
        .queryParam("story", false)
        .queryParam("urgent", false)
        .request()
        .header(HttpHeaders.USER_AGENT, "cluck cluck, i'm a parrot")
        .header(HeaderUtils.UNIDENTIFIED_ACCESS_KEY, Base64.getEncoder().encodeToString(UNIDENTIFIED_ACCESS_BYTES))
        .put(Entity.entity(initializeMultiPayload(recipients, new byte[257<<10], true, 256<<10), MultiRecipientMessageProvider.MEDIA_TYPE));

    checkBadMultiRecipientResponse(response, 400);
  }

  @Test
  void testSendStoryToUnknownAccount() throws Exception {
    String accessBytes = Base64.getEncoder().encodeToString(UNIDENTIFIED_ACCESS_BYTES);
    String json = jsonFixture("fixtures/current_message_single_device.json");
    UUID unknownUUID = UUID.randomUUID();
    IncomingMessageList list = SystemMapper.jsonMapper().readValue(json, IncomingMessageList.class);

    try (final Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/%s", unknownUUID))
            .queryParam("story", "true")
            .request()
            .header(HeaderUtils.UNIDENTIFIED_ACCESS_KEY, accessBytes)
            .put(Entity.entity(list, MediaType.APPLICATION_JSON_TYPE))) {

      assertThat("200 masks unknown recipient", response.getStatus(), is(equalTo(200)));
    }
  }

  @ParameterizedTest
  @MethodSource
  void testSendMultiRecipientMessageToUnknownAccounts(boolean story, boolean known, boolean useExplicitIdentifier) {

    when(messagesManager.insertSharedMultiRecipientMessagePayload(any(SealedSenderMultiRecipientMessage.class)))
        .thenReturn(new byte[]{1});

    final Recipient r1;
    if (known) {
      r1 = new Recipient(SINGLE_DEVICE_ACI_ID, SINGLE_DEVICE_ID1, SINGLE_DEVICE_REG_ID1, new byte[48]);
    } else {
      r1 = new Recipient(new AciServiceIdentifier(UUID.randomUUID()), (byte) 99, 999, new byte[48]);
    }

    Recipient r2 = new Recipient(MULTI_DEVICE_ACI_ID, MULTI_DEVICE_ID1, MULTI_DEVICE_REG_ID1, new byte[48]);
    Recipient r3 = new Recipient(MULTI_DEVICE_ACI_ID, MULTI_DEVICE_ID2, MULTI_DEVICE_REG_ID2, new byte[48]);
    Recipient r4 = new Recipient(MULTI_DEVICE_ACI_ID, MULTI_DEVICE_ID3, MULTI_DEVICE_REG_ID3, new byte[48]);

    List<Recipient> recipients = List.of(r1, r2, r3, r4);

    byte[] buffer = new byte[2048];
    InputStream stream = initializeMultiPayload(recipients, buffer, useExplicitIdentifier);
    // set up the entity to use in our PUT request
    Entity<InputStream> entity = Entity.entity(stream, MultiRecipientMessageProvider.MEDIA_TYPE);

    // This looks weird, but there is a method to the madness.
    // new bytes[16] is equivalent to UNIDENTIFIED_ACCESS_BYTES ^ UNIDENTIFIED_ACCESS_BYTES
    // (i.e. we need to XOR all the access keys together)
    String accessBytes = Base64.getEncoder().encodeToString(new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);

    // start building the request
    Invocation.Builder bldr = resources
        .getJerseyTest()
        .target("/v1/messages/multi_recipient")
        .queryParam("online", true)
        .queryParam("ts", 1663798405641L)
        .queryParam("story", story)
        .request()
        .header(HttpHeaders.USER_AGENT, "Test User Agent")
        .header(HeaderUtils.UNIDENTIFIED_ACCESS_KEY, accessBytes);

    // make the PUT request
    try (final Response response = bldr.put(entity)) {
      if (story || known) {
        // it's a story so we unconditionally get 200 ok
        assertEquals(200, response.getStatus());
      } else {
        // unknown recipient means 404 not found
        assertEquals(404, response.getStatus());
      }
    }
  }

  private static Stream<Arguments> testSendMultiRecipientMessageToUnknownAccounts() {
    return Stream.of(
        Arguments.of(true, true, false),
        Arguments.of(true, false, false),
        Arguments.of(false, true, false),
        Arguments.of(false, false, false),

        Arguments.of(true, true, true),
        Arguments.of(true, false, true),
        Arguments.of(false, true, true),
        Arguments.of(false, false, true)
    );
  }

  @Test
  void sendMultiRecipientMessageMismatchedDevices() throws JsonProcessingException {

    final ServiceIdentifier serviceIdentifier = MULTI_DEVICE_ACI_ID;

    final byte extraDeviceId = MULTI_DEVICE_ID3 + 1;

    final List<Recipient> recipients = List.of(
        new Recipient(serviceIdentifier, MULTI_DEVICE_ID1, MULTI_DEVICE_REG_ID1, new byte[48]),
        new Recipient(serviceIdentifier, MULTI_DEVICE_ID2, MULTI_DEVICE_REG_ID2, new byte[48]),
        new Recipient(serviceIdentifier, MULTI_DEVICE_ID3, MULTI_DEVICE_REG_ID3, new byte[48]),
        new Recipient(serviceIdentifier, extraDeviceId, 1234, new byte[48]));

    // initialize our binary payload and create an input stream
    final byte[] buffer = new byte[2048];
    final InputStream stream = initializeMultiPayload(recipients, buffer, true);

    // set up the entity to use in our PUT request
    final Entity<InputStream> entity = Entity.entity(stream, MultiRecipientMessageProvider.MEDIA_TYPE);

    // start building the request
    final Invocation.Builder invocationBuilder = resources
        .getJerseyTest()
        .target("/v1/messages/multi_recipient")
        .queryParam("online", false)
        .queryParam("ts", System.currentTimeMillis())
        .queryParam("story", false)
        .queryParam("urgent", true)
        .request()
        .header(HttpHeaders.USER_AGENT, "test")
        .header(HeaderUtils.UNIDENTIFIED_ACCESS_KEY, Base64.getEncoder().encodeToString(UNIDENTIFIED_ACCESS_BYTES));

    // make the PUT request
    try (final Response response = invocationBuilder.put(entity)) {
      assertEquals(409, response.getStatus());

      final List<AccountMismatchedDevices> mismatchedDevices =
          SystemMapper.jsonMapper().readValue(response.readEntity(String.class),
              SystemMapper.jsonMapper().getTypeFactory()
                  .constructCollectionType(List.class, AccountMismatchedDevices.class));

      assertEquals(List.of(new AccountMismatchedDevices(serviceIdentifier,
              new MismatchedDevices(Collections.emptyList(), List.of(extraDeviceId)))),
          mismatchedDevices);
    }
  }

  @Test
  void sendMultiRecipientMessageStaleDevices() throws JsonProcessingException {
    final ServiceIdentifier serviceIdentifier = MULTI_DEVICE_ACI_ID;
    final List<Recipient> recipients = List.of(
        new Recipient(serviceIdentifier, MULTI_DEVICE_ID1, MULTI_DEVICE_REG_ID1 + 1, new byte[48]),
        new Recipient(serviceIdentifier, MULTI_DEVICE_ID2, MULTI_DEVICE_REG_ID2 + 1, new byte[48]),
        new Recipient(serviceIdentifier, MULTI_DEVICE_ID3, MULTI_DEVICE_REG_ID3 + 1, new byte[48]));

    // initialize our binary payload and create an input stream
    byte[] buffer = new byte[2048];
    // InputStream stream = initializeMultiPayload(recipientUUID, buffer);
    InputStream stream = initializeMultiPayload(recipients, buffer, true);

    // set up the entity to use in our PUT request
    Entity<InputStream> entity = Entity.entity(stream, MultiRecipientMessageProvider.MEDIA_TYPE);

    // start building the request
    final Invocation.Builder invocationBuilder = resources
        .getJerseyTest()
        .target("/v1/messages/multi_recipient")
        .queryParam("online", false)
        .queryParam("ts", System.currentTimeMillis())
        .queryParam("story", false)
        .queryParam("urgent", true)
        .request()
        .header(HttpHeaders.USER_AGENT, "test")
        .header(HeaderUtils.UNIDENTIFIED_ACCESS_KEY, Base64.getEncoder().encodeToString(UNIDENTIFIED_ACCESS_BYTES));

    // make the PUT request
    try (final Response response = invocationBuilder.put(entity)) {
      assertEquals(410, response.getStatus());

      final List<AccountStaleDevices> staleDevices =
          SystemMapper.jsonMapper().readValue(response.readEntity(String.class),
              SystemMapper.jsonMapper().getTypeFactory()
                  .constructCollectionType(List.class, AccountStaleDevices.class));

      assertEquals(1, staleDevices.size());
      assertEquals(serviceIdentifier, staleDevices.getFirst().uuid());
      assertEquals(Set.of(MULTI_DEVICE_ID1, MULTI_DEVICE_ID2, MULTI_DEVICE_ID3),
          new HashSet<>(staleDevices.getFirst().devices().staleDevices()));
    }
  }

  @Test
  void sendMultiRecipientMessageStoryRateLimited() {
    final List<Recipient> recipients = List.of(new Recipient(SINGLE_DEVICE_ACI_ID, SINGLE_DEVICE_ID1, SINGLE_DEVICE_REG_ID1, new byte[48]));
    // initialize our binary payload and create an input stream
    byte[] buffer = new byte[2048];
    // InputStream stream = initializeMultiPayload(recipientUUID, buffer);
    InputStream stream = initializeMultiPayload(recipients, buffer, true);

    // set up the entity to use in our PUT request
    Entity<InputStream> entity = Entity.entity(stream, MultiRecipientMessageProvider.MEDIA_TYPE);

    // start building the request
    final Invocation.Builder invocationBuilder = resources
        .getJerseyTest()
        .target("/v1/messages/multi_recipient")
        .queryParam("online", false)
        .queryParam("ts", System.currentTimeMillis())
        .queryParam("story", true)
        .queryParam("urgent", true)
        .request()
        .header(HttpHeaders.USER_AGENT, "test")
        .header(HeaderUtils.UNIDENTIFIED_ACCESS_KEY, Base64.getEncoder().encodeToString(UNIDENTIFIED_ACCESS_BYTES));

    when(rateLimiter.validateAsync(any(UUID.class)))
        .thenReturn(CompletableFuture.failedFuture(new RateLimitExceededException(Duration.ofSeconds(77))));

    try (final Response response = invocationBuilder.put(entity)) {
      assertEquals(429, response.getStatus());
    }
  }

  @SuppressWarnings("SameParameterValue")
  private void checkBadMultiRecipientResponse(Response response, int expectedCode) throws Exception {
    assertThat("Unexpected response", response.getStatus(), is(equalTo(expectedCode)));
    verify(messageSender, never()).sendMessage(any(), any(), any(), anyBoolean());
  }

  @SuppressWarnings("SameParameterValue")
  private static Envelope generateEnvelope(UUID guid, int type, long timestamp, UUID sourceUuid,
      byte sourceDevice, UUID destinationUuid, UUID updatedPni, byte[] content, long serverTimestamp) {
    return generateEnvelope(guid, type, timestamp, sourceUuid, sourceDevice, destinationUuid, updatedPni, content, serverTimestamp, false);
  }

  private static Envelope generateEnvelope(UUID guid, int type, long timestamp, UUID sourceUuid,
      byte sourceDevice, UUID destinationUuid, UUID updatedPni, byte[] content, long serverTimestamp, boolean story) {

    final MessageProtos.Envelope.Builder builder = MessageProtos.Envelope.newBuilder()
        .setType(MessageProtos.Envelope.Type.forNumber(type))
        .setClientTimestamp(timestamp)
        .setServerTimestamp(serverTimestamp)
        .setDestinationServiceId(destinationUuid.toString())
        .setStory(story)
        .setServerGuid(guid.toString());

    if (sourceUuid != null) {
      builder.setSourceServiceId(sourceUuid.toString());
      builder.setSourceDevice(sourceDevice);
    }

    if (content != null) {
      builder.setContent(ByteString.copyFrom(content));
    }

    if (updatedPni != null) {
      builder.setUpdatedPni(updatedPni.toString());
    }

    return builder.build();
  }

}
