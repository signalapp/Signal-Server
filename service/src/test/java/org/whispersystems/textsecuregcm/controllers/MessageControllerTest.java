/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.whispersystems.textsecuregcm.tests.util.JsonHelpers.asJson;
import static org.whispersystems.textsecuregcm.tests.util.JsonHelpers.jsonFixture;

import com.fasterxml.jackson.core.JsonProcessingException;
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
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
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
import org.mockito.ArgumentCaptor;
import org.signal.libsignal.zkgroup.ServerSecretParams;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.UnidentifiedAccessUtil;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicInboundMessageByteLimitConfiguration;
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
import org.whispersystems.textsecuregcm.identity.IdentityType;
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
import org.whispersystems.textsecuregcm.storage.PhoneNumberIdentifiers;
import org.whispersystems.textsecuregcm.storage.RemovedMessage;
import org.whispersystems.textsecuregcm.storage.ReportMessageManager;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.tests.util.MultiRecipientMessageHelper;
import org.whispersystems.textsecuregcm.tests.util.TestRecipient;
import org.whispersystems.textsecuregcm.util.CompletableFutureTestUtil;
import org.whispersystems.textsecuregcm.util.HeaderUtils;
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.TestClock;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import org.whispersystems.websocket.WebsocketHeaders;
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
  private static final PhoneNumberIdentifiers phoneNumberIdentifiers = mock(PhoneNumberIdentifiers.class);
  private static final PushNotificationManager pushNotificationManager = mock(PushNotificationManager.class);
  private static final PushNotificationScheduler pushNotificationScheduler = mock(PushNotificationScheduler.class);
  private static final ReportMessageManager reportMessageManager = mock(ReportMessageManager.class);
  private static final Scheduler messageDeliveryScheduler = Schedulers.newBoundedElastic(10, 10_000, "messageDelivery");

  @SuppressWarnings("unchecked")
  private static final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager =
      mock(DynamicConfigurationManager.class);

  private static final ServerSecretParams serverSecretParams = ServerSecretParams.generate();

  private static final TestClock clock = TestClock.now();

  private static final Instant START_OF_DAY = LocalDate.now(clock).atStartOfDay().toInstant(ZoneOffset.UTC);

  private static final ResourceExtension resources = ResourceExtension.builder()
      .addProperty(ServerProperties.UNWRAP_COMPLETION_STAGE_IN_WRITER_ENABLE, Boolean.TRUE)
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(new AuthValueFactoryProvider.Binder<>(AuthenticatedDevice.class))
      .addProvider(RateLimitExceededExceptionMapper.class)
      .addProvider(MultiRecipientMessageProvider.class)
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(
          new MessageController(rateLimiters, cardinalityEstimator, messageSender, receiptSender, accountsManager,
              messagesManager, phoneNumberIdentifiers, pushNotificationManager, pushNotificationScheduler,
              reportMessageManager, messageDeliveryScheduler, mock(ClientReleaseManager.class),
              dynamicConfigurationManager, serverSecretParams, SpamChecker.noop(), new MessageMetrics(), mock(MessageDeliveryLoopMonitor.class),
              clock))
      .build();

  @BeforeEach
  void setup() {
    reset(pushNotificationScheduler);

    when(messageSender.sendMultiRecipientMessage(any(), any(), anyLong(), anyBoolean(), anyBoolean(), anyBoolean()))
        .thenReturn(CompletableFuture.completedFuture(null));

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
        phoneNumberIdentifiers,
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

      @SuppressWarnings("unchecked") final ArgumentCaptor<Map<Byte, Envelope>> captor = ArgumentCaptor.forClass(Map.class);
      verify(messageSender).sendMessages(any(), captor.capture());

      assertEquals(1, captor.getValue().size());
      final Envelope message = captor.getValue().values().stream().findFirst().orElseThrow();

      assertTrue(message.hasSourceServiceId());
      assertTrue(message.hasSourceDevice());
      assertTrue(message.getUrgent());
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

      @SuppressWarnings("unchecked") final ArgumentCaptor<Map<Byte, Envelope>> captor = ArgumentCaptor.forClass(Map.class);
      verify(messageSender).sendMessages(any(), captor.capture());

      assertEquals(1, captor.getValue().size());
      final Envelope message = captor.getValue().values().stream().findFirst().orElseThrow();

      assertTrue(message.hasSourceServiceId());
      assertTrue(message.hasSourceDevice());
      assertFalse(message.getUrgent());
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

      @SuppressWarnings("unchecked") final ArgumentCaptor<Map<Byte, Envelope>> captor = ArgumentCaptor.forClass(Map.class);
      verify(messageSender).sendMessages(any(), captor.capture());

      assertEquals(1, captor.getValue().size());
      final Envelope message = captor.getValue().values().stream().findFirst().orElseThrow();

      assertTrue(message.hasSourceServiceId());
      assertTrue(message.hasSourceDevice());
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

      @SuppressWarnings("unchecked") final ArgumentCaptor<Map<Byte, Envelope>> captor = ArgumentCaptor.forClass(Map.class);
      verify(messageSender).sendMessages(any(), captor.capture());

      assertEquals(1, captor.getValue().size());
      final Envelope message = captor.getValue().values().stream().findFirst().orElseThrow();

      assertFalse(message.hasSourceServiceId());
      assertFalse(message.hasSourceDevice());
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
        @SuppressWarnings("unchecked") final ArgumentCaptor<Map<Byte, Envelope>> captor = ArgumentCaptor.forClass(Map.class);
        verify(messageSender).sendMessages(any(), captor.capture());

        assertEquals(1, captor.getValue().size());
        final Envelope message = captor.getValue().values().stream().findFirst().orElseThrow();

        assertFalse(message.hasSourceServiceId());
        assertFalse(message.hasSourceDevice());
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

  @ParameterizedTest
  @CsvSource({
      "-1, 422",
      "0, 200",
      "1, 200",
      "8640000000000000, 200",
      "8640000000000001, 422",

      // This is something of a quirk; because this failure is happening at the parsing layer (we can't parse it as a
      // `long`) instead of the validation layer, we get a 400 instead of a 422
      "99999999999999999999999999999999999, 400"
  })
  void testSingleDeviceExtremeTimestamp(final String timestamp, final int expectedStatus) throws JsonProcessingException {
    final String jsonTemplate = """
        {
            "timestamp" : %s,
            "messages" : [{
                "type" : 1,
                "destinationDeviceId" : 1,
                "content" : "Zm9vYmFyego"
            }]
        }
        """;

    final String json = String.format(jsonTemplate, timestamp);

    try (final Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/%s", SINGLE_DEVICE_UUID))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.json(json))) {

      assertThat(response.getStatus(), is(equalTo(expectedStatus)));
    }
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

      @SuppressWarnings("unchecked") final ArgumentCaptor<Map<Byte, Envelope>> envelopeCaptor =
          ArgumentCaptor.forClass(Map.class);

      verify(messageSender).sendMessages(any(Account.class), envelopeCaptor.capture());

      assertEquals(3, envelopeCaptor.getValue().size());

      envelopeCaptor.getValue().values().forEach(envelope -> {
        assertTrue(envelope.getUrgent());
      });
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

      @SuppressWarnings("unchecked") final ArgumentCaptor<Map<Byte, Envelope>> envelopeCaptor =
          ArgumentCaptor.forClass(Map.class);

      verify(messageSender).sendMessages(any(Account.class), envelopeCaptor.capture());

      assertEquals(3, envelopeCaptor.getValue().size());

      envelopeCaptor.getValue().values().forEach(envelope -> {
        assertFalse(envelope.getUrgent());
      });
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

      verify(messageSender).sendMessages(any(Account.class),
          argThat(messagesByDeviceId -> messagesByDeviceId.size() == 3));
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
            .header(WebsocketHeaders.X_SIGNAL_RECEIVE_STORIES, receiveStories ? "true" : "false")
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
      verify(accountsManager, never()).findRecentlyDeletedPhoneNumberIdentifier(any(UUID.class));
      verify(phoneNumberIdentifiers, never()).getPhoneNumber(any());
    }
  }

  @Test
  void testReportMesageByE164DeletedAccount() {
    final String senderNumber = "+12125550001";
    final UUID senderAci = UUID.randomUUID();
    final UUID senderPni = UUID.randomUUID();
    final String userAgent = "user-agent";
    UUID messageGuid = UUID.randomUUID();

    final Account account = mock(Account.class);
    when(account.getUuid()).thenReturn(senderAci);
    when(account.getNumber()).thenReturn(senderNumber);
    when(account.getPhoneNumberIdentifier()).thenReturn(senderPni);

    when(accountsManager.getByE164(senderNumber)).thenReturn(Optional.empty());
    when(phoneNumberIdentifiers.getPhoneNumberIdentifier(senderNumber)).thenReturn(CompletableFuture.completedFuture(senderPni));
    when(accountsManager.findRecentlyDeletedAccountIdentifier(senderPni)).thenReturn(Optional.of(senderAci));

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
    when(phoneNumberIdentifiers.getPhoneNumber(senderPni)).thenReturn(CompletableFuture.completedFuture(List.of(senderNumber)));

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
      verify(accountsManager, never()).findRecentlyDeletedPhoneNumberIdentifier(any(UUID.class));
      verify(phoneNumberIdentifiers, never()).getPhoneNumber(any());
    }
  }

  @Test
  void testReportMessageByAciDeletedAccount() {
    final String senderNumber = "+12125550001";
    final UUID senderAci = UUID.randomUUID();
    final UUID senderPni = UUID.randomUUID();
    final String userAgent = "user-agent";
    UUID messageGuid = UUID.randomUUID();

    final Account account = mock(Account.class);
    when(account.getUuid()).thenReturn(senderAci);
    when(account.getNumber()).thenReturn(senderNumber);
    when(account.getPhoneNumberIdentifier()).thenReturn(senderPni);

    when(accountsManager.getByAccountIdentifier(senderAci)).thenReturn(Optional.empty());
    when(accountsManager.findRecentlyDeletedPhoneNumberIdentifier(senderAci)).thenReturn(Optional.of(senderPni));
    when(phoneNumberIdentifiers.getPhoneNumber(senderPni)).thenReturn(CompletableFuture.completedFuture(List.of(senderNumber)));

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
    when(accountsManager.findRecentlyDeletedPhoneNumberIdentifier(senderAci)).thenReturn(Optional.of(senderPni));
    when(phoneNumberIdentifiers.getPhoneNumber(senderPni)).thenReturn(CompletableFuture.completedFuture(List.of(senderNumber)));

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
      verify(accountsManager, never()).findRecentlyDeletedPhoneNumberIdentifier(any(UUID.class));
      verify(phoneNumberIdentifiers, never()).getPhoneNumber(any());
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
    when(accountsManager.findRecentlyDeletedPhoneNumberIdentifier(senderAci)).thenReturn(Optional.of(senderPni));
    when(phoneNumberIdentifiers.getPhoneNumber(senderPni)).thenReturn(CompletableFuture.completedFuture(List.of(senderNumber)));

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
  void testValidateContentLength() {
    final int contentLength = Math.toIntExact(MessageController.MAX_MESSAGE_SIZE + 1);
    final byte[] contentBytes = new byte[contentLength];
    Arrays.fill(contentBytes, (byte) 1);

    try (final Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/%s", SINGLE_DEVICE_UUID))
            .request()
            .header(HeaderUtils.UNIDENTIFIED_ACCESS_KEY, Base64.getEncoder().encodeToString(UNIDENTIFIED_ACCESS_BYTES))
            .put(Entity.entity(new IncomingMessageList(
                    List.of(new IncomingMessage(1, (byte) 1, 1, Base64.getEncoder().encodeToString(contentBytes))), false, true,
                    System.currentTimeMillis()),
                MediaType.APPLICATION_JSON_TYPE))) {

      assertThat("Bad response", response.getStatus(), is(equalTo(413)));

      verify(messageSender, never()).sendMessages(any(), any());
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
        verify(messageSender).sendMessages(any(), any());
      } else {
        assertEquals(422, response.getStatus());
        verify(messageSender, never()).sendMessages(any(), any());
      }
    }
  }

  private static Stream<Arguments> testValidateEnvelopeType() {
    return Stream.of(
        Arguments.of("fixtures/current_message_single_device.json", true),
        Arguments.of("fixtures/current_message_single_device_server_receipt_type.json", false)
    );
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  @ParameterizedTest
  @MethodSource
  void sendMultiRecipientMessage(final Map<ServiceIdentifier, Account> accountsByServiceIdentifier,
      final byte[] multiRecipientMessage,
      final long timestamp,
      final boolean isStory,
      final boolean rateLimit,
      final Optional<String> maybeAccessKey,
      final Optional<String> maybeGroupSendToken,
      final int expectedStatus,
      final Set<Account> expectedResolvedAccounts,
      final Set<ServiceIdentifier> expectedUuids404) {

    clock.pin(START_OF_DAY);

    when(accountsManager.getByServiceIdentifierAsync(any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    accountsByServiceIdentifier.forEach(((serviceIdentifier, account) ->
        when(accountsManager.getByServiceIdentifierAsync(serviceIdentifier))
            .thenReturn(CompletableFuture.completedFuture(Optional.of(account)))));

    final boolean ephemeral = true;
    final boolean urgent = false;

    final Invocation.Builder invocationBuilder = resources
        .getJerseyTest()
        .target("/v1/messages/multi_recipient")
        .queryParam("ts", timestamp)
        .queryParam("online", ephemeral)
        .queryParam("story", isStory)
        .queryParam("urgent", urgent)
        .request();

    maybeAccessKey.ifPresent(accessKey ->
        invocationBuilder.header(HeaderUtils.UNIDENTIFIED_ACCESS_KEY, accessKey));

    maybeGroupSendToken.ifPresent(groupSendToken ->
        invocationBuilder.header(HeaderUtils.GROUP_SEND_TOKEN, groupSendToken));

    if (rateLimit) {
      when(rateLimiter.validateAsync(any(UUID.class)))
          .thenReturn(CompletableFuture.failedFuture(new RateLimitExceededException(Duration.ofSeconds(77))));
    } else {
      when(rateLimiter.validateAsync(any(UUID.class)))
          .thenReturn(CompletableFuture.completedFuture(null));
    }

    try (final Response response = invocationBuilder
        .put(Entity.entity(multiRecipientMessage, MultiRecipientMessageProvider.MEDIA_TYPE))) {

      assertThat(response.getStatus(), is(equalTo(expectedStatus)));

      if (expectedStatus == 200) {
        final SendMultiRecipientMessageResponse entity = response.readEntity(SendMultiRecipientMessageResponse.class);
        assertThat(Set.copyOf(entity.uuids404()), equalTo(expectedUuids404));
      }

      if (expectedStatus == 200 && !expectedResolvedAccounts.isEmpty()) {
        verify(messageSender).sendMultiRecipientMessage(any(),
            argThat(resolvedRecipients ->
                new HashSet<>(resolvedRecipients.values()).equals(expectedResolvedAccounts)),
            anyLong(),
            eq(isStory),
            eq(ephemeral),
            eq(urgent));
      } else {
        verify(messageSender, never()).sendMultiRecipientMessage(any(), any(), anyLong(), anyBoolean(), anyBoolean(), anyBoolean());
      }
    }
  }

  private static List<Arguments> sendMultiRecipientMessage() throws Exception {
    final UUID singleDeviceAccountAci = UUID.randomUUID();
    final UUID singleDeviceAccountPni = UUID.randomUUID();
    final UUID multiDeviceAccountAci = UUID.randomUUID();
    final UUID multiDeviceAccountPni = UUID.randomUUID();

    final byte[] singleDeviceAccountUak = TestRandomUtil.nextBytes(UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH);
    final byte[] multiDeviceAccountUak = TestRandomUtil.nextBytes(UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH);

    final int singleDevicePrimaryRegistrationId = 1;
    final int multiDevicePrimaryRegistrationId = 2;
    final int multiDeviceLinkedRegistrationId = 3;

    final Device singleDeviceAccountPrimary = mock(Device.class);
    when(singleDeviceAccountPrimary.getId()).thenReturn(Device.PRIMARY_ID);
    when(singleDeviceAccountPrimary.getRegistrationId()).thenReturn(singleDevicePrimaryRegistrationId);

    final Device multiDeviceAccountPrimary = mock(Device.class);
    when(multiDeviceAccountPrimary.getId()).thenReturn(Device.PRIMARY_ID);
    when(multiDeviceAccountPrimary.getRegistrationId()).thenReturn(multiDevicePrimaryRegistrationId);

    final Device multiDeviceAccountLinked = mock(Device.class);
    when(multiDeviceAccountLinked.getId()).thenReturn((byte) (Device.PRIMARY_ID + 1));
    when(multiDeviceAccountLinked.getRegistrationId()).thenReturn(multiDeviceLinkedRegistrationId);

    final Account singleDeviceAccount = mock(Account.class);
    when(singleDeviceAccount.getIdentifier(IdentityType.ACI)).thenReturn(singleDeviceAccountAci);
    when(singleDeviceAccount.getUnidentifiedAccessKey()).thenReturn(Optional.of(singleDeviceAccountUak));
    when(singleDeviceAccount.getDevices()).thenReturn(List.of(singleDeviceAccountPrimary));
    when(singleDeviceAccount.getDevice(anyByte())).thenReturn(Optional.empty());
    when(singleDeviceAccount.getDevice(Device.PRIMARY_ID)).thenReturn(Optional.of(singleDeviceAccountPrimary));

    final Account multiDeviceAccount = mock(Account.class);
    when(multiDeviceAccount.getIdentifier(IdentityType.ACI)).thenReturn(multiDeviceAccountAci);
    when(multiDeviceAccount.getUnidentifiedAccessKey()).thenReturn(Optional.of(multiDeviceAccountUak));
    when(multiDeviceAccount.getDevices()).thenReturn(List.of(multiDeviceAccountPrimary, multiDeviceAccountLinked));
    when(multiDeviceAccount.getDevice(anyByte())).thenReturn(Optional.empty());
    when(multiDeviceAccount.getDevice(Device.PRIMARY_ID)).thenReturn(Optional.of(multiDeviceAccountPrimary));
    when(multiDeviceAccount.getDevice((byte) (Device.PRIMARY_ID + 1))).thenReturn(Optional.of(multiDeviceAccountLinked));

    final String groupSendEndorsement = AuthHelper.validGroupSendTokenHeader(serverSecretParams,
        List.of(new AciServiceIdentifier(singleDeviceAccountAci), new AciServiceIdentifier(multiDeviceAccountAci)),
        START_OF_DAY.plus(Duration.ofDays(1)));

    final Map<ServiceIdentifier, Account> accountsByServiceIdentifier = Map.of(
        new AciServiceIdentifier(singleDeviceAccountAci), singleDeviceAccount,
        new AciServiceIdentifier(multiDeviceAccountAci), multiDeviceAccount,
        new PniServiceIdentifier(singleDeviceAccountPni), singleDeviceAccount,
        new PniServiceIdentifier(multiDeviceAccountPni), multiDeviceAccount);

    final byte[] aciMessage = MultiRecipientMessageHelper.generateMultiRecipientMessage(List.of(
        new TestRecipient(new AciServiceIdentifier(singleDeviceAccountAci), Device.PRIMARY_ID, singleDevicePrimaryRegistrationId, new byte[48]),
        new TestRecipient(new AciServiceIdentifier(multiDeviceAccountAci), Device.PRIMARY_ID, multiDevicePrimaryRegistrationId, new byte[48]),
        new TestRecipient(new AciServiceIdentifier(multiDeviceAccountAci), (byte) (Device.PRIMARY_ID + 1), multiDeviceLinkedRegistrationId, new byte[48])));

    return List.of(
        Arguments.argumentSet("Multi-recipient story",
            accountsByServiceIdentifier,
            aciMessage,
            clock.instant().toEpochMilli(),
            true,
            false,
            Optional.empty(),
            Optional.empty(),
            200,
            Set.of(singleDeviceAccount, multiDeviceAccount),
            Set.of()),

        Arguments.argumentSet("Multi-recipient message with combined UAKs",
            accountsByServiceIdentifier,
            aciMessage,
            clock.instant().toEpochMilli(),
            false,
            false,
            Optional.of(Base64.getEncoder().encodeToString(UnidentifiedAccessUtil.getCombinedUnidentifiedAccessKey(List.of(singleDeviceAccount, multiDeviceAccount)))),
            Optional.empty(),
            200,
            Set.of(singleDeviceAccount, multiDeviceAccount),
            Set.of()),

        Arguments.argumentSet("Multi-recipient message with group send endorsement",
            accountsByServiceIdentifier,
            aciMessage,
            clock.instant().toEpochMilli(),
            false,
            false,
            Optional.empty(),
            Optional.of(groupSendEndorsement),
            200,
            Set.of(singleDeviceAccount, multiDeviceAccount),
            Set.of()),

        Arguments.argumentSet("Incorrect combined UAK",
            accountsByServiceIdentifier,
            aciMessage,
            clock.instant().toEpochMilli(),
            false,
            false,
            Optional.of(Base64.getEncoder().encodeToString(TestRandomUtil.nextBytes(UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH))),
            Optional.empty(),
            401,
            Set.of(singleDeviceAccount, multiDeviceAccount),
            Set.of()),

        Arguments.argumentSet("Incorrect group send endorsement",
            accountsByServiceIdentifier,
            aciMessage,
            clock.instant().toEpochMilli(),
            false,
            false,
            Optional.empty(),
            Optional.of(AuthHelper.validGroupSendTokenHeader(serverSecretParams,
                List.of(new AciServiceIdentifier(UUID.randomUUID())),
                START_OF_DAY.plus(Duration.ofDays(1)))),
            401,
            Set.of(singleDeviceAccount, multiDeviceAccount),
            Set.of()),

        // Stories don't require credentials of any kind, but for historical reasons, we don't reject a combined UAK if
        // provided
        Arguments.argumentSet("Story with combined UAKs",
            accountsByServiceIdentifier,
            aciMessage,
            clock.instant().toEpochMilli(),
            true,
            false,
            Optional.of(Base64.getEncoder().encodeToString(UnidentifiedAccessUtil.getCombinedUnidentifiedAccessKey(List.of(singleDeviceAccount, multiDeviceAccount)))),
            Optional.empty(),
            200,
            Set.of(singleDeviceAccount, multiDeviceAccount),
            Set.of()),

        Arguments.argumentSet("Story with group send endorsement",
            accountsByServiceIdentifier,
            aciMessage,
            clock.instant().toEpochMilli(),
            true,
            false,
            Optional.empty(),
            Optional.of(groupSendEndorsement),
            400,
            Set.of(singleDeviceAccount, multiDeviceAccount),
            Set.of()),

        Arguments.argumentSet("Conflicting credentials",
            accountsByServiceIdentifier,
            aciMessage,
            clock.instant().toEpochMilli(),
            false,
            false,
            Optional.of(Base64.getEncoder().encodeToString(UnidentifiedAccessUtil.getCombinedUnidentifiedAccessKey(List.of(singleDeviceAccount, multiDeviceAccount)))),
            Optional.of(groupSendEndorsement),
            400,
            Set.of(singleDeviceAccount, multiDeviceAccount),
            Set.of()),

        Arguments.argumentSet("No credentials",
            accountsByServiceIdentifier,
            aciMessage,
            clock.instant().toEpochMilli(),
            false,
            false,
            Optional.empty(),
            Optional.empty(),
            401,
            Set.of(singleDeviceAccount, multiDeviceAccount),
            Set.of()),

        Arguments.argumentSet("Oversized payload",
            accountsByServiceIdentifier,
            MultiRecipientMessageHelper.generateMultiRecipientMessage(List.of(
                new TestRecipient(new AciServiceIdentifier(singleDeviceAccountAci), Device.PRIMARY_ID, singleDevicePrimaryRegistrationId, new byte[48]),
                new TestRecipient(new AciServiceIdentifier(multiDeviceAccountAci), Device.PRIMARY_ID, multiDevicePrimaryRegistrationId, new byte[48]),
                new TestRecipient(new AciServiceIdentifier(multiDeviceAccountAci), (byte) (Device.PRIMARY_ID + 1), multiDeviceLinkedRegistrationId, new byte[48])),
                MultiRecipientMessageProvider.MAX_MESSAGE_SIZE),
            clock.instant().toEpochMilli(),
            false,
            false,
            Optional.empty(),
            Optional.of(groupSendEndorsement),
            413,
            Set.of(singleDeviceAccount, multiDeviceAccount),
            Set.of()),

        Arguments.argumentSet("Negative timestamp",
            accountsByServiceIdentifier,
            aciMessage,
            -1,
            false,
            false,
            Optional.empty(),
            Optional.of(groupSendEndorsement),
            400,
            Set.of(singleDeviceAccount, multiDeviceAccount),
            Set.of()),

        Arguments.argumentSet("Excessive timestamp",
            accountsByServiceIdentifier,
            aciMessage,
            MessageController.MAX_TIMESTAMP + 1,
            false,
            false,
            Optional.empty(),
            Optional.of(groupSendEndorsement),
            400,
            Set.of(singleDeviceAccount, multiDeviceAccount),
            Set.of()),

        Arguments.argumentSet("Empty recipient list",
            accountsByServiceIdentifier,
            MultiRecipientMessageHelper.generateMultiRecipientMessage(List.of()),
            clock.instant().toEpochMilli(),
            false,
            false,
            Optional.empty(),
            Optional.of(AuthHelper.validGroupSendTokenHeader(serverSecretParams,
                List.of(),
                START_OF_DAY.plus(Duration.ofDays(1)))),
            400,
            Set.of(singleDeviceAccount, multiDeviceAccount),
            Set.of()),

        Arguments.argumentSet("Story with empty recipient list",
            accountsByServiceIdentifier,
            MultiRecipientMessageHelper.generateMultiRecipientMessage(List.of()),
            clock.instant().toEpochMilli(),
            true,
            false,
            Optional.empty(),
            Optional.empty(),
            400,
            Set.of(singleDeviceAccount, multiDeviceAccount),
            Set.of()),

        Arguments.argumentSet("Duplicate recipient",
            accountsByServiceIdentifier,
            MultiRecipientMessageHelper.generateMultiRecipientMessage(List.of(
                    new TestRecipient(new AciServiceIdentifier(singleDeviceAccountAci), Device.PRIMARY_ID, singleDevicePrimaryRegistrationId, new byte[48]),
                    new TestRecipient(new AciServiceIdentifier(singleDeviceAccountAci), Device.PRIMARY_ID, singleDevicePrimaryRegistrationId, new byte[48]))),
            clock.instant().toEpochMilli(),
            false,
            false,
            Optional.empty(),
            Optional.of(groupSendEndorsement),
            400,
            Set.of(singleDeviceAccount, multiDeviceAccount),
            Set.of()),

        Arguments.argumentSet("Missing account",
            Map.of(),
            aciMessage,
            clock.instant().toEpochMilli(),
            false,
            false,
            Optional.empty(),
            Optional.of(groupSendEndorsement),
            200,
            Collections.emptySet(),
            Set.of(new AciServiceIdentifier(singleDeviceAccountAci), new AciServiceIdentifier(multiDeviceAccountAci))),

        Arguments.argumentSet("One missing and one existing account",
            Map.of(new AciServiceIdentifier(singleDeviceAccountAci), singleDeviceAccount),
            aciMessage,
            clock.instant().toEpochMilli(),
            false,
            false,
            Optional.empty(),
            Optional.of(groupSendEndorsement),
            200,
            Set.of(singleDeviceAccount),
            Set.of(new AciServiceIdentifier(multiDeviceAccountAci))),

        Arguments.argumentSet("Missing account for story",
            Map.of(),
            aciMessage,
            clock.instant().toEpochMilli(),
            true,
            false,
            Optional.empty(),
            Optional.empty(),
            200,
            Collections.emptySet(),
            Set.of()),

        Arguments.argumentSet("One missing and one existing account for story",
            Map.of(new AciServiceIdentifier(singleDeviceAccountAci), singleDeviceAccount),
            aciMessage,
            clock.instant().toEpochMilli(),
            true,
            false,
            Optional.empty(),
            Optional.empty(),
            200,
            Set.of(singleDeviceAccount),
            Set.of()),

        Arguments.argumentSet("Missing device",
            accountsByServiceIdentifier,
            MultiRecipientMessageHelper.generateMultiRecipientMessage(List.of(
                new TestRecipient(new AciServiceIdentifier(singleDeviceAccountAci), Device.PRIMARY_ID, singleDevicePrimaryRegistrationId, new byte[48]),
                new TestRecipient(new AciServiceIdentifier(multiDeviceAccountAci), Device.PRIMARY_ID, multiDevicePrimaryRegistrationId, new byte[48]))),
            clock.instant().toEpochMilli(),
            false,
            false,
            Optional.empty(),
            Optional.of(groupSendEndorsement),
            409,
            Set.of(singleDeviceAccount, multiDeviceAccount),
            Set.of()),

        Arguments.argumentSet("Extra device",
            accountsByServiceIdentifier,
            MultiRecipientMessageHelper.generateMultiRecipientMessage(List.of(
                new TestRecipient(new AciServiceIdentifier(singleDeviceAccountAci), Device.PRIMARY_ID, singleDevicePrimaryRegistrationId, new byte[48]),
                new TestRecipient(new AciServiceIdentifier(multiDeviceAccountAci), Device.PRIMARY_ID, multiDevicePrimaryRegistrationId, new byte[48]),
                new TestRecipient(new AciServiceIdentifier(multiDeviceAccountAci), (byte) (Device.PRIMARY_ID + 1), multiDeviceLinkedRegistrationId, new byte[48]),
                new TestRecipient(new AciServiceIdentifier(multiDeviceAccountAci), (byte) (Device.PRIMARY_ID + 2), multiDeviceLinkedRegistrationId + 1, new byte[48]))),
            clock.instant().toEpochMilli(),
            false,
            false,
            Optional.empty(),
            Optional.of(groupSendEndorsement),
            409,
            Set.of(singleDeviceAccount, multiDeviceAccount),
            Set.of()),

        Arguments.argumentSet("Stale registration ID",
            accountsByServiceIdentifier,
            MultiRecipientMessageHelper.generateMultiRecipientMessage(List.of(
                new TestRecipient(new AciServiceIdentifier(singleDeviceAccountAci), Device.PRIMARY_ID, singleDevicePrimaryRegistrationId, new byte[48]),
                new TestRecipient(new AciServiceIdentifier(multiDeviceAccountAci), Device.PRIMARY_ID, multiDevicePrimaryRegistrationId, new byte[48]),
                new TestRecipient(new AciServiceIdentifier(multiDeviceAccountAci), (byte) (Device.PRIMARY_ID + 1), multiDeviceLinkedRegistrationId + 1, new byte[48]))),
            clock.instant().toEpochMilli(),
            false,
            false,
            Optional.empty(),
            Optional.of(groupSendEndorsement),
            410,
            Set.of(singleDeviceAccount, multiDeviceAccount),
            Set.of()),

        Arguments.argumentSet("Rate-limited story",
            accountsByServiceIdentifier,
            aciMessage,
            clock.instant().toEpochMilli(),
            true,
            true,
            Optional.empty(),
            Optional.empty(),
            429,
            Set.of(singleDeviceAccount, multiDeviceAccount),
            Set.of()),

        Arguments.argumentSet("Story to PNI recipients",
            accountsByServiceIdentifier,
            MultiRecipientMessageHelper.generateMultiRecipientMessage(List.of(
                new TestRecipient(new PniServiceIdentifier(singleDeviceAccountPni), Device.PRIMARY_ID, singleDevicePrimaryRegistrationId, new byte[48]),
                new TestRecipient(new PniServiceIdentifier(multiDeviceAccountPni), Device.PRIMARY_ID, multiDevicePrimaryRegistrationId, new byte[48]),
                new TestRecipient(new PniServiceIdentifier(multiDeviceAccountPni), (byte) (Device.PRIMARY_ID + 1), multiDeviceLinkedRegistrationId, new byte[48]))),
            clock.instant().toEpochMilli(),
            true,
            false,
            Optional.empty(),
            Optional.empty(),
            200,
            Set.of(singleDeviceAccount, multiDeviceAccount),
            Set.of()),

        Arguments.argumentSet("Multi-recipient message to PNI recipients with UAK",
            accountsByServiceIdentifier,
            MultiRecipientMessageHelper.generateMultiRecipientMessage(List.of(
                new TestRecipient(new PniServiceIdentifier(singleDeviceAccountPni), Device.PRIMARY_ID, singleDevicePrimaryRegistrationId, new byte[48]),
                new TestRecipient(new PniServiceIdentifier(multiDeviceAccountPni), Device.PRIMARY_ID, multiDevicePrimaryRegistrationId, new byte[48]),
                new TestRecipient(new PniServiceIdentifier(multiDeviceAccountPni), (byte) (Device.PRIMARY_ID + 1), multiDeviceLinkedRegistrationId, new byte[48]))),
            clock.instant().toEpochMilli(),
            false,
            false,
            Optional.of(Base64.getEncoder().encodeToString(UnidentifiedAccessUtil.getCombinedUnidentifiedAccessKey(List.of(singleDeviceAccount, multiDeviceAccount)))),
            Optional.empty(),
            401,
            Set.of(singleDeviceAccount, multiDeviceAccount),
            Set.of()),

        Arguments.argumentSet("Multi-recipient message to PNI recipients with group send endorsement",
            accountsByServiceIdentifier,
            MultiRecipientMessageHelper.generateMultiRecipientMessage(List.of(
                new TestRecipient(new PniServiceIdentifier(singleDeviceAccountPni), Device.PRIMARY_ID, singleDevicePrimaryRegistrationId, new byte[48]),
                new TestRecipient(new PniServiceIdentifier(multiDeviceAccountPni), Device.PRIMARY_ID, multiDevicePrimaryRegistrationId, new byte[48]),
                new TestRecipient(new PniServiceIdentifier(multiDeviceAccountPni), (byte) (Device.PRIMARY_ID + 1), multiDeviceLinkedRegistrationId, new byte[48]))),
            clock.instant().toEpochMilli(),
            false,
            false,
            Optional.empty(),
            Optional.of(AuthHelper.validGroupSendTokenHeader(serverSecretParams,
                List.of(new PniServiceIdentifier(singleDeviceAccountPni), new PniServiceIdentifier(multiDeviceAccountPni)),
                START_OF_DAY.plus(Duration.ofDays(1)))),
            200,
            Set.of(singleDeviceAccount, multiDeviceAccount),
            Set.of())
    );
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

  @Test
  void decodedSize() {
    for (int size = MessageController.MAX_MESSAGE_SIZE - 3; size <= MessageController.MAX_MESSAGE_SIZE + 3; size++) {
      final byte[] bytes = TestRandomUtil.nextBytes(size);
      final String base64Encoded = Base64.getEncoder().encodeToString(bytes);

      assertEquals(bytes.length, MessageController.decodedSize(base64Encoded));
    }
  }
}
