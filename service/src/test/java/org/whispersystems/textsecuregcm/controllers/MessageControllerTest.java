/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.whispersystems.textsecuregcm.tests.util.JsonHelpers.asJson;
import static org.whispersystems.textsecuregcm.tests.util.JsonHelpers.jsonFixture;

import com.google.common.collect.ImmutableSet;
import com.google.common.net.HttpHeaders;
import com.google.protobuf.ByteString;
import io.dropwizard.auth.PolymorphicAuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Base64;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.DisabledPermittedAuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.OptionalAccess;
import org.whispersystems.textsecuregcm.entities.IncomingMessage;
import org.whispersystems.textsecuregcm.entities.IncomingMessageList;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.entities.MismatchedDevices;
import org.whispersystems.textsecuregcm.entities.MultiRecipientMessage;
import org.whispersystems.textsecuregcm.entities.MultiRecipientMessage.Recipient;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntity;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntityList;
import org.whispersystems.textsecuregcm.entities.SendMultiRecipientMessageResponse;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;
import org.whispersystems.textsecuregcm.entities.StaleDevices;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.mappers.RateLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.providers.MultiRecipientMessageProvider;
import org.whispersystems.textsecuregcm.push.MessageSender;
import org.whispersystems.textsecuregcm.push.PushNotificationManager;
import org.whispersystems.textsecuregcm.push.ReceiptSender;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.DeletedAccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.storage.ReportMessageManager;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.websocket.Stories;
import reactor.core.publisher.Mono;

@ExtendWith(DropwizardExtensionsSupport.class)
class MessageControllerTest {

  private static final String SINGLE_DEVICE_RECIPIENT = "+14151111111";
  private static final UUID   SINGLE_DEVICE_UUID      = UUID.fromString("11111111-1111-1111-1111-111111111111");
  private static final UUID   SINGLE_DEVICE_PNI       = UUID.fromString("11111111-0000-0000-0000-111111111111");
  private static final int SINGLE_DEVICE_ID1 = 1;
  private static final int SINGLE_DEVICE_REG_ID1 = 111;

  private static final String MULTI_DEVICE_RECIPIENT = "+14152222222";
  private static final UUID MULTI_DEVICE_UUID = UUID.fromString("22222222-2222-2222-2222-222222222222");
  private static final UUID MULTI_DEVICE_PNI = UUID.fromString("22222222-0000-0000-0000-222222222222");
  private static final int MULTI_DEVICE_ID1 = 1;
  private static final int MULTI_DEVICE_ID2 = 2;
  private static final int MULTI_DEVICE_ID3 = 3;
  private static final int MULTI_DEVICE_REG_ID1 = 222;
  private static final int MULTI_DEVICE_REG_ID2 = 333;
  private static final int MULTI_DEVICE_REG_ID3 = 444;

  private static final byte[] UNIDENTIFIED_ACCESS_BYTES = "0123456789abcdef".getBytes();

  private static final String INTERNATIONAL_RECIPIENT = "+61123456789";
  private static final UUID INTERNATIONAL_UUID = UUID.fromString("33333333-3333-3333-3333-333333333333");

  private Account internationalAccount;

  @SuppressWarnings("unchecked")
  private static final RedisAdvancedClusterCommands<String, String> redisCommands  = mock(RedisAdvancedClusterCommands.class);

  private static final MessageSender messageSender = mock(MessageSender.class);
  private static final ReceiptSender receiptSender = mock(ReceiptSender.class);
  private static final AccountsManager accountsManager = mock(AccountsManager.class);
  private static final DeletedAccountsManager deletedAccountsManager = mock(DeletedAccountsManager.class);
  private static final MessagesManager messagesManager = mock(MessagesManager.class);
  private static final RateLimiters rateLimiters = mock(RateLimiters.class);
  private static final RateLimiter rateLimiter = mock(RateLimiter.class);
  private static final PushNotificationManager pushNotificationManager = mock(PushNotificationManager.class);
  private static final ReportMessageManager reportMessageManager = mock(ReportMessageManager.class);
  private static final ExecutorService multiRecipientMessageExecutor = mock(ExecutorService.class);

  private static final ResourceExtension resources = ResourceExtension.builder()
      .addProperty(ServerProperties.UNWRAP_COMPLETION_STAGE_IN_WRITER_ENABLE, Boolean.TRUE)
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(new PolymorphicAuthValueFactoryProvider.Binder<>(
          ImmutableSet.of(AuthenticatedAccount.class, DisabledPermittedAuthenticatedAccount.class)))
      .addProvider(RateLimitExceededExceptionMapper.class)
      .addProvider(MultiRecipientMessageProvider.class)
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(
          new MessageController(rateLimiters, messageSender, receiptSender, accountsManager, deletedAccountsManager,
              messagesManager, pushNotificationManager, reportMessageManager, multiRecipientMessageExecutor))
      .build();

  @BeforeEach
  void setup() {
    final List<Device> singleDeviceList = List.of(
        generateTestDevice(SINGLE_DEVICE_ID1, SINGLE_DEVICE_REG_ID1, 1111, new SignedPreKey(333, "baz", "boop"), System.currentTimeMillis(), System.currentTimeMillis())
    );

    final List<Device> multiDeviceList = List.of(
        generateTestDevice(MULTI_DEVICE_ID1, MULTI_DEVICE_REG_ID1, 2222, new SignedPreKey(111, "foo", "bar"), System.currentTimeMillis(), System.currentTimeMillis()),
        generateTestDevice(MULTI_DEVICE_ID2, MULTI_DEVICE_REG_ID2, 3333, new SignedPreKey(222, "oof", "rab"), System.currentTimeMillis(), System.currentTimeMillis()),
        generateTestDevice(MULTI_DEVICE_ID3, MULTI_DEVICE_REG_ID3, 4444, null, System.currentTimeMillis(), System.currentTimeMillis() - TimeUnit.DAYS.toMillis(31))
    );

    Account singleDeviceAccount  = AccountsHelper.generateTestAccount(SINGLE_DEVICE_RECIPIENT, SINGLE_DEVICE_UUID, SINGLE_DEVICE_PNI, singleDeviceList, UNIDENTIFIED_ACCESS_BYTES);
    Account multiDeviceAccount   = AccountsHelper.generateTestAccount(MULTI_DEVICE_RECIPIENT, MULTI_DEVICE_UUID, MULTI_DEVICE_PNI, multiDeviceList, UNIDENTIFIED_ACCESS_BYTES);
    internationalAccount         = AccountsHelper.generateTestAccount(INTERNATIONAL_RECIPIENT, INTERNATIONAL_UUID, UUID.randomUUID(), singleDeviceList, UNIDENTIFIED_ACCESS_BYTES);

    when(accountsManager.getByAccountIdentifier(eq(SINGLE_DEVICE_UUID))).thenReturn(Optional.of(singleDeviceAccount));
    when(accountsManager.getByPhoneNumberIdentifier(SINGLE_DEVICE_PNI)).thenReturn(Optional.of(singleDeviceAccount));
    when(accountsManager.getByAccountIdentifier(eq(MULTI_DEVICE_UUID))).thenReturn(Optional.of(multiDeviceAccount));
    when(accountsManager.getByPhoneNumberIdentifier(MULTI_DEVICE_PNI)).thenReturn(Optional.of(multiDeviceAccount));
    when(accountsManager.getByAccountIdentifier(INTERNATIONAL_UUID)).thenReturn(Optional.of(internationalAccount));

    when(rateLimiters.getMessagesLimiter()).thenReturn(rateLimiter);
  }

  private static Device generateTestDevice(final long id, final int registrationId, final int pniRegistrationId, final SignedPreKey signedPreKey, final long createdAt, final long lastSeen) {
    final Device device = new Device();
    device.setId(id);
    device.setRegistrationId(registrationId);
    device.setPhoneNumberIdentityRegistrationId(pniRegistrationId);
    device.setSignedPreKey(signedPreKey);
    device.setCreated(createdAt);
    device.setLastSeen(lastSeen);
    device.setGcmId("isgcm");

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
        pushNotificationManager,
        reportMessageManager,
        multiRecipientMessageExecutor
    );
  }

  @Test
  void testSendFromDisabledAccount() throws Exception {
    Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/%s", SINGLE_DEVICE_UUID))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.DISABLED_UUID, AuthHelper.DISABLED_PASSWORD))
            .put(Entity.entity(SystemMapper.getMapper().readValue(jsonFixture("fixtures/current_message_single_device.json"),
                    IncomingMessageList.class),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat("Unauthorized response", response.getStatus(), is(equalTo(401)));
  }

  @Test
  void testSingleDeviceCurrent() throws Exception {
    Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/%s", SINGLE_DEVICE_UUID))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(SystemMapper.getMapper().readValue(jsonFixture("fixtures/current_message_single_device.json"),
                    IncomingMessageList.class),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat("Good Response", response.getStatus(), is(equalTo(200)));

    ArgumentCaptor<Envelope> captor = ArgumentCaptor.forClass(Envelope.class);
    verify(messageSender, times(1)).sendMessage(any(Account.class), any(Device.class), captor.capture(), eq(false));

    assertTrue(captor.getValue().hasSourceUuid());
    assertTrue(captor.getValue().hasSourceDevice());
    assertTrue(captor.getValue().getUrgent());
  }

  @Test
  void testSingleDeviceCurrentNotUrgent() throws Exception {
    Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/%s", SINGLE_DEVICE_UUID))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(SystemMapper.getMapper().readValue(jsonFixture("fixtures/current_message_single_device_not_urgent.json"),
                    IncomingMessageList.class),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat("Good Response", response.getStatus(), is(equalTo(200)));

    ArgumentCaptor<Envelope> captor = ArgumentCaptor.forClass(Envelope.class);
    verify(messageSender, times(1)).sendMessage(any(Account.class), any(Device.class), captor.capture(), eq(false));

    assertTrue(captor.getValue().hasSourceUuid());
    assertTrue(captor.getValue().hasSourceDevice());
    assertFalse(captor.getValue().getUrgent());
  }

  @Test
  void testSingleDeviceCurrentByPni() throws Exception {
    Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/%s", SINGLE_DEVICE_PNI))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(SystemMapper.getMapper().readValue(jsonFixture("fixtures/current_message_single_device.json"),
                    IncomingMessageList.class),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat("Good Response", response.getStatus(), is(equalTo(200)));

    ArgumentCaptor<Envelope> captor = ArgumentCaptor.forClass(Envelope.class);
    verify(messageSender, times(1)).sendMessage(any(Account.class), any(Device.class), captor.capture(), eq(false));

    assertTrue(captor.getValue().hasSourceUuid());
    assertTrue(captor.getValue().hasSourceDevice());
  }

  @Test
  void testNullMessageInList() throws Exception {
    Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/%s", SINGLE_DEVICE_UUID))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(SystemMapper.getMapper().readValue(jsonFixture("fixtures/current_message_null_message_in_list.json"), IncomingMessageList.class),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat("Bad request", response.getStatus(), is(equalTo(422)));
  }

  @Test
  void testSingleDeviceCurrentUnidentified() throws Exception {
    Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/%s", SINGLE_DEVICE_UUID))
            .request()
            .header(OptionalAccess.UNIDENTIFIED, Base64.getEncoder().encodeToString(UNIDENTIFIED_ACCESS_BYTES))
            .put(Entity.entity(SystemMapper.getMapper().readValue(jsonFixture("fixtures/current_message_single_device.json"),
                    IncomingMessageList.class),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat("Good Response", response.getStatus(), is(equalTo(200)));

    ArgumentCaptor<Envelope> captor = ArgumentCaptor.forClass(Envelope.class);
    verify(messageSender, times(1)).sendMessage(any(Account.class), any(Device.class), captor.capture(), eq(false));

    assertFalse(captor.getValue().hasSourceUuid());
    assertFalse(captor.getValue().hasSourceDevice());
  }

  @Test
  void testSendBadAuth() throws Exception {
    Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/%s", SINGLE_DEVICE_UUID))
            .request()
            .put(Entity.entity(SystemMapper.getMapper().readValue(jsonFixture("fixtures/current_message_single_device.json"),
                    IncomingMessageList.class),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat("Good Response", response.getStatus(), is(equalTo(401)));
  }

  @Test
  void testMultiDeviceMissing() throws Exception {
    Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/%s", MULTI_DEVICE_UUID))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(SystemMapper.getMapper().readValue(jsonFixture("fixtures/current_message_single_device.json"),
                    IncomingMessageList.class),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat("Good Response Code", response.getStatus(), is(equalTo(409)));

    assertThat("Good Response Body",
               asJson(response.readEntity(MismatchedDevices.class)),
               is(equalTo(jsonFixture("fixtures/missing_device_response.json"))));

    verifyNoMoreInteractions(messageSender);
  }

  @Test
  void testMultiDeviceExtra() throws Exception {
    Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/%s", MULTI_DEVICE_UUID))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(SystemMapper.getMapper().readValue(jsonFixture("fixtures/current_message_extra_device.json"),
                    IncomingMessageList.class),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat("Good Response Code", response.getStatus(), is(equalTo(409)));

    assertThat("Good Response Body",
               asJson(response.readEntity(MismatchedDevices.class)),
               is(equalTo(jsonFixture("fixtures/missing_device_response2.json"))));

    verifyNoMoreInteractions(messageSender);
  }

  @Test
  void testMultiDevice() throws Exception {
    Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/%s", MULTI_DEVICE_UUID))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(SystemMapper.getMapper().readValue(jsonFixture("fixtures/current_message_multi_device.json"),
                    IncomingMessageList.class),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat("Good Response Code", response.getStatus(), is(equalTo(200)));

    final ArgumentCaptor<Envelope> envelopeCaptor = ArgumentCaptor.forClass(Envelope.class);

    verify(messageSender, times(2)).sendMessage(any(Account.class), any(Device.class), envelopeCaptor.capture(), eq(false));

    envelopeCaptor.getAllValues().forEach(envelope -> assertTrue(envelope.getUrgent()));
  }

  @Test
  void testMultiDeviceNotUrgent() throws Exception {
    Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/%s", MULTI_DEVICE_UUID))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(SystemMapper.getMapper().readValue(jsonFixture("fixtures/current_message_multi_device_not_urgent.json"),
                    IncomingMessageList.class),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat("Good Response Code", response.getStatus(), is(equalTo(200)));

    final ArgumentCaptor<Envelope> envelopeCaptor = ArgumentCaptor.forClass(Envelope.class);

    verify(messageSender, times(2)).sendMessage(any(Account.class), any(Device.class), envelopeCaptor.capture(), eq(false));

    envelopeCaptor.getAllValues().forEach(envelope -> assertFalse(envelope.getUrgent()));
  }

  @Test
  void testMultiDeviceByPni() throws Exception {
    Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/%s", MULTI_DEVICE_PNI))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(SystemMapper.getMapper().readValue(jsonFixture("fixtures/current_message_multi_device_pni.json"),
                    IncomingMessageList.class),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat("Good Response Code", response.getStatus(), is(equalTo(200)));

    verify(messageSender, times(2)).sendMessage(any(Account.class), any(Device.class), any(Envelope.class), eq(false));
  }

  @Test
  void testRegistrationIdMismatch() throws Exception {
    Response response =
        resources.getJerseyTest().target(String.format("/v1/messages/%s", MULTI_DEVICE_UUID))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(SystemMapper.getMapper().readValue(jsonFixture("fixtures/current_message_registration_id.json"),
                    IncomingMessageList.class),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat("Good Response Code", response.getStatus(), is(equalTo(410)));

    assertThat("Good Response Body",
               asJson(response.readEntity(StaleDevices.class)),
               is(equalTo(jsonFixture("fixtures/mismatched_registration_id.json"))));

    verifyNoMoreInteractions(messageSender);
  }

  @ParameterizedTest
  @MethodSource
  void testGetMessages(boolean receiveStories) {

    final long timestampOne = 313377;
    final long timestampTwo = 313388;

    final UUID messageGuidOne = UUID.randomUUID();
    final UUID messageGuidTwo = UUID.randomUUID();
    final UUID sourceUuid = UUID.randomUUID();

    final UUID updatedPniOne = UUID.randomUUID();

    List<Envelope> envelopes = List.of(
        generateEnvelope(messageGuidOne, Envelope.Type.CIPHERTEXT_VALUE, timestampOne, sourceUuid, 2,
            AuthHelper.VALID_UUID, updatedPniOne, "hi there".getBytes(), 0, false),
        generateEnvelope(messageGuidTwo, Envelope.Type.SERVER_DELIVERY_RECEIPT_VALUE, timestampTwo, sourceUuid, 2,
            AuthHelper.VALID_UUID, null, null, 0, true)
    );

    when(messagesManager.getMessagesForDevice(eq(AuthHelper.VALID_UUID), eq(1L), anyBoolean()))
        .thenReturn(Mono.just(new Pair<>(envelopes, false)));

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

    OutgoingMessageEntity first = messages.get(0);
    assertEquals(first.timestamp(), timestampOne);
    assertEquals(first.guid(), messageGuidOne);
    assertEquals(first.sourceUuid(), sourceUuid);
    assertEquals(updatedPniOne, first.updatedPni());

    if (receiveStories) {
      OutgoingMessageEntity second = messages.get(1);
      assertEquals(second.timestamp(), timestampTwo);
      assertEquals(second.guid(), messageGuidTwo);
      assertEquals(second.sourceUuid(), sourceUuid);
      assertNull(second.updatedPni());
    }

    verify(pushNotificationManager).handleMessagesRetrieved(AuthHelper.VALID_ACCOUNT, AuthHelper.VALID_DEVICE, userAgent);
  }

  private static Stream<Arguments> testGetMessages() {
    return Stream.of(
        Arguments.of(true),
        Arguments.of(false)
    );
  }

  @Test
  void testGetMessagesBadAuth() {
    final long timestampOne = 313377;
    final long timestampTwo = 313388;

    final List<Envelope> messages = List.of(
        generateEnvelope(UUID.randomUUID(), Envelope.Type.CIPHERTEXT_VALUE, timestampOne, UUID.randomUUID(), 2,
            AuthHelper.VALID_UUID, null, "hi there".getBytes(), 0),
        generateEnvelope(UUID.randomUUID(), Envelope.Type.SERVER_DELIVERY_RECEIPT_VALUE, timestampTwo,
            UUID.randomUUID(), 2, AuthHelper.VALID_UUID, null, null, 0)
    );

    when(messagesManager.getMessagesForDevice(eq(AuthHelper.VALID_UUID), eq(1L), anyBoolean()))
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
    long timestamp = System.currentTimeMillis();

    UUID sourceUuid = UUID.randomUUID();

    UUID uuid1 = UUID.randomUUID();
    when(messagesManager.delete(AuthHelper.VALID_UUID, 1, uuid1, null))
        .thenReturn(
            CompletableFuture.completedFuture(Optional.of(generateEnvelope(uuid1, Envelope.Type.CIPHERTEXT_VALUE,
                timestamp, sourceUuid, 1, AuthHelper.VALID_UUID, null, "hi".getBytes(), 0))));

    UUID uuid2 = UUID.randomUUID();
    when(messagesManager.delete(AuthHelper.VALID_UUID, 1, uuid2, null))
        .thenReturn(
            CompletableFuture.completedFuture(Optional.of(generateEnvelope(
                uuid2, Envelope.Type.SERVER_DELIVERY_RECEIPT_VALUE,
                System.currentTimeMillis(), sourceUuid, 1, AuthHelper.VALID_UUID, null, null, 0))));

    UUID uuid3 = UUID.randomUUID();
    when(messagesManager.delete(AuthHelper.VALID_UUID, 1, uuid3, null))
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    UUID uuid4 = UUID.randomUUID();
    when(messagesManager.delete(AuthHelper.VALID_UUID, 1, uuid4, null))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Oh No")));

    Response response = resources.getJerseyTest()
        .target(String.format("/v1/messages/uuid/%s", uuid1))
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .delete();

    assertThat("Good Response Code", response.getStatus(), is(equalTo(204)));
    verify(receiptSender).sendReceipt(eq(AuthHelper.VALID_UUID), eq(1L),
        eq(sourceUuid), eq(timestamp));

    response = resources.getJerseyTest()
        .target(String.format("/v1/messages/uuid/%s", uuid2))
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .delete();

    assertThat("Good Response Code", response.getStatus(), is(equalTo(204)));
    verifyNoMoreInteractions(receiptSender);

    response = resources.getJerseyTest()
        .target(String.format("/v1/messages/uuid/%s", uuid3))
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .delete();

    assertThat("Good Response Code", response.getStatus(), is(equalTo(204)));
    verifyNoMoreInteractions(receiptSender);

    response = resources.getJerseyTest()
        .target(String.format("/v1/messages/uuid/%s", uuid4))
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .delete();

    assertThat("Bad Response Code", response.getStatus(), is(equalTo(500)));
    verifyNoMoreInteractions(receiptSender);

  }

  @Test
  void testReportMessageByE164() {

    final String senderNumber = "+12125550001";
    final UUID senderAci = UUID.randomUUID();
    final UUID senderPni = UUID.randomUUID();
    UUID messageGuid = UUID.randomUUID();

    final Account account = mock(Account.class);
    when(account.getUuid()).thenReturn(senderAci);
    when(account.getNumber()).thenReturn(senderNumber);
    when(account.getPhoneNumberIdentifier()).thenReturn(senderPni);

    when(accountsManager.getByE164(senderNumber)).thenReturn(Optional.of(account));
    when(deletedAccountsManager.findDeletedAccountAci(senderNumber)).thenReturn(Optional.of(senderAci));
    when(accountsManager.getPhoneNumberIdentifier(senderNumber)).thenReturn(senderPni);

    Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/report/%s/%s", senderNumber, messageGuid))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .post(null);

    assertThat(response.getStatus(), is(equalTo(202)));

    verify(reportMessageManager).report(Optional.of(senderNumber), Optional.of(senderAci), Optional.of(senderPni),
        messageGuid, AuthHelper.VALID_UUID);
    verify(deletedAccountsManager, never()).findDeletedAccountE164(any(UUID.class));
    verify(accountsManager, never()).getPhoneNumberIdentifier(anyString());

    when(accountsManager.getByE164(senderNumber)).thenReturn(Optional.empty());
    messageGuid = UUID.randomUUID();

    response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/report/%s/%s", senderNumber, messageGuid))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .post(null);

    assertThat(response.getStatus(), is(equalTo(202)));

    verify(reportMessageManager).report(Optional.of(senderNumber), Optional.of(senderAci), Optional.of(senderPni),
        messageGuid, AuthHelper.VALID_UUID);
  }

  @Test
  void testReportMessageByAci() {

    final String senderNumber = "+12125550001";
    final UUID senderAci = UUID.randomUUID();
    final UUID senderPni = UUID.randomUUID();
    UUID messageGuid = UUID.randomUUID();

    final Account account = mock(Account.class);
    when(account.getUuid()).thenReturn(senderAci);
    when(account.getNumber()).thenReturn(senderNumber);
    when(account.getPhoneNumberIdentifier()).thenReturn(senderPni);

    when(accountsManager.getByAccountIdentifier(senderAci)).thenReturn(Optional.of(account));
    when(deletedAccountsManager.findDeletedAccountE164(senderAci)).thenReturn(Optional.of(senderNumber));
    when(accountsManager.getPhoneNumberIdentifier(senderNumber)).thenReturn(senderPni);

    Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/report/%s/%s", senderAci, messageGuid))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .post(null);

    assertThat(response.getStatus(), is(equalTo(202)));

    verify(reportMessageManager).report(Optional.of(senderNumber), Optional.of(senderAci), Optional.of(senderPni),
        messageGuid, AuthHelper.VALID_UUID);
    verify(deletedAccountsManager, never()).findDeletedAccountE164(any(UUID.class));
    verify(accountsManager, never()).getPhoneNumberIdentifier(anyString());

    when(accountsManager.getByAccountIdentifier(senderAci)).thenReturn(Optional.empty());

    messageGuid = UUID.randomUUID();

    response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/report/%s/%s", senderAci, messageGuid))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .post(null);

    assertThat(response.getStatus(), is(equalTo(202)));

    verify(reportMessageManager).report(Optional.of(senderNumber), Optional.of(senderAci), Optional.of(senderPni),
        messageGuid, AuthHelper.VALID_UUID);
  }

  @Test
  void testValidateContentLength() throws Exception {
    final int contentLength = Math.toIntExact(MessageController.MAX_MESSAGE_SIZE + 1);
    final byte[] contentBytes = new byte[contentLength];
    Arrays.fill(contentBytes, (byte) 1);

    Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/%s", SINGLE_DEVICE_UUID))
            .request()
            .header(OptionalAccess.UNIDENTIFIED, Base64.getEncoder().encodeToString(UNIDENTIFIED_ACCESS_BYTES))
            .put(Entity.entity(new IncomingMessageList(
                    List.of(new IncomingMessage(1, 1L, 1, new String(contentBytes))), false, true,
                    System.currentTimeMillis()),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat("Bad response", response.getStatus(), is(equalTo(413)));

    verify(messageSender, never()).sendMessage(any(Account.class), any(Device.class), any(Envelope.class),
        anyBoolean());
  }

  @ParameterizedTest
  @MethodSource
  void testValidateEnvelopeType(String payloadFilename, boolean expectOk) throws Exception {
    Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/%s", SINGLE_DEVICE_UUID))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .header(HttpHeaders.USER_AGENT, "Test-UA")
            .put(Entity.entity(SystemMapper.getMapper().readValue(jsonFixture(payloadFilename), IncomingMessageList.class),
                MediaType.APPLICATION_JSON_TYPE));

    if (expectOk) {
      assertEquals(200, response.getStatus());

      final ArgumentCaptor<Envelope> captor = ArgumentCaptor.forClass(Envelope.class);
      verify(messageSender).sendMessage(any(Account.class), any(Device.class), captor.capture(), eq(false));
    } else {
      assertEquals(400, response.getStatus());
      verify(messageSender, never()).sendMessage(any(), any(), any(), anyBoolean());
    }
  }

  private static Stream<Arguments> testValidateEnvelopeType() {
    return Stream.of(
        Arguments.of("fixtures/current_message_single_device.json", true),
        Arguments.of("fixtures/current_message_single_device_server_receipt_type.json", false)
    );
  }

  private static void writePayloadDeviceId(ByteBuffer bb, long deviceId) {
    long x = deviceId;
    // write the device-id in the 7-bit varint format we use, least significant bytes first.
    do {
      long b = x & 0x7f;
      x = x >>> 7;
      if (x != 0) b |= 0x80;
      bb.put((byte)b);
    } while (x != 0);
  }

  private static void writeMultiPayloadRecipient(ByteBuffer bb, Recipient r) throws Exception {
    long msb = r.getUuid().getMostSignificantBits();
    long lsb = r.getUuid().getLeastSignificantBits();
    bb.putLong(msb);            // uuid (first 8 bytes)
    bb.putLong(lsb);            // uuid (last 8 bytes)
    writePayloadDeviceId(bb, r.getDeviceId()); // device id (1-9 bytes)
    bb.putShort((short) r.getRegistrationId()); // registration id (2 bytes)
    bb.put(r.getPerRecipientKeyMaterial()); // key material (48 bytes)
  }

  private static InputStream initializeMultiPayload(List<Recipient> recipients, byte[] buffer) throws Exception {
    // initialize a binary payload according to our wire format
    ByteBuffer bb = ByteBuffer.wrap(buffer);
    bb.order(ByteOrder.BIG_ENDIAN);

    // first write the header
    bb.put(MultiRecipientMessageProvider.VERSION); // version byte
    bb.put((byte)recipients.size());               // count varint

    Iterator<Recipient> it = recipients.iterator();
    while (it.hasNext()) {
      writeMultiPayloadRecipient(bb, it.next());
    }

    // now write the actual message body (empty for now)
    bb.put(new byte[39]);            // payload (variable but >= 32, 39 bytes here)

    // return the input stream
    return new ByteArrayInputStream(buffer, 0, bb.position());
  }

  @ParameterizedTest
  @MethodSource
  void testMultiRecipientMessage(UUID recipientUUID, boolean authorize, boolean isStory, boolean urgent) throws Exception {

    final List<Recipient> recipients;
    if (recipientUUID == MULTI_DEVICE_UUID) {
      recipients = List.of(
        new Recipient(MULTI_DEVICE_UUID, MULTI_DEVICE_ID1, MULTI_DEVICE_REG_ID1, new byte[48]),
        new Recipient(MULTI_DEVICE_UUID, MULTI_DEVICE_ID2, MULTI_DEVICE_REG_ID2, new byte[48])
      );
    } else {
      recipients = List.of(new Recipient(SINGLE_DEVICE_UUID, SINGLE_DEVICE_ID1, SINGLE_DEVICE_REG_ID1, new byte[48]));
    }

    // initialize our binary payload and create an input stream
    byte[] buffer = new byte[2048];
    //InputStream stream = initializeMultiPayload(recipientUUID, buffer);
    InputStream stream = initializeMultiPayload(recipients, buffer);

    // set up the entity to use in our PUT request
    Entity<InputStream> entity = Entity.entity(stream, MultiRecipientMessageProvider.MEDIA_TYPE);

    when(multiRecipientMessageExecutor.invokeAll(any()))
        .thenAnswer(answer -> {
          final List<Callable> tasks = answer.getArgument(0, List.class);
          tasks.forEach(c -> {
            try {
              c.call();
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          });
          return null;
        });

    // start building the request
    Invocation.Builder bldr = resources
        .getJerseyTest()
        .target("/v1/messages/multi_recipient")
        .queryParam("online", true)
        .queryParam("ts", 1663798405641L)
        .queryParam("story", isStory)
        .queryParam("urgent", urgent)
        .request()
        .header(HttpHeaders.USER_AGENT, "FIXME");

    // add access header if needed
    if (authorize) {
      String encodedBytes = Base64.getEncoder().encodeToString(UNIDENTIFIED_ACCESS_BYTES);
      bldr = bldr.header(OptionalAccess.UNIDENTIFIED, encodedBytes);
    }

    // make the PUT request
    Response response = bldr.put(entity);

    if (authorize) {
      ArgumentCaptor<Envelope> envelopeArgumentCaptor = ArgumentCaptor.forClass(Envelope.class);
      verify(messageSender, atLeastOnce()).sendMessage(any(), any(), envelopeArgumentCaptor.capture(), anyBoolean());
      assertEquals(urgent, envelopeArgumentCaptor.getValue().getUrgent());
    }

    // We have a 2x2x2 grid of possible situations based on:
    //   - recipient enabled stories?
    //   - sender is authorized?
    //   - message is a story?
    //
    // (urgent is not included in the grid because it has no effect
    // on any of the other settings.)

    if (recipientUUID == MULTI_DEVICE_UUID) {
      // This is the case where the recipient has enabled stories.
      if(isStory) {
        // We are sending a story, so we ignore access checks and expect this
        // to go out to both the recipient's devices.
        checkGoodMultiRecipientResponse(response, 2);
      } else {
        // We are not sending a story, so we need to do access checks.
        if (authorize) {
          // When authorized we send a message to the recipient's devices.
          checkGoodMultiRecipientResponse(response, 2);
        } else {
          // When forbidden, we return a 401 error.
          checkBadMultiRecipientResponse(response, 401);
        }
      }
    } else {
      // This is the case where the recipient has not enabled stories.
      if (isStory) {
        // We are sending a story, so we ignore access checks.
        // this recipient has one device.
        checkGoodMultiRecipientResponse(response, 1);
      } else {
        // We are not sending a story so check access.
        if (authorize) {
          // If allowed, send a message to the recipient's one device.
          checkGoodMultiRecipientResponse(response, 1);
        } else {
          // If forbidden, return a 401 error.
          checkBadMultiRecipientResponse(response, 401);
        }
      }
    }
  }

  // Arguments here are: recipient-UUID, is-authorized?, is-story?
  private static Stream<Arguments> testMultiRecipientMessage() {
    return Stream.of(
        Arguments.of(MULTI_DEVICE_UUID, false, true, true),
        Arguments.of(MULTI_DEVICE_UUID, false, false, true),
        Arguments.of(SINGLE_DEVICE_UUID, false, true, true),
        Arguments.of(SINGLE_DEVICE_UUID, false, false, true),
        Arguments.of(MULTI_DEVICE_UUID, true, true, true),
        Arguments.of(MULTI_DEVICE_UUID, true, false, true),
        Arguments.of(SINGLE_DEVICE_UUID, true, true, true),
        Arguments.of(SINGLE_DEVICE_UUID, true, false, true),
        Arguments.of(MULTI_DEVICE_UUID, false, true, false),
        Arguments.of(MULTI_DEVICE_UUID, false, false, false),
        Arguments.of(SINGLE_DEVICE_UUID, false, true, false),
        Arguments.of(SINGLE_DEVICE_UUID, false, false, false),
        Arguments.of(MULTI_DEVICE_UUID, true, true, false),
        Arguments.of(MULTI_DEVICE_UUID, true, false, false),
        Arguments.of(SINGLE_DEVICE_UUID, true, true, false),
        Arguments.of(SINGLE_DEVICE_UUID, true, false, false)
    );
  }

  @Test
  void testSendStoryToUnknownAccount() throws Exception {
    String accessBytes = Base64.getEncoder().encodeToString(UNIDENTIFIED_ACCESS_BYTES);
    String json = jsonFixture("fixtures/current_message_single_device.json");
    UUID unknownUUID = UUID.randomUUID();
    IncomingMessageList list = SystemMapper.getMapper().readValue(json, IncomingMessageList.class);
    Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/%s", unknownUUID))
            .queryParam("story", "true")
            .request()
            .header(OptionalAccess.UNIDENTIFIED, accessBytes)
            .put(Entity.entity(list, MediaType.APPLICATION_JSON_TYPE));

    assertThat("200 masks unknown recipient", response.getStatus(), is(equalTo(200)));
  }

  @ParameterizedTest
  @MethodSource
  void testSendMultiRecipientMessageToUnknownAccounts(boolean story, boolean known) throws Exception {

    final Recipient r1;
    if (known) {
      r1 = new Recipient(SINGLE_DEVICE_UUID, SINGLE_DEVICE_ID1, SINGLE_DEVICE_REG_ID1, new byte[48]);
    } else {
      r1 = new Recipient(UUID.randomUUID(), 999, 999, new byte[48]);
    }

    Recipient r2 = new Recipient(MULTI_DEVICE_UUID, MULTI_DEVICE_ID1, MULTI_DEVICE_REG_ID1, new byte[48]);
    Recipient r3 = new Recipient(MULTI_DEVICE_UUID, MULTI_DEVICE_ID2, MULTI_DEVICE_REG_ID2, new byte[48]);

    List<Recipient> recipients = List.of(r1, r2, r3);

    byte[] buffer = new byte[2048];
    InputStream stream = initializeMultiPayload(recipients, buffer);
    // set up the entity to use in our PUT request
    Entity<InputStream> entity = Entity.entity(stream, MultiRecipientMessageProvider.MEDIA_TYPE);

    // This looks weird, but there is a method to the madness.
    // new bytes[16] is equivalent to UNIDENTIFIED_ACCESS_BYTES ^ UNIDENTIFIED_ACCESS_BYTES
    // (i.e. we need to XOR all the access keys together)
    String accessBytes = Base64.getEncoder().encodeToString(new byte[16]);

    // start building the request
    Invocation.Builder bldr = resources
        .getJerseyTest()
        .target("/v1/messages/multi_recipient")
        .queryParam("online", true)
        .queryParam("ts", 1663798405641L)
        .queryParam("story", story)
        .request()
        .header(HttpHeaders.USER_AGENT, "Test User Agent")
        .header(OptionalAccess.UNIDENTIFIED, accessBytes);

    // make the PUT request
    Response response = bldr.put(entity);

    if (story || known) {
      // it's a story so we unconditionally get 200 ok
      assertEquals(200, response.getStatus());
    } else {
      // unknown recipient means 404 not found
      assertEquals(404, response.getStatus());
    }
  }

  private static Stream<Arguments> testSendMultiRecipientMessageToUnknownAccounts() {
    return Stream.of(
        Arguments.of(true, true),
        Arguments.of(true, false),
        Arguments.of(false, true),
        Arguments.of(false, false));
  }

  private void checkBadMultiRecipientResponse(Response response, int expectedCode) throws Exception {
    assertThat("Unexpected response", response.getStatus(), is(equalTo(expectedCode)));
    verify(messageSender, never()).sendMessage(any(), any(), any(), anyBoolean());
    verify(multiRecipientMessageExecutor, never()).invokeAll(any());
  }

  private void checkGoodMultiRecipientResponse(Response response, int expectedCount) throws Exception {
    assertThat("Unexpected response", response.getStatus(), is(equalTo(200)));
    ArgumentCaptor<List<Callable<Void>>> captor = ArgumentCaptor.forClass(List.class);
    verify(multiRecipientMessageExecutor, times(1)).invokeAll(captor.capture());
    assert (captor.getValue().size() == expectedCount);
    SendMultiRecipientMessageResponse smrmr = response.readEntity(SendMultiRecipientMessageResponse.class);
    assert (smrmr.getUUIDs404().isEmpty());
  }

  private static Envelope generateEnvelope(UUID guid, int type, long timestamp, UUID sourceUuid,
      int sourceDevice, UUID destinationUuid, UUID updatedPni, byte[] content, long serverTimestamp) {
    return generateEnvelope(guid, type, timestamp, sourceUuid, sourceDevice, destinationUuid, updatedPni, content, serverTimestamp, false);
  }

  private static Envelope generateEnvelope(UUID guid, int type, long timestamp, UUID sourceUuid,
      int sourceDevice, UUID destinationUuid, UUID updatedPni, byte[] content, long serverTimestamp, boolean story) {

    final MessageProtos.Envelope.Builder builder = MessageProtos.Envelope.newBuilder()
        .setType(MessageProtos.Envelope.Type.forNumber(type))
        .setTimestamp(timestamp)
        .setServerTimestamp(serverTimestamp)
        .setDestinationUuid(destinationUuid.toString())
        .setStory(story)
        .setServerGuid(guid.toString());

    if (sourceUuid != null) {
      builder.setSourceUuid(sourceUuid.toString());
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

  private static Recipient genRecipient(Random rng) {
    UUID u1 = UUID.randomUUID(); // non-null
    long d1 = rng.nextLong() & 0x3fffffffffffffffL + 1; // 1 to 4611686018427387903
    int dr1 = rng.nextInt() & 0xffff; // 0 to 65535
    byte[] perKeyBytes = new byte[48]; // size=48, non-null
    rng.nextBytes(perKeyBytes);
    return new Recipient(u1, d1, dr1, perKeyBytes);
  }

  private static void roundTripVarint(long expected, byte [] bytes) throws Exception {
    ByteBuffer bb = ByteBuffer.wrap(bytes);
    writePayloadDeviceId(bb, expected);
    InputStream stream = new ByteArrayInputStream(bytes, 0, bb.position());
    long got = MultiRecipientMessageProvider.readVarint(stream);
    assertEquals(expected, got, String.format("encoded as: %s", Arrays.toString(bytes)));
  }

  @Test
  void testVarintPayload() throws Exception {
    Random rng = new Random();
    byte[] bytes = new byte[12];

    // some static test cases
    for (long i = 1L; i <= 10L; i++) {
      roundTripVarint(i, bytes);
    }
    roundTripVarint(Long.MAX_VALUE, bytes);

    for (int i = 0; i < 1000; i++) {
      // we need to ensure positive device IDs
      long start = rng.nextLong() & Long.MAX_VALUE;
      if (start == 0L) start = 1L;

      // run the test for this case
      roundTripVarint(start, bytes);
    }
  }

  @Test
  void testMultiPayloadRoundtrip() throws Exception {
    Random rng = new java.util.Random();
    List<Recipient> expected = new LinkedList<>();
    for(int i = 0; i < 100; i++) {
      expected.add(genRecipient(rng));
    }

    byte[] buffer = new byte[100 + expected.size() * 100];
    InputStream entityStream = initializeMultiPayload(expected, buffer);
    MultiRecipientMessageProvider provider = new MultiRecipientMessageProvider();
    // the provider ignores the headers, java reflection, etc. so we don't use those here.
    MultiRecipientMessage res = provider.readFrom(null, null, null, null, null, entityStream);
    List<Recipient> got = Arrays.asList(res.getRecipients());

    assertEquals(expected, got);
  }


}
