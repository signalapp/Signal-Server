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
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
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

@ExtendWith(DropwizardExtensionsSupport.class)
class MessageControllerTest {

  private static final String SINGLE_DEVICE_RECIPIENT = "+14151111111";
  private static final UUID   SINGLE_DEVICE_UUID      = UUID.randomUUID();
  private static final UUID   SINGLE_DEVICE_PNI       = UUID.randomUUID();
  private static final int SINGLE_DEVICE_ID1 = 1;
  private static final int SINGLE_DEVICE_REG_ID1 = 111;

  private static final String MULTI_DEVICE_RECIPIENT = "+14152222222";
  private static final UUID MULTI_DEVICE_UUID = UUID.randomUUID();
  private static final UUID MULTI_DEVICE_PNI = UUID.randomUUID();
  private static final int MULTI_DEVICE_ID1 = 1;
  private static final int MULTI_DEVICE_ID2 = 2;
  private static final int MULTI_DEVICE_ID3 = 3;
  private static final int MULTI_DEVICE_REG_ID1 = 222;
  private static final int MULTI_DEVICE_REG_ID2 = 333;
  private static final int MULTI_DEVICE_REG_ID3 = 444;

  private static final byte[] UNIDENTIFIED_ACCESS_BYTES = "0123456789abcdef".getBytes();

  private static final String INTERNATIONAL_RECIPIENT = "+61123456789";
  private static final UUID INTERNATIONAL_UUID = UUID.randomUUID();

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
        .thenReturn(new Pair<>(envelopes, false));

    final String userAgent = "Test-UA";

    OutgoingMessageEntityList response =
        resources.getJerseyTest().target("/v1/messages/")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .header(Stories.X_SIGNAL_RECEIVE_STORIES, receiveStories ? "true" : "false")
            .header("USer-Agent", userAgent)
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
        .thenReturn(new Pair<>(messages, false));

    Response response =
        resources.getJerseyTest().target("/v1/messages/")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.INVALID_PASSWORD))
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .get();

    assertThat("Unauthorized response", response.getStatus(), is(equalTo(401)));
  }

  @Test
  void testDeleteMessages() throws Exception {
    long timestamp = System.currentTimeMillis();

    UUID sourceUuid = UUID.randomUUID();

    UUID uuid1 = UUID.randomUUID();
    when(messagesManager.delete(AuthHelper.VALID_UUID, 1, uuid1, null)).thenReturn(Optional.of(generateEnvelope(
        uuid1, Envelope.Type.CIPHERTEXT_VALUE,
        timestamp, sourceUuid, 1, AuthHelper.VALID_UUID, null, "hi".getBytes(), 0)));

    UUID uuid2 = UUID.randomUUID();
    when(messagesManager.delete(AuthHelper.VALID_UUID, 1, uuid2, null)).thenReturn(Optional.of(generateEnvelope(
        uuid2, Envelope.Type.SERVER_DELIVERY_RECEIPT_VALUE,
        System.currentTimeMillis(), sourceUuid, 1, AuthHelper.VALID_UUID, null, null, 0)));

    UUID uuid3 = UUID.randomUUID();
    when(messagesManager.delete(AuthHelper.VALID_UUID, 1, uuid3, null)).thenReturn(Optional.empty());

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
                    List.of(new IncomingMessage(1, 1L, 1, new String(contentBytes))), false, true, false,
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
            .header("User-Agent", "FIXME")
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

  private void writeMultiPayloadRecipient(ByteBuffer bb, long msb, long lsb, int deviceId, int regId) throws Exception {
    bb.putLong(msb);            // uuid (first 8 bytes)
    bb.putLong(lsb);            // uuid (last 8 bytes)
    int x = deviceId;
    // write the device-id in the 7-bit varint format we use, least significant bytes first.
    do {
      bb.put((byte)(x & 0x7f));
      x = x >>> 7;
    } while (x != 0);
    bb.putShort((short) regId); // registration id short
    bb.put(new byte[48]);       // key material (48 bytes)
  }

  private InputStream initializeMultiPayload(UUID recipientUUID, byte[] buffer) throws Exception {
    // initialize a binary payload according to our wire format
    ByteBuffer bb = ByteBuffer.wrap(buffer);
    bb.order(ByteOrder.BIG_ENDIAN);

    // determine how many recipient/device pairs we will be writing
    int count;
    if (recipientUUID == MULTI_DEVICE_UUID) { count = 2; }
    else if (recipientUUID == SINGLE_DEVICE_UUID) { count = 1; }
    else { throw new Exception("unknown UUID: " + recipientUUID); }

    // first write the header header
    bb.put(MultiRecipientMessageProvider.VERSION); // version byte
    bb.put((byte)count);                           // count varint, # of active devices for this user

    long msb = recipientUUID.getMostSignificantBits();
    long lsb = recipientUUID.getLeastSignificantBits();

    // write the recipient data for each recipient/device pair
    if (recipientUUID == MULTI_DEVICE_UUID) {
      writeMultiPayloadRecipient(bb, msb, lsb, MULTI_DEVICE_ID1, MULTI_DEVICE_REG_ID1);
      writeMultiPayloadRecipient(bb, msb, lsb, MULTI_DEVICE_ID2, MULTI_DEVICE_REG_ID2);
    } else {
      writeMultiPayloadRecipient(bb, msb, lsb, SINGLE_DEVICE_ID1, SINGLE_DEVICE_REG_ID1);
    }

    // now write the actual message body (empty for now)
    bb.put(new byte[39]);            // payload (variable but >= 32, 39 bytes here)

    // return the input stream
    return new ByteArrayInputStream(buffer, 0, bb.position());
  }

  @ParameterizedTest
  @MethodSource
  void testMultiRecipientMessage(UUID recipientUUID, boolean authorize, boolean isStory) throws Exception {

    // initialize our binary payload and create an input stream
    byte[] buffer = new byte[2048];
    InputStream stream = initializeMultiPayload(recipientUUID, buffer);

    // set up the entity to use in our PUT request
    Entity<InputStream> entity = Entity.entity(stream, MultiRecipientMessageProvider.MEDIA_TYPE);

    // start building the request
    Invocation.Builder bldr = resources
        .getJerseyTest()
        .target("/v1/messages/multi_recipient")
        .queryParam("online", true)
        .queryParam("ts", 1663798405641L)
        .queryParam("story", isStory)
        .request()
        .header("User-Agent", "FIXME");

    // add access header if needed
    if (authorize) {
      String encodedBytes = Base64.getEncoder().encodeToString(UNIDENTIFIED_ACCESS_BYTES);
      bldr = bldr.header(OptionalAccess.UNIDENTIFIED, encodedBytes);
    }

    // make the PUT request
    Response response = bldr.put(entity);

    // We have a 2x2x2 grid of possible situations based on:
    //   - recipient enabled stories?
    //   - sender is authorized?
    //   - message is a story?

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
        Arguments.of(MULTI_DEVICE_UUID, false, true),
        Arguments.of(MULTI_DEVICE_UUID, false, false),
        Arguments.of(SINGLE_DEVICE_UUID, false, true),
        Arguments.of(SINGLE_DEVICE_UUID, false, false),
        Arguments.of(MULTI_DEVICE_UUID, true, true),
        Arguments.of(MULTI_DEVICE_UUID, true, false),
        Arguments.of(SINGLE_DEVICE_UUID, true, true),
        Arguments.of(SINGLE_DEVICE_UUID, true, false)
    );
  }

  private void checkBadMultiRecipientResponse(Response response, int expectedCode) throws Exception {
    assertThat("Unexpected response", response.getStatus(), is(equalTo(expectedCode)));
    verify(messageSender, never()).sendMessage(any(), any(), any(), anyBoolean());
    verify(multiRecipientMessageExecutor, never()).invokeAll(any());
  }

  private void checkGoodMultiRecipientResponse(Response response, int expectedCount) throws Exception {
    assertThat("Unexpected response", response.getStatus(), is(equalTo(200)));
    verify(messageSender, never()).sendMessage(any(), any(), any(), anyBoolean());
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
}
