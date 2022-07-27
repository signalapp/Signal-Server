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
import io.dropwizard.auth.PolymorphicAuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import javax.ws.rs.client.Entity;
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
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.entities.MismatchedDevices;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntity;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntityList;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;
import org.whispersystems.textsecuregcm.entities.StaleDevices;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.mappers.RateLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.push.ApnFallbackManager;
import org.whispersystems.textsecuregcm.push.MessageSender;
import org.whispersystems.textsecuregcm.push.ReceiptSender;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.DeletedAccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.storage.ReportMessageManager;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.SystemMapper;

@ExtendWith(DropwizardExtensionsSupport.class)
class MessageControllerTest {

  private static final String SINGLE_DEVICE_RECIPIENT = "+14151111111";
  private static final UUID   SINGLE_DEVICE_UUID      = UUID.randomUUID();
  private static final UUID   SINGLE_DEVICE_PNI       = UUID.randomUUID();

  private static final String MULTI_DEVICE_RECIPIENT = "+14152222222";
  private static final UUID MULTI_DEVICE_UUID = UUID.randomUUID();
  private static final UUID MULTI_DEVICE_PNI = UUID.randomUUID();

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
  private static final ApnFallbackManager apnFallbackManager = mock(ApnFallbackManager.class);
  private static final ReportMessageManager reportMessageManager = mock(ReportMessageManager.class);
  private static final ExecutorService multiRecipientMessageExecutor = mock(ExecutorService.class);

  private static final ResourceExtension resources = ResourceExtension.builder()
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(new PolymorphicAuthValueFactoryProvider.Binder<>(
          ImmutableSet.of(AuthenticatedAccount.class, DisabledPermittedAuthenticatedAccount.class)))
      .addProvider(RateLimitExceededExceptionMapper.class)
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(
          new MessageController(rateLimiters, messageSender, receiptSender, accountsManager, deletedAccountsManager,
              messagesManager, apnFallbackManager, reportMessageManager, multiRecipientMessageExecutor))
      .build();

  @BeforeEach
  void setup() {
    final List<Device> singleDeviceList = List.of(
        generateTestDevice(1, 111, 1111, new SignedPreKey(333, "baz", "boop"), System.currentTimeMillis(), System.currentTimeMillis())
    );

    final List<Device> multiDeviceList = List.of(
        generateTestDevice(1, 222, 2222, new SignedPreKey(111, "foo", "bar"), System.currentTimeMillis(), System.currentTimeMillis()),
        generateTestDevice(2, 333, 3333, new SignedPreKey(222, "oof", "rab"), System.currentTimeMillis(), System.currentTimeMillis()),
        generateTestDevice(3, 444, 4444, null, System.currentTimeMillis(), System.currentTimeMillis() - TimeUnit.DAYS.toMillis(31))
    );

    Account singleDeviceAccount  = AccountsHelper.generateTestAccount(SINGLE_DEVICE_RECIPIENT, SINGLE_DEVICE_UUID, SINGLE_DEVICE_PNI, singleDeviceList, "1234".getBytes());
    Account multiDeviceAccount   = AccountsHelper.generateTestAccount(MULTI_DEVICE_RECIPIENT, MULTI_DEVICE_UUID, MULTI_DEVICE_PNI, multiDeviceList, "1234".getBytes());
    internationalAccount         = AccountsHelper.generateTestAccount(INTERNATIONAL_RECIPIENT, INTERNATIONAL_UUID, UUID.randomUUID(), singleDeviceList, "1234".getBytes());

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
        apnFallbackManager,
        reportMessageManager
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

    assertTrue(captor.getValue().hasSource());
    assertTrue(captor.getValue().hasSourceDevice());
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

    assertTrue(captor.getValue().hasSource());
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
            .header(OptionalAccess.UNIDENTIFIED, Base64.getEncoder().encodeToString("1234".getBytes()))
            .put(Entity.entity(SystemMapper.getMapper().readValue(jsonFixture("fixtures/current_message_single_device.json"),
                    IncomingMessageList.class),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat("Good Response", response.getStatus(), is(equalTo(200)));

    ArgumentCaptor<Envelope> captor = ArgumentCaptor.forClass(Envelope.class);
    verify(messageSender, times(1)).sendMessage(any(Account.class), any(Device.class), captor.capture(), eq(false));

    assertFalse(captor.getValue().hasSource());
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

    verify(messageSender, times(2)).sendMessage(any(Account.class), any(Device.class), any(Envelope.class), eq(false));
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

  @Test
  void testGetMessages() {

    final long timestampOne = 313377;
    final long timestampTwo = 313388;

    final UUID messageGuidOne = UUID.randomUUID();
    final UUID sourceUuid = UUID.randomUUID();

    final UUID updatedPniOne = UUID.randomUUID();

    List<OutgoingMessageEntity> messages = new LinkedList<>() {{
      add(new OutgoingMessageEntity(messageGuidOne, Envelope.Type.CIPHERTEXT_VALUE, timestampOne, "+14152222222", sourceUuid, 2, AuthHelper.VALID_UUID, updatedPniOne, "hi there".getBytes(), 0));
      add(new OutgoingMessageEntity(null, Envelope.Type.SERVER_DELIVERY_RECEIPT_VALUE, timestampTwo, "+14152222222", sourceUuid, 2, AuthHelper.VALID_UUID, null, null, 0));
    }};

    OutgoingMessageEntityList messagesList = new OutgoingMessageEntityList(messages, false);

    when(messagesManager.getMessagesForDevice(eq(AuthHelper.VALID_UUID), eq(1L), anyString(), anyBoolean())).thenReturn(messagesList);

    OutgoingMessageEntityList response =
        resources.getJerseyTest().target("/v1/messages/")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .get(OutgoingMessageEntityList.class);

    assertEquals(response.getMessages().size(), 2);

    assertEquals(response.getMessages().get(0).getTimestamp(), timestampOne);
    assertEquals(response.getMessages().get(1).getTimestamp(), timestampTwo);

    assertEquals(response.getMessages().get(0).getGuid(), messageGuidOne);
    assertNull(response.getMessages().get(1).getGuid());

    assertEquals(response.getMessages().get(0).getSourceUuid(), sourceUuid);
    assertEquals(response.getMessages().get(1).getSourceUuid(), sourceUuid);

    assertEquals(updatedPniOne, response.getMessages().get(0).getUpdatedPni());
    assertNull(response.getMessages().get(1).getUpdatedPni());
  }

  @Test
  void testGetMessagesBadAuth() {
    final long timestampOne = 313377;
    final long timestampTwo = 313388;

    List<OutgoingMessageEntity> messages = new LinkedList<>() {{
      add(new OutgoingMessageEntity(UUID.randomUUID(), Envelope.Type.CIPHERTEXT_VALUE, timestampOne, "+14152222222", UUID.randomUUID(), 2, AuthHelper.VALID_UUID, null, "hi there".getBytes(), 0));
      add(new OutgoingMessageEntity(UUID.randomUUID(), Envelope.Type.SERVER_DELIVERY_RECEIPT_VALUE, timestampTwo, "+14152222222", UUID.randomUUID(), 2, AuthHelper.VALID_UUID, null, null, 0));
    }};

    OutgoingMessageEntityList messagesList = new OutgoingMessageEntityList(messages, false);

    when(messagesManager.getMessagesForDevice(eq(AuthHelper.VALID_UUID), eq(1L), anyString(), anyBoolean())).thenReturn(messagesList);

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
    when(messagesManager.delete(AuthHelper.VALID_UUID, 1, uuid1, null)).thenReturn(Optional.of(new OutgoingMessageEntity(
        uuid1, Envelope.Type.CIPHERTEXT_VALUE,
        timestamp, "+14152222222", sourceUuid, 1, AuthHelper.VALID_UUID, null, "hi".getBytes(), 0)));

    UUID uuid2 = UUID.randomUUID();
    when(messagesManager.delete(AuthHelper.VALID_UUID, 1, uuid2, null)).thenReturn(Optional.of(new OutgoingMessageEntity(
        uuid2, Envelope.Type.SERVER_DELIVERY_RECEIPT_VALUE,
        System.currentTimeMillis(), "+14152222222", sourceUuid, 1, AuthHelper.VALID_UUID, null, null, 0)));

    UUID uuid3 = UUID.randomUUID();
    when(messagesManager.delete(AuthHelper.VALID_UUID, 1, uuid3, null)).thenReturn(Optional.empty());

    Response response = resources.getJerseyTest()
        .target(String.format("/v1/messages/uuid/%s", uuid1))
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .delete();

    assertThat("Good Response Code", response.getStatus(), is(equalTo(204)));
    verify(receiptSender).sendReceipt(any(AuthenticatedAccount.class), eq(sourceUuid), eq(timestamp));

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
            .header(OptionalAccess.UNIDENTIFIED, Base64.getEncoder().encodeToString("1234".getBytes()))
            .put(Entity.entity(new IncomingMessageList(
                    List.of(new IncomingMessage(1, null, 1L, 1, new String(contentBytes))), false,
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
}
