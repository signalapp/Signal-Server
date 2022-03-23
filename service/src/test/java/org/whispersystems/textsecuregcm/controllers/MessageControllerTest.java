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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.vdurmont.semver4j.Semver;
import io.dropwizard.auth.PolymorphicAuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.assertj.core.api.Assertions;
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
import org.whispersystems.textsecuregcm.providers.MultiDeviceMessageListProvider;
import org.whispersystems.textsecuregcm.push.ApnFallbackManager;
import org.whispersystems.textsecuregcm.push.MessageSender;
import org.whispersystems.textsecuregcm.push.ReceiptSender;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.storage.ReportMessageManager;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.Pair;

@ExtendWith(DropwizardExtensionsSupport.class)
class MessageControllerTest {

  private static final String SINGLE_DEVICE_RECIPIENT = "+14151111111";
  private static final UUID   SINGLE_DEVICE_UUID      = UUID.randomUUID();
  private static final UUID   SINGLE_DEVICE_PNI       = UUID.randomUUID();

  private static final String MULTI_DEVICE_RECIPIENT  = "+14152222222";
  private static final UUID   MULTI_DEVICE_UUID       = UUID.randomUUID();

  private static final String INTERNATIONAL_RECIPIENT = "+61123456789";
  private static final UUID   INTERNATIONAL_UUID      = UUID.randomUUID();

  private Account internationalAccount;

  @SuppressWarnings("unchecked")
  private static final RedisAdvancedClusterCommands<String, String> redisCommands  = mock(RedisAdvancedClusterCommands.class);

  private static final MessageSender               messageSender               = mock(MessageSender.class);
  private static final ReceiptSender               receiptSender               = mock(ReceiptSender.class);
  private static final AccountsManager             accountsManager             = mock(AccountsManager.class);
  private static final MessagesManager             messagesManager             = mock(MessagesManager.class);
  private static final RateLimiters                rateLimiters                = mock(RateLimiters.class);
  private static final RateLimiter                 rateLimiter                 = mock(RateLimiter.class);
  private static final ApnFallbackManager          apnFallbackManager          = mock(ApnFallbackManager.class);
  private static final ReportMessageManager reportMessageManager = mock(ReportMessageManager.class);
  private static final ExecutorService multiRecipientMessageExecutor = mock(ExecutorService.class);

  private static final ObjectMapper mapper = new ObjectMapper();

  private static final ResourceExtension resources = ResourceExtension.builder()
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(new PolymorphicAuthValueFactoryProvider.Binder<>(
          ImmutableSet.of(AuthenticatedAccount.class, DisabledPermittedAuthenticatedAccount.class)))
      .addProvider(RateLimitExceededExceptionMapper.class)
      .addProvider(MultiDeviceMessageListProvider.class)
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(new MessageController(rateLimiters, messageSender, receiptSender, accountsManager,
          messagesManager, apnFallbackManager, reportMessageManager, multiRecipientMessageExecutor))
      .build();

  @BeforeEach
  void setup() {
    Set<Device> singleDeviceList = new HashSet<>() {{
      add(new Device(1, null, "foo", "bar",
          "isgcm", null, null, false, 111, new SignedPreKey(333, "baz", "boop"), System.currentTimeMillis(),
          System.currentTimeMillis(), "Test", 0, new Device.DeviceCapabilities(true, false, false, true, true, false,
          false, false, false, false, false)));
    }};

    Set<Device> multiDeviceList = new HashSet<>() {{
      add(new Device(1, null, "foo", "bar",
          "isgcm", null, null, false, 222, new SignedPreKey(111, "foo", "bar"), System.currentTimeMillis(),
          System.currentTimeMillis(), "Test", 0, new Device.DeviceCapabilities(true, false, false, true, false, false,
          false, false, false, false, false)));
      add(new Device(2, null, "foo", "bar",
          "isgcm", null, null, false, 333, new SignedPreKey(222, "oof", "rab"), System.currentTimeMillis(),
          System.currentTimeMillis(), "Test", 0, new Device.DeviceCapabilities(true, false, false, true, false, false,
          false, false, false, false, false)));
      add(new Device(3, null, "foo", "bar",
          "isgcm", null, null, false, 444, null, System.currentTimeMillis() - TimeUnit.DAYS.toMillis(31),
          System.currentTimeMillis(), "Test", 0, new Device.DeviceCapabilities(false, false, false, false, false, false,
          false, false, false, false, false)));
    }};

    Account singleDeviceAccount  = new Account(SINGLE_DEVICE_RECIPIENT, SINGLE_DEVICE_UUID, SINGLE_DEVICE_PNI, singleDeviceList, "1234".getBytes());
    Account multiDeviceAccount   = new Account(MULTI_DEVICE_RECIPIENT, MULTI_DEVICE_UUID, UUID.randomUUID(), multiDeviceList, "1234".getBytes());
    internationalAccount = new Account(INTERNATIONAL_RECIPIENT, INTERNATIONAL_UUID, UUID.randomUUID(), singleDeviceList, "1234".getBytes());

    when(accountsManager.getByAccountIdentifier(eq(SINGLE_DEVICE_UUID))).thenReturn(Optional.of(singleDeviceAccount));
    when(accountsManager.getByPhoneNumberIdentifier(SINGLE_DEVICE_PNI)).thenReturn(Optional.of(singleDeviceAccount));
    when(accountsManager.getByAccountIdentifier(eq(MULTI_DEVICE_UUID))).thenReturn(Optional.of(multiDeviceAccount));
    when(accountsManager.getByAccountIdentifier(INTERNATIONAL_UUID)).thenReturn(Optional.of(internationalAccount));

    when(rateLimiters.getMessagesLimiter()).thenReturn(rateLimiter);
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

  private static Stream<Entity<?>> currentMessageSingleDevicePayloads() {
    ByteArrayOutputStream messageStream = new ByteArrayOutputStream();
    messageStream.write(1); // version
    messageStream.write(1); // count
    messageStream.write(1); // device ID
    messageStream.writeBytes(new byte[] { (byte)0, (byte)111 }); // registration ID
    messageStream.write(1); // message type
    messageStream.write(3); // message length
    messageStream.writeBytes(new byte[] { (byte)1, (byte)2, (byte)3 }); // message contents

    try {
      return Stream.of(
        Entity.entity(mapper.readValue(jsonFixture("fixtures/current_message_single_device.json"),
                                      IncomingMessageList.class),
                      MediaType.APPLICATION_JSON_TYPE),
        Entity.entity(messageStream.toByteArray(), MultiDeviceMessageListProvider.MEDIA_TYPE)
      );
    } catch (Exception e) {
      throw new AssertionError(e);
    }
  }

  @ParameterizedTest
  @MethodSource("currentMessageSingleDevicePayloads")
  void testSendFromDisabledAccount(Entity<?> payload) throws Exception {
    Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/%s", SINGLE_DEVICE_UUID))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.DISABLED_UUID, AuthHelper.DISABLED_PASSWORD))
            .put(payload);

    assertThat("Unauthorized response", response.getStatus(), is(equalTo(401)));
  }

  @ParameterizedTest
  @MethodSource("currentMessageSingleDevicePayloads")
  void testSingleDeviceCurrent(Entity<?> payload) throws Exception {
    Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/%s", SINGLE_DEVICE_UUID))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(payload);

    assertThat("Good Response", response.getStatus(), is(equalTo(200)));

    ArgumentCaptor<Envelope> captor = ArgumentCaptor.forClass(Envelope.class);
    verify(messageSender, times(1)).sendMessage(any(Account.class), any(Device.class), captor.capture(), eq(false));

    assertTrue(captor.getValue().hasSource());
    assertTrue(captor.getValue().hasSourceDevice());
  }

  @ParameterizedTest
  @MethodSource
  void testSingleDeviceCurrentBadType(final String userAgentString, final boolean expectAcceptMessage) throws Exception {
    Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/%s", SINGLE_DEVICE_UUID))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .header("User-Agent", userAgentString)
            .put(Entity.entity(mapper.readValue(jsonFixture("fixtures/current_message_single_device_bad_type.json"), IncomingMessageList.class),
                MediaType.APPLICATION_JSON_TYPE));

    if (expectAcceptMessage) {
      assertEquals(200, response.getStatus());

      final ArgumentCaptor<Envelope> captor = ArgumentCaptor.forClass(Envelope.class);
      verify(messageSender).sendMessage(any(Account.class), any(Device.class), captor.capture(), eq(false));
    } else {
      assertEquals(400, response.getStatus());
      verify(messageSender, never()).sendMessage(any(), any(), any(), anyBoolean());
    }
  }

  private static Stream<Arguments> testSingleDeviceCurrentBadType() {
    return Stream.of(
        Arguments.of(String.format("Signal-iOS/%s iOS/14.2", MessageController.FIRST_IOS_VERSION_WITH_INCORRECT_ENVELOPE_TYPE), true),
        Arguments.of(String.format("Signal-iOS/%s iOS/14.2", MessageController.FIRST_IOS_VERSION_WITH_INCORRECT_ENVELOPE_TYPE.nextPatch()), true),
        Arguments.of(String.format("Signal-iOS/%s iOS/14.2", new Semver("5.22.0.38")), true),
        Arguments.of(String.format("Signal-iOS/%s iOS/14.2", MessageController.IOS_VERSION_WITH_FIXED_ENVELOPE_TYPE.withIncMinor(-1)), true),
        Arguments.of(String.format("Signal-iOS/%s iOS/14.2", MessageController.IOS_VERSION_WITH_FIXED_ENVELOPE_TYPE), false)
    );
  }

  @ParameterizedTest
  @MethodSource("currentMessageSingleDevicePayloads")
  void testSingleDeviceCurrentByPni(Entity<?> payload) throws Exception {
    Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/%s", SINGLE_DEVICE_PNI))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(payload);

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
            .put(Entity.entity(mapper.readValue(jsonFixture("fixtures/current_message_null_message_in_list.json"), IncomingMessageList.class),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat("Bad request", response.getStatus(), is(equalTo(422)));
  }

  @ParameterizedTest
  @MethodSource("currentMessageSingleDevicePayloads")
  void testSingleDeviceCurrentUnidentified(Entity<?> payload) throws Exception {
    Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/%s", SINGLE_DEVICE_UUID))
            .request()
            .header(OptionalAccess.UNIDENTIFIED, Base64.getEncoder().encodeToString("1234".getBytes()))
            .put(payload);

    assertThat("Good Response", response.getStatus(), is(equalTo(200)));

    ArgumentCaptor<Envelope> captor = ArgumentCaptor.forClass(Envelope.class);
    verify(messageSender, times(1)).sendMessage(any(Account.class), any(Device.class), captor.capture(), eq(false));

    assertFalse(captor.getValue().hasSource());
    assertFalse(captor.getValue().hasSourceDevice());
  }

  @ParameterizedTest
  @MethodSource("currentMessageSingleDevicePayloads")
  void testSendBadAuth(Entity<?> payload) throws Exception {
    Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/%s", SINGLE_DEVICE_UUID))
            .request()
            .put(payload);

    assertThat("Good Response", response.getStatus(), is(equalTo(401)));
  }

  @ParameterizedTest
  @MethodSource("currentMessageSingleDevicePayloads")
  void testMultiDeviceMissing(Entity<?> payload) throws Exception {
    Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/%s", MULTI_DEVICE_UUID))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(payload);

    assertThat("Good Response Code", response.getStatus(), is(equalTo(409)));

    assertThat("Good Response Body",
               asJson(response.readEntity(MismatchedDevices.class)),
               is(equalTo(jsonFixture("fixtures/missing_device_response.json"))));

    verifyNoMoreInteractions(messageSender);
  }

  @ParameterizedTest
  @MethodSource
  void testMultiDeviceExtra(Entity<?> payload) throws Exception {
    Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/%s", MULTI_DEVICE_UUID))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(payload);

    assertThat("Good Response Code", response.getStatus(), is(equalTo(409)));

    assertThat("Good Response Body",
               asJson(response.readEntity(MismatchedDevices.class)),
               is(equalTo(jsonFixture("fixtures/missing_device_response2.json"))));

    verifyNoMoreInteractions(messageSender);
  }

  private static Stream<Entity<?>> testMultiDeviceExtra() {
    ByteArrayOutputStream messageStream = new ByteArrayOutputStream();
    messageStream.write(1); // version
    messageStream.write(2); // count

    messageStream.write(1); // device ID
    messageStream.writeBytes(new byte[] { (byte)0, (byte)111 }); // registration ID
    messageStream.write(1); // message type
    messageStream.write(3); // message length
    messageStream.writeBytes(new byte[] { (byte)1, (byte)2, (byte)3 }); // message contents

    messageStream.write(3); // device ID
    messageStream.writeBytes(new byte[] { (byte)0, (byte)111 }); // registration ID
    messageStream.write(1); // message type
    messageStream.write(3); // message length
    messageStream.writeBytes(new byte[] { (byte)1, (byte)2, (byte)3 }); // message contents

    try {
      return Stream.of(
        Entity.entity(mapper.readValue(jsonFixture("fixtures/current_message_extra_device.json"),
                                      IncomingMessageList.class),
                      MediaType.APPLICATION_JSON_TYPE),
        Entity.entity(messageStream.toByteArray(), MultiDeviceMessageListProvider.MEDIA_TYPE)
      );
    } catch (Exception e) {
      throw new AssertionError(e);
    }
  }

  @ParameterizedTest
  @MethodSource
  void testMultiDevice(Entity<?> payload) throws Exception {
    Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/%s", MULTI_DEVICE_UUID))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(payload);

    assertThat("Good Response Code", response.getStatus(), is(equalTo(200)));

    verify(messageSender, times(2)).sendMessage(any(Account.class), any(Device.class), any(Envelope.class), eq(false));
  }

  private static Stream<Entity<?>> testMultiDevice() {
    ByteArrayOutputStream messageStream = new ByteArrayOutputStream();
    messageStream.write(1); // version
    messageStream.write(2); // count

    messageStream.write(1); // device ID
    messageStream.writeBytes(new byte[] { (byte)0, (byte)222 }); // registration ID
    messageStream.write(1); // message type
    messageStream.write(3); // message length
    messageStream.writeBytes(new byte[] { (byte)1, (byte)2, (byte)3 }); // message contents

    messageStream.write(2); // device ID
    messageStream.writeBytes(new byte[] { (byte)1, (byte)77 }); // registration ID (333 = 1 * 256 + 77)
    messageStream.write(1); // message type
    messageStream.write(3); // message length
    messageStream.writeBytes(new byte[] { (byte)1, (byte)2, (byte)3 }); // message contents

    try {
      return Stream.of(
        Entity.entity(mapper.readValue(jsonFixture("fixtures/current_message_multi_device.json"),
                                      IncomingMessageList.class),
                      MediaType.APPLICATION_JSON_TYPE),
        Entity.entity(messageStream.toByteArray(), MultiDeviceMessageListProvider.MEDIA_TYPE)
      );
    } catch (Exception e) {
      throw new AssertionError(e);
    }
  }

  @ParameterizedTest
  @MethodSource
  void testRegistrationIdMismatch(Entity<?> payload) throws Exception {
    Response response =
        resources.getJerseyTest().target(String.format("/v1/messages/%s", MULTI_DEVICE_UUID))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(payload);

    assertThat("Good Response Code", response.getStatus(), is(equalTo(410)));

    assertThat("Good Response Body",
               asJson(response.readEntity(StaleDevices.class)),
               is(equalTo(jsonFixture("fixtures/mismatched_registration_id.json"))));

    verifyNoMoreInteractions(messageSender);
  }

  private static Stream<Entity<?>> testRegistrationIdMismatch() {
    ByteArrayOutputStream messageStream = new ByteArrayOutputStream();
    messageStream.write(1); // version
    messageStream.write(2); // count

    messageStream.write(1); // device ID
    messageStream.writeBytes(new byte[] { (byte)0, (byte)222 }); // registration ID
    messageStream.write(1); // message type
    messageStream.write(3); // message length
    messageStream.writeBytes(new byte[] { (byte)1, (byte)2, (byte)3 }); // message contents

    messageStream.write(2); // device ID
    messageStream.writeBytes(new byte[] { (byte)0, (byte)77 }); // wrong registration ID
    messageStream.write(1); // message type
    messageStream.write(3); // message length
    messageStream.writeBytes(new byte[] { (byte)1, (byte)2, (byte)3 }); // message contents

    try {
      return Stream.of(
        Entity.entity(mapper.readValue(jsonFixture("fixtures/current_message_registration_id.json"),
                                      IncomingMessageList.class),
                      MediaType.APPLICATION_JSON_TYPE),
        Entity.entity(messageStream.toByteArray(), MultiDeviceMessageListProvider.MEDIA_TYPE)
      );
    } catch (Exception e) {
      throw new AssertionError(e);
    }
  }

  @Test
  void testGetMessages() throws Exception {

    final long timestampOne = 313377;
    final long timestampTwo = 313388;

    final UUID messageGuidOne = UUID.randomUUID();
    final UUID sourceUuid     = UUID.randomUUID();

    List<OutgoingMessageEntity> messages = new LinkedList<>() {{
      add(new OutgoingMessageEntity(messageGuidOne, Envelope.Type.CIPHERTEXT_VALUE, timestampOne, "+14152222222", sourceUuid, 2, AuthHelper.VALID_UUID, "hi there".getBytes(), 0));
      add(new OutgoingMessageEntity(null, Envelope.Type.SERVER_DELIVERY_RECEIPT_VALUE, timestampTwo, "+14152222222", sourceUuid, 2, AuthHelper.VALID_UUID, null, 0));
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
  }

  @Test
  void testGetMessagesBadAuth() throws Exception {
    final long timestampOne = 313377;
    final long timestampTwo = 313388;

    List<OutgoingMessageEntity> messages = new LinkedList<OutgoingMessageEntity>() {{
      add(new OutgoingMessageEntity(UUID.randomUUID(), Envelope.Type.CIPHERTEXT_VALUE, timestampOne, "+14152222222", UUID.randomUUID(), 2, AuthHelper.VALID_UUID, "hi there".getBytes(), 0));
      add(new OutgoingMessageEntity(UUID.randomUUID(), Envelope.Type.SERVER_DELIVERY_RECEIPT_VALUE, timestampTwo, "+14152222222", UUID.randomUUID(), 2, AuthHelper.VALID_UUID, null, 0));
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
    when(messagesManager.delete(AuthHelper.VALID_UUID, 1, uuid1)).thenReturn(Optional.of(new OutgoingMessageEntity(
        uuid1, Envelope.Type.CIPHERTEXT_VALUE,
        timestamp, "+14152222222", sourceUuid, 1, AuthHelper.VALID_UUID, "hi".getBytes(), 0)));

    UUID uuid2 = UUID.randomUUID();
    when(messagesManager.delete(AuthHelper.VALID_UUID, 1, uuid2)).thenReturn(Optional.of(new OutgoingMessageEntity(
        uuid2, Envelope.Type.SERVER_DELIVERY_RECEIPT_VALUE,
        System.currentTimeMillis(), "+14152222222", sourceUuid, 1, AuthHelper.VALID_UUID, null, 0)));

    UUID uuid3 = UUID.randomUUID();
    when(messagesManager.delete(AuthHelper.VALID_UUID, 1, uuid3)).thenReturn(Optional.empty());

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
  void testReportMessage() {

    final String senderNumber = "+12125550001";
    final UUID messageGuid = UUID.randomUUID();

    final Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/report/%s/%s", senderNumber, messageGuid))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .post(null);

    assertThat(response.getStatus(), is(equalTo(202)));

    verify(reportMessageManager).report(senderNumber, messageGuid, AuthHelper.VALID_UUID);
  }

  @ParameterizedTest
  @MethodSource
  void testValidateContentLength(Entity<?> payload) throws Exception {
    Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/%s", SINGLE_DEVICE_UUID))
            .request()
            .header(OptionalAccess.UNIDENTIFIED, Base64.getEncoder().encodeToString("1234".getBytes()))
            .put(payload);

    assertThat("Bad response", response.getStatus(), is(equalTo(413)));

    verify(messageSender, never()).sendMessage(any(Account.class), any(Device.class), any(Envelope.class),
        anyBoolean());
  }

  private static Stream<Entity<?>> testValidateContentLength() {
    final int contentLength = Math.toIntExact(MessageController.MAX_MESSAGE_SIZE + 1);
    ByteArrayOutputStream messageStream = new ByteArrayOutputStream();
    messageStream.write(1); // version
    messageStream.write(1); // count
    messageStream.write(1); // device ID
    messageStream.writeBytes(new byte[]{(byte) 0, (byte) 111}); // registration ID
    messageStream.write(1); // message type
    writeVarint(contentLength, messageStream); // message length
    final byte[] contentBytes = new byte[contentLength];
    Arrays.fill(contentBytes, (byte) 1);
    messageStream.writeBytes(contentBytes); // message contents

    try {
      return Stream.of(
          Entity.entity(new IncomingMessageList(
                  List.of(new IncomingMessage(1, null, 1L, 1, new String(contentBytes))), false,
                  System.currentTimeMillis()),
              MediaType.APPLICATION_JSON_TYPE),
          Entity.entity(messageStream.toByteArray(), MultiDeviceMessageListProvider.MEDIA_TYPE)
      );
    } catch (Exception e) {
      throw new AssertionError(e);
    }
  }

  private static void writeVarint(int value, ByteArrayOutputStream outputStream) {
    while (true) {
      int bits = value & 0x7f;
      value >>>= 7;
      if (value == 0) {
        outputStream.write((byte) bits);
        return;
      }
      outputStream.write((byte) (bits | 0x80));
    }
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
            .put(Entity.entity(mapper.readValue(jsonFixture(payloadFilename), IncomingMessageList.class),
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

  static Account mockAccountWithDeviceAndRegId(Object... deviceAndRegistrationIds) {
    Account account = mock(Account.class);
    if (deviceAndRegistrationIds.length % 2 != 0) {
      throw new IllegalArgumentException("invalid number of arguments specified; must be even");
    }
    for (int i = 0; i < deviceAndRegistrationIds.length; i+=2) {
      if (!(deviceAndRegistrationIds[i] instanceof Long)) {
        throw new IllegalArgumentException("device id is not instance of long at index " + i);
      }
      if (!(deviceAndRegistrationIds[i + 1] instanceof Integer)) {
        throw new IllegalArgumentException("registration id is not instance of integer at index " + (i + 1));
      }
      Long deviceId = (Long) deviceAndRegistrationIds[i];
      Integer registrationId = (Integer) deviceAndRegistrationIds[i + 1];
      Device device = mock(Device.class);
      when(device.getRegistrationId()).thenReturn(registrationId);
      when(account.getDevice(deviceId)).thenReturn(Optional.of(device));
    }
    return account;
  }

  static Collection<Pair<Long, Integer>> deviceAndRegistrationIds(Object... deviceAndRegistrationIds) {
    final Collection<Pair<Long, Integer>> result = new HashSet<>(deviceAndRegistrationIds.length);
    if (deviceAndRegistrationIds.length % 2 != 0) {
      throw new IllegalArgumentException("invalid number of arguments specified; must be even");
    }
    for (int i = 0; i < deviceAndRegistrationIds.length; i += 2) {
      if (!(deviceAndRegistrationIds[i] instanceof Long)) {
        throw new IllegalArgumentException("device id is not instance of long at index " + i);
      }
      if (!(deviceAndRegistrationIds[i + 1] instanceof Integer)) {
        throw new IllegalArgumentException("registration id is not instance of integer at index " + (i + 1));
      }
      Long deviceId = (Long) deviceAndRegistrationIds[i];
      Integer registrationId = (Integer) deviceAndRegistrationIds[i + 1];
      result.add(new Pair<>(deviceId, registrationId));
    }
    return result;
  }

  static Stream<Arguments> validateRegistrationIdsSource() {
    return Stream.of(
        arguments(
            mockAccountWithDeviceAndRegId(1L, 0xFFFF, 2L, 0xDEAD, 3L, 0xBEEF),
            deviceAndRegistrationIds(1L, 0xFFFF, 2L, 0xDEAD, 3L, 0xBEEF),
            null),
        arguments(
            mockAccountWithDeviceAndRegId(1L, 42),
            deviceAndRegistrationIds(1L, 1492),
            Set.of(1L)),
        arguments(
            mockAccountWithDeviceAndRegId(1L, 42),
            deviceAndRegistrationIds(1L, 42),
            null),
        arguments(
            mockAccountWithDeviceAndRegId(1L, 42),
            deviceAndRegistrationIds(1L, 0),
            null),
        arguments(
            mockAccountWithDeviceAndRegId(1L, 42, 2L, 255),
            deviceAndRegistrationIds(1L, 0, 2L, 42),
            Set.of(2L)),
        arguments(
            mockAccountWithDeviceAndRegId(1L, 42, 2L, 256),
            deviceAndRegistrationIds(1L, 41, 2L, 257),
            Set.of(1L, 2L))
    );
  }

  @ParameterizedTest
  @MethodSource("validateRegistrationIdsSource")
  void testValidateRegistrationIds(
      Account account,
      Collection<Pair<Long, Integer>> deviceAndRegistrationIds,
      Set<Long> expectedStaleDeviceIds) throws Exception {
    if (expectedStaleDeviceIds != null) {
      Assertions.assertThat(assertThrows(StaleDevicesException.class, () -> {
        MessageController.validateRegistrationIds(account, deviceAndRegistrationIds.stream());
      }).getStaleDevices()).hasSameElementsAs(expectedStaleDeviceIds);
    } else {
      MessageController.validateRegistrationIds(account, deviceAndRegistrationIds.stream());
    }
  }

  static Account mockAccountWithDeviceAndEnabled(Object... deviceIdAndEnabled) {
    Account account = mock(Account.class);
    if (deviceIdAndEnabled.length % 2 != 0) {
      throw new IllegalArgumentException("invalid number of arguments specified; must be even");
    }
    final Set<Device> devices = new HashSet<>(deviceIdAndEnabled.length / 2);
    for (int i = 0; i < deviceIdAndEnabled.length; i+=2) {
      if (!(deviceIdAndEnabled[i] instanceof Long)) {
        throw new IllegalArgumentException("device id is not instance of long at index " + i);
      }
      if (!(deviceIdAndEnabled[i + 1] instanceof Boolean)) {
        throw new IllegalArgumentException("enabled is not instance of boolean at index " + (i + 1));
      }
      Long deviceId = (Long) deviceIdAndEnabled[i];
      Boolean enabled = (Boolean) deviceIdAndEnabled[i + 1];
      Device device = mock(Device.class);
      when(device.isEnabled()).thenReturn(enabled);
      when(device.getId()).thenReturn(deviceId);
      when(account.getDevice(deviceId)).thenReturn(Optional.of(device));
      devices.add(device);
    }
    when(account.getDevices()).thenReturn(devices);
    return account;
  }

  static Stream<Arguments> validateCompleteDeviceListSource() {
    return Stream.of(
        arguments(
            mockAccountWithDeviceAndEnabled(1L, true, 2L, false, 3L, true),
            Set.of(1L, 3L),
            null,
            null,
            false,
            null),
        arguments(
            mockAccountWithDeviceAndEnabled(1L, true, 2L, false, 3L, true),
            Set.of(1L, 2L, 3L),
            null,
            Set.of(2L),
            false,
            null),
        arguments(
            mockAccountWithDeviceAndEnabled(1L, true, 2L, false, 3L, true),
            Set.of(1L),
            Set.of(3L),
            null,
            false,
            null),
        arguments(
            mockAccountWithDeviceAndEnabled(1L, true, 2L, false, 3L, true),
            Set.of(1L, 2L),
            Set.of(3L),
            Set.of(2L),
            false,
            null),
        arguments(
            mockAccountWithDeviceAndEnabled(1L, true, 2L, false, 3L, true),
            Set.of(1L),
            Set.of(3L),
            Set.of(1L),
            true,
            1L
        ),
        arguments(
            mockAccountWithDeviceAndEnabled(1L, true, 2L, false, 3L, true),
            Set.of(2L),
            Set.of(3L),
            Set.of(2L),
            true,
            1L
        ),
        arguments(
            mockAccountWithDeviceAndEnabled(1L, true, 2L, false, 3L, true),
            Set.of(3L),
            null,
            null,
            true,
            1L
        )
    );
  }

  @ParameterizedTest
  @MethodSource("validateCompleteDeviceListSource")
  void testValidateCompleteDeviceList(
      Account account,
      Set<Long> deviceIds,
      Collection<Long> expectedMissingDeviceIds,
      Collection<Long> expectedExtraDeviceIds,
      boolean isSyncMessage,
      Long authenticatedDeviceId) throws Exception {
    if (expectedMissingDeviceIds != null || expectedExtraDeviceIds != null) {
      final MismatchedDevicesException mismatchedDevicesException = assertThrows(MismatchedDevicesException.class,
          () -> MessageController.validateCompleteDeviceList(account, deviceIds, isSyncMessage,
              Optional.ofNullable(authenticatedDeviceId)));
      if (expectedMissingDeviceIds != null) {
        Assertions.assertThat(mismatchedDevicesException.getMissingDevices())
            .hasSameElementsAs(expectedMissingDeviceIds);
      }
      if (expectedExtraDeviceIds != null) {
        Assertions.assertThat(mismatchedDevicesException.getExtraDevices()).hasSameElementsAs(expectedExtraDeviceIds);
      }
    } else {
      MessageController.validateCompleteDeviceList(account, deviceIds, isSyncMessage,
          Optional.ofNullable(authenticatedDeviceId));
    }
  }
}
