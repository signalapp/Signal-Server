package org.whispersystems.textsecuregcm.tests.controllers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.whispersystems.textsecuregcm.auth.AmbiguousIdentifier;
import org.whispersystems.textsecuregcm.auth.DisabledPermittedAccount;
import org.whispersystems.textsecuregcm.auth.OptionalAccess;
import org.whispersystems.textsecuregcm.controllers.MessageController;
import org.whispersystems.textsecuregcm.entities.IncomingMessageList;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.entities.MismatchedDevices;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntity;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntityList;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;
import org.whispersystems.textsecuregcm.entities.StaleDevices;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.push.ApnFallbackManager;
import org.whispersystems.textsecuregcm.push.PushSender;
import org.whispersystems.textsecuregcm.push.ReceiptSender;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.Base64;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import io.dropwizard.auth.PolymorphicAuthValueFactoryProvider;
import io.dropwizard.testing.junit.ResourceTestRule;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.whispersystems.textsecuregcm.tests.util.JsonHelpers.asJson;
import static org.whispersystems.textsecuregcm.tests.util.JsonHelpers.jsonFixture;

public class MessageControllerTest {

  private static final String SINGLE_DEVICE_RECIPIENT = "+14151111111";
  private static final UUID   SINGLE_DEVICE_UUID      = UUID.randomUUID();

  private static final String MULTI_DEVICE_RECIPIENT  = "+14152222222";
  private static final UUID   MULTI_DEVICE_UUID       = UUID.randomUUID();

  private  final PushSender             pushSender             = mock(PushSender.class            );
  private  final ReceiptSender          receiptSender          = mock(ReceiptSender.class);
  private  final AccountsManager        accountsManager        = mock(AccountsManager.class       );
  private  final MessagesManager        messagesManager        = mock(MessagesManager.class);
  private  final RateLimiters           rateLimiters           = mock(RateLimiters.class          );
  private  final RateLimiter            rateLimiter            = mock(RateLimiter.class           );
  private  final ApnFallbackManager     apnFallbackManager     = mock(ApnFallbackManager.class);

  private  final ObjectMapper mapper = new ObjectMapper();

  @Rule
  public final ResourceTestRule resources = ResourceTestRule.builder()
                                                            .addProvider(AuthHelper.getAuthFilter())
                                                            .addProvider(new PolymorphicAuthValueFactoryProvider.Binder<>(ImmutableSet.of(Account.class, DisabledPermittedAccount.class)))
                                                            .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
                                                            .addResource(new MessageController(rateLimiters, pushSender, receiptSender, accountsManager,
                                                                                               messagesManager, apnFallbackManager))
                                                            .build();


  @Before
  public void setup() throws Exception {
    Set<Device> singleDeviceList = new HashSet<Device>() {{
      add(new Device(1, null, "foo", "bar", "baz", "isgcm", null, null, false, 111, new SignedPreKey(333, "baz", "boop"), System.currentTimeMillis(), System.currentTimeMillis(), "Test", 0, new Device.DeviceCapabilities(true, true, true)));
    }};

    Set<Device> multiDeviceList = new HashSet<Device>() {{
      add(new Device(1, null, "foo", "bar", "baz", "isgcm", null, null, false, 222, new SignedPreKey(111, "foo", "bar"), System.currentTimeMillis(), System.currentTimeMillis(), "Test", 0, new Device.DeviceCapabilities(true, true, true)));
      add(new Device(2, null, "foo", "bar", "baz", "isgcm", null, null, false, 333, new SignedPreKey(222, "oof", "rab"), System.currentTimeMillis(), System.currentTimeMillis(), "Test", 0, new Device.DeviceCapabilities(true, true, true)));
      add(new Device(3, null, "foo", "bar", "baz", "isgcm", null, null, false, 444, null, System.currentTimeMillis() - TimeUnit.DAYS.toMillis(31), System.currentTimeMillis(), "Test", 0, new Device.DeviceCapabilities(false, false, false)));
    }};

    Account singleDeviceAccount = new Account(SINGLE_DEVICE_RECIPIENT, SINGLE_DEVICE_UUID, singleDeviceList, "1234".getBytes());
    Account multiDeviceAccount  = new Account(MULTI_DEVICE_RECIPIENT, MULTI_DEVICE_UUID, multiDeviceList, "1234".getBytes());

    when(accountsManager.get(eq(SINGLE_DEVICE_RECIPIENT))).thenReturn(Optional.of(singleDeviceAccount));
    when(accountsManager.get(argThat((ArgumentMatcher<AmbiguousIdentifier>) identifier -> identifier != null && identifier.hasNumber() && identifier.getNumber().equals(SINGLE_DEVICE_RECIPIENT)))).thenReturn(Optional.of(singleDeviceAccount));
    when(accountsManager.get(eq(MULTI_DEVICE_RECIPIENT))).thenReturn(Optional.of(multiDeviceAccount));
    when(accountsManager.get(argThat((ArgumentMatcher<AmbiguousIdentifier>) identifier -> identifier != null && identifier.hasNumber() && identifier.getNumber().equals(MULTI_DEVICE_RECIPIENT)))).thenReturn(Optional.of(multiDeviceAccount));

    when(rateLimiters.getMessagesLimiter()).thenReturn(rateLimiter);
  }

  @Test
  public synchronized void testSendFromDisabledAccount() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/messages/%s", SINGLE_DEVICE_RECIPIENT))
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.DISABLED_NUMBER, AuthHelper.DISABLED_PASSWORD))
                 .put(Entity.entity(mapper.readValue(jsonFixture("fixtures/current_message_single_device.json"), IncomingMessageList.class),
                                    MediaType.APPLICATION_JSON_TYPE));

    assertThat("Unauthorized response", response.getStatus(), is(equalTo(401)));
  }

  @Test
  public synchronized void testSingleDeviceCurrent() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/messages/%s", SINGLE_DEVICE_RECIPIENT))
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                 .put(Entity.entity(mapper.readValue(jsonFixture("fixtures/current_message_single_device.json"), IncomingMessageList.class),
                                    MediaType.APPLICATION_JSON_TYPE));

    assertThat("Good Response", response.getStatus(), is(equalTo(200)));

    ArgumentCaptor<Envelope> captor = ArgumentCaptor.forClass(Envelope.class);
    verify(pushSender, times(1)).sendMessage(any(Account.class), any(Device.class), captor.capture(), eq(false));

    assertTrue(captor.getValue().hasSource());
    assertTrue(captor.getValue().hasSourceDevice());
  }

  @Test
  public synchronized void testSingleDeviceCurrentUnidentified() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/messages/%s", SINGLE_DEVICE_RECIPIENT))
                 .request()
                 .header(OptionalAccess.UNIDENTIFIED, Base64.encodeBytes("1234".getBytes()))
                 .put(Entity.entity(mapper.readValue(jsonFixture("fixtures/current_message_single_device.json"), IncomingMessageList.class),
                                    MediaType.APPLICATION_JSON_TYPE));

    assertThat("Good Response", response.getStatus(), is(equalTo(200)));

    ArgumentCaptor<Envelope> captor = ArgumentCaptor.forClass(Envelope.class);
    verify(pushSender, times(1)).sendMessage(any(Account.class), any(Device.class), captor.capture(), eq(false));

    assertFalse(captor.getValue().hasSource());
    assertFalse(captor.getValue().hasSourceDevice());
  }


  @Test
  public synchronized void testSendBadAuth() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/messages/%s", SINGLE_DEVICE_RECIPIENT))
                 .request()
                 .put(Entity.entity(mapper.readValue(jsonFixture("fixtures/current_message_single_device.json"), IncomingMessageList.class),
                                    MediaType.APPLICATION_JSON_TYPE));

    assertThat("Good Response", response.getStatus(), is(equalTo(401)));
  }

  @Test
  public synchronized void testMultiDeviceMissing() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/messages/%s", MULTI_DEVICE_RECIPIENT))
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                 .put(Entity.entity(mapper.readValue(jsonFixture("fixtures/current_message_single_device.json"), IncomingMessageList.class),
                                    MediaType.APPLICATION_JSON_TYPE));

    assertThat("Good Response Code", response.getStatus(), is(equalTo(409)));

    assertThat("Good Response Body",
               asJson(response.readEntity(MismatchedDevices.class)),
               is(equalTo(jsonFixture("fixtures/missing_device_response.json"))));

    verifyNoMoreInteractions(pushSender);
  }

  @Test
  public synchronized void testMultiDeviceExtra() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/messages/%s", MULTI_DEVICE_RECIPIENT))
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                 .put(Entity.entity(mapper.readValue(jsonFixture("fixtures/current_message_extra_device.json"), IncomingMessageList.class),
                                    MediaType.APPLICATION_JSON_TYPE));

    assertThat("Good Response Code", response.getStatus(), is(equalTo(409)));

    assertThat("Good Response Body",
               asJson(response.readEntity(MismatchedDevices.class)),
               is(equalTo(jsonFixture("fixtures/missing_device_response2.json"))));

    verifyNoMoreInteractions(pushSender);
  }

  @Test
  public synchronized void testMultiDevice() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/messages/%s", MULTI_DEVICE_RECIPIENT))
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                 .put(Entity.entity(mapper.readValue(jsonFixture("fixtures/current_message_multi_device.json"), IncomingMessageList.class),
                                    MediaType.APPLICATION_JSON_TYPE));

    assertThat("Good Response Code", response.getStatus(), is(equalTo(200)));

    verify(pushSender, times(2)).sendMessage(any(Account.class), any(Device.class), any(Envelope.class), eq(false));
  }

  @Test
  public synchronized void testRegistrationIdMismatch() throws Exception {
    Response response =
        resources.getJerseyTest().target(String.format("/v1/messages/%s", MULTI_DEVICE_RECIPIENT))
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                 .put(Entity.entity(mapper.readValue(jsonFixture("fixtures/current_message_registration_id.json"), IncomingMessageList.class),
                                    MediaType.APPLICATION_JSON_TYPE));

    assertThat("Good Response Code", response.getStatus(), is(equalTo(410)));

    assertThat("Good Response Body",
               asJson(response.readEntity(StaleDevices.class)),
               is(equalTo(jsonFixture("fixtures/mismatched_registration_id.json"))));

    verifyNoMoreInteractions(pushSender);

  }

  @Test
  public synchronized void testGetMessages() throws Exception {

    final long timestampOne = 313377;
    final long timestampTwo = 313388;

    final UUID messageGuidOne = UUID.randomUUID();
    final UUID sourceUuid     = UUID.randomUUID();

    List<OutgoingMessageEntity> messages = new LinkedList<>() {{
      add(new OutgoingMessageEntity(1L, false, messageGuidOne, Envelope.Type.CIPHERTEXT_VALUE, null, timestampOne, "+14152222222", sourceUuid, 2, "hi there".getBytes(), null, 0));
      add(new OutgoingMessageEntity(2L, false, null, Envelope.Type.RECEIPT_VALUE, null, timestampTwo, "+14152222222", sourceUuid, 2, null, null, 0));
    }};

    OutgoingMessageEntityList messagesList = new OutgoingMessageEntityList(messages, false);

    when(messagesManager.getMessagesForDevice(eq(AuthHelper.VALID_NUMBER), eq(1L))).thenReturn(messagesList);

    OutgoingMessageEntityList response =
        resources.getJerseyTest().target("/v1/messages/")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID.toString(), AuthHelper.VALID_PASSWORD))
                 .accept(MediaType.APPLICATION_JSON_TYPE)
                 .get(OutgoingMessageEntityList.class);


    assertEquals(response.getMessages().size(), 2);

    assertEquals(response.getMessages().get(0).getId(), 0);
    assertEquals(response.getMessages().get(1).getId(), 0);

    assertEquals(response.getMessages().get(0).getTimestamp(), timestampOne);
    assertEquals(response.getMessages().get(1).getTimestamp(), timestampTwo);

    assertEquals(response.getMessages().get(0).getGuid(), messageGuidOne);
    assertNull(response.getMessages().get(1).getGuid());

    assertEquals(response.getMessages().get(0).getSourceUuid(), sourceUuid);
    assertEquals(response.getMessages().get(1).getSourceUuid(), sourceUuid);
  }

  @Test
  public synchronized void testGetMessagesBadAuth() throws Exception {
    final long timestampOne = 313377;
    final long timestampTwo = 313388;

    List<OutgoingMessageEntity> messages = new LinkedList<OutgoingMessageEntity>() {{
      add(new OutgoingMessageEntity(1L, false, UUID.randomUUID(), Envelope.Type.CIPHERTEXT_VALUE, null, timestampOne, "+14152222222", UUID.randomUUID(), 2, "hi there".getBytes(), null, 0));
      add(new OutgoingMessageEntity(2L, false, UUID.randomUUID(), Envelope.Type.RECEIPT_VALUE, null, timestampTwo, "+14152222222", UUID.randomUUID(), 2, null, null, 0));
    }};

    OutgoingMessageEntityList messagesList = new OutgoingMessageEntityList(messages, false);

    when(messagesManager.getMessagesForDevice(eq(AuthHelper.VALID_NUMBER), eq(1L))).thenReturn(messagesList);

    Response response =
        resources.getJerseyTest().target("/v1/messages/")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID.toString(), AuthHelper.INVALID_PASSWORD))
                 .accept(MediaType.APPLICATION_JSON_TYPE)
                 .get();

    assertThat("Unauthorized response", response.getStatus(), is(equalTo(401)));
  }

  @Test
  public synchronized void testDeleteMessages() throws Exception {
    long timestamp = System.currentTimeMillis();

    UUID sourceUuid = UUID.randomUUID();

    when(messagesManager.delete(AuthHelper.VALID_NUMBER, 1, "+14152222222", 31337))
        .thenReturn(Optional.of(new OutgoingMessageEntity(31337L, true, null,
                                                          Envelope.Type.CIPHERTEXT_VALUE,
                                                          null, timestamp,
                                                          "+14152222222", sourceUuid, 1, "hi".getBytes(), null, 0)));

    when(messagesManager.delete(AuthHelper.VALID_NUMBER, 1, "+14152222222", 31338))
        .thenReturn(Optional.of(new OutgoingMessageEntity(31337L, true, null,
                                                          Envelope.Type.RECEIPT_VALUE,
                                                          null, System.currentTimeMillis(),
                                                          "+14152222222", sourceUuid, 1, null, null, 0)));


    when(messagesManager.delete(AuthHelper.VALID_NUMBER, 1, "+14152222222", 31339))
        .thenReturn(Optional.empty());

    Response response = resources.getJerseyTest()
                                 .target(String.format("/v1/messages/%s/%d", "+14152222222", 31337))
                                 .request()
                                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                                 .delete();

    assertThat("Good Response Code", response.getStatus(), is(equalTo(204)));
    verify(receiptSender).sendReceipt(any(Account.class), eq("+14152222222"), eq(timestamp));

    response = resources.getJerseyTest()
                        .target(String.format("/v1/messages/%s/%d", "+14152222222", 31338))
                        .request()
                        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID.toString(), AuthHelper.VALID_PASSWORD))
                        .delete();

    assertThat("Good Response Code", response.getStatus(), is(equalTo(204)));
    verifyNoMoreInteractions(receiptSender);

    response = resources.getJerseyTest()
                        .target(String.format("/v1/messages/%s/%d", "+14152222222", 31339))
                        .request()
                        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                        .delete();

    assertThat("Good Response Code", response.getStatus(), is(equalTo(204)));
    verifyNoMoreInteractions(receiptSender);

  }

}
