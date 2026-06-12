/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.whispersystems.textsecuregcm.filters.RemoteAddressFilter.REMOTE_ADDRESS_ATTRIBUTE_NAME;

import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.protobuf.ByteString;
import io.dropwizard.core.Application;
import io.dropwizard.core.Configuration;
import io.dropwizard.core.setup.Environment;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import java.net.URI;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.RandomStringUtils;
import org.eclipse.jetty.ee10.websocket.server.config.JettyWebSocketServletContainerInitializer;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.ArgumentCaptor;
import org.whispersystems.textsecuregcm.asn.AsnInfoProvider;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.DisconnectionRequestListener;
import org.whispersystems.textsecuregcm.auth.DisconnectionRequestManager;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.filters.PriorityFilter;
import org.whispersystems.textsecuregcm.filters.RemoteAddressFilter;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.limits.MessageDeliveryLoopMonitor;
import org.whispersystems.textsecuregcm.metrics.MessageMetrics;
import org.whispersystems.textsecuregcm.push.PushNotificationManager;
import org.whispersystems.textsecuregcm.push.PushNotificationScheduler;
import org.whispersystems.textsecuregcm.push.ReceiptSender;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.ClientReleaseManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.MessageStream;
import org.whispersystems.textsecuregcm.storage.MessageStreamEntry;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.tests.util.TestWebsocketListener;
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import org.whispersystems.websocket.WebSocketResourceProviderFactory;
import org.whispersystems.websocket.WebsocketHeaders;
import org.whispersystems.websocket.configuration.WebSocketConfiguration;
import org.whispersystems.websocket.setup.WebSocketEnvironment;
import reactor.adapter.JdkFlowAdapter;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

@ExtendWith(DropwizardExtensionsSupport.class)
@Timeout(value = 15, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class AuthenticatedConnectListenerIntegrationTest {

  @RegisterExtension
  static final DropwizardAppExtension<Configuration> DROPWIZARD_APP_EXTENSION =
      new DropwizardAppExtension<>(TestApplication.class);

  private static final UUID ACCOUNT_UUID = UUID.randomUUID();
  private static final byte DEVICE_ID = Device.PRIMARY_ID;
  private static final String E164 = PhoneNumberUtil.getInstance()
      .format(PhoneNumberUtil.getInstance().getExampleNumber("US"), PhoneNumberUtil.PhoneNumberFormat.E164);


  private static final DisconnectionRequestManager disconnectionRequestManager = mock(DisconnectionRequestManager.class);
  private static final MessagesManager messagesManager = mock(MessagesManager.class);
  private static final AccountsManager accountsManager = mock(AccountsManager.class);
  private static final Account account = mock(Account.class);
  private static final Device device = mock(Device.class);

  private WebSocketClient client;

  @BeforeEach
  void setUp() throws Exception {
    reset(messagesManager, disconnectionRequestManager, accountsManager, account, device);
    when(messagesManager.mayHaveMessages(any(), any())).thenReturn(CompletableFuture.completedFuture(false));
    when(account.getNumber()).thenReturn(E164);
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(ACCOUNT_UUID);
    when(account.getDevice(DEVICE_ID)).thenReturn(Optional.of(device));
    when(device.getId()).thenReturn(DEVICE_ID);
    when(accountsManager.getByAccountIdentifier(ACCOUNT_UUID)).thenReturn(Optional.of(account));

    client = new WebSocketClient();
    client.start();
  }

  @AfterEach
  void tearDown() throws Exception {
    client.stop();
  }

  public static class TestApplication extends Application<Configuration> {

    @Override
    public void run(final Configuration configuration, final Environment environment) {
      final AuthenticatedConnectListener connectListener = new AuthenticatedConnectListener(
          accountsManager,
          mock(ReceiptSender.class),
          messagesManager,
          new MessageMetrics(),
          mock(PushNotificationManager.class),
          mock(PushNotificationScheduler.class),
          disconnectionRequestManager,
          Schedulers.boundedElastic(),
          () -> mock(AsnInfoProvider.class),
          mock(ClientReleaseManager.class),
          mock(MessageDeliveryLoopMonitor.class),
          mock(ExperimentEnrollmentManager.class));

      final WebSocketEnvironment<AuthenticatedDevice> webSocketEnvironment =
          new WebSocketEnvironment<>(environment, new WebSocketConfiguration());

      webSocketEnvironment.setAuthenticator(_ ->
          Optional.of(new AuthenticatedDevice(ACCOUNT_UUID, DEVICE_ID, Instant.now())));
      webSocketEnvironment.setConnectListener(connectListener);

      final WebSocketResourceProviderFactory<AuthenticatedDevice> webSocketServlet =
          new WebSocketResourceProviderFactory<>(webSocketEnvironment, AuthenticatedDevice.class,
              REMOTE_ADDRESS_ATTRIBUTE_NAME);

      JettyWebSocketServletContainerInitializer.configure(environment.getApplicationContext(),
          (servletContext, container) -> {
            container.addMapping("/websocket", webSocketServlet);
            PriorityFilter.ensureFilter(servletContext, new RemoteAddressFilter());
          });
    }
  }

  @Test
  void messagesDeliveredOnlyWhenHeaderAbsent() throws Exception {
    final UUID messageGuid = UUID.randomUUID();
    final MessageProtos.Envelope envelope = generateRandomMessage(messageGuid);

    final MessageStream messageStream = mock(MessageStream.class);
    when(messageStream.getMessages()).thenReturn(JdkFlowAdapter.publisherToFlowPublisher(Flux.just(
        new MessageStreamEntry.Envelope(envelope),
        new MessageStreamEntry.QueueEmpty())));
    when(messageStream.acknowledgeMessage(any(), anyLong())).thenReturn(CompletableFuture.completedFuture(null));
    when(messagesManager.getMessages(ACCOUNT_UUID, device)).thenReturn(messageStream);
    final URI uri = URI.create("ws://127.0.0.1:" + DROPWIZARD_APP_EXTENSION.getLocalPort() + "/websocket");

    final TestWebsocketListener disabledListener = new TestWebsocketListener();
    final ClientUpgradeRequest disabledRequest = new ClientUpgradeRequest(uri);
    disabledRequest.setHeader(WebsocketHeaders.X_SIGNAL_DISABLE_MESSAGES, "true");
    try (Session _ = client.connect(disabledListener, disabledRequest).get(5, TimeUnit.SECONDS)) {
      assertThrows(TimeoutException.class, () -> disabledListener.queueEmptyFuture().get(10,  TimeUnit.MILLISECONDS));
      assertTrue(disabledListener.getReceivedEnvelopes().isEmpty());
      assertFalse(disabledListener.queueEmptyFuture().isDone());
    }

    final TestWebsocketListener enabledListener = new TestWebsocketListener();
    try (Session ignored = client.connect(enabledListener, uri).get(5, TimeUnit.SECONDS)) {
      enabledListener.queueEmptyFuture().get(5, TimeUnit.SECONDS);
      assertEquals(1, enabledListener.getReceivedEnvelopes().size());
      assertEquals(
          UUIDUtil.toByteString(messageGuid),
          enabledListener.getReceivedEnvelopes().getFirst().getServerGuid());
    }
  }

  @Test
  void allDisconnected() throws Exception {
    final MessageStream messageStream = mock(MessageStream.class);
    when(messageStream.getMessages())
        .thenReturn(JdkFlowAdapter.publisherToFlowPublisher(Flux.never()));
    when(messagesManager.getMessages(ACCOUNT_UUID, device)).thenReturn(messageStream);

    final URI uri = URI.create("ws://127.0.0.1:" + DROPWIZARD_APP_EXTENSION.getLocalPort() + "/websocket");

    final TestWebsocketListener disabledListener = new TestWebsocketListener();
    final TestWebsocketListener enabledListener = new TestWebsocketListener();
    final ClientUpgradeRequest disabledRequest = new ClientUpgradeRequest(uri);
    disabledRequest.setHeader(WebsocketHeaders.X_SIGNAL_DISABLE_MESSAGES, "true");

    final ArgumentCaptor<DisconnectionRequestListener> captor =
        ArgumentCaptor.forClass(DisconnectionRequestListener.class);
    client.connect(disabledListener, disabledRequest).get(5, TimeUnit.SECONDS);
    client.connect(enabledListener, uri).get(5, TimeUnit.SECONDS);

    // Simulate a disconnection request
    verify(disconnectionRequestManager, timeout(1000).times(2))
        .addListener(eq(ACCOUNT_UUID), eq(DEVICE_ID), captor.capture());
    captor.getAllValues().forEach(listener -> listener.handleDisconnectionRequest());

    assertEquals(4401, disabledListener.closeFuture().join());
    assertEquals(4401, enabledListener.closeFuture().join());
  }

  private static MessageProtos.Envelope generateRandomMessage(final UUID messageGuid) {
    final long timestamp = System.currentTimeMillis();
    return MessageProtos.Envelope.newBuilder()
        .setClientTimestamp(timestamp)
        .setServerTimestamp(timestamp)
        .setContent(ByteString.copyFromUtf8(RandomStringUtils.secure().nextAlphanumeric(256)))
        .setType(MessageProtos.Envelope.Type.CIPHERTEXT)
        .setServerGuid(UUIDUtil.toByteString(messageGuid))
        .setSourceServiceId(new AciServiceIdentifier(UUID.randomUUID()).toCompactByteString())
        .setDestinationServiceId(new AciServiceIdentifier(ACCOUNT_UUID).toCompactByteString())
        .build();
  }
}
