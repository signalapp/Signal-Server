package org.whispersystems.textsecuregcm.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.InvalidProtocolBufferException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.push.ProvisioningManager;
import org.whispersystems.websocket.WebSocketClient;
import org.whispersystems.websocket.session.WebSocketSessionContext;

class ProvisioningConnectListenerTest {

  private ProvisioningManager provisioningManager;
  private ProvisioningConnectListener provisioningConnectListener;
  private ScheduledExecutorService scheduledExecutorService;

  private static Duration TIMEOUT = Duration.ofSeconds(5);

  @BeforeEach
  void setUp() {
    provisioningManager = mock(ProvisioningManager.class);
    scheduledExecutorService = mock(ScheduledExecutorService.class);
    provisioningConnectListener =
        new ProvisioningConnectListener(provisioningManager, scheduledExecutorService, TIMEOUT);
  }

  @Test
  void onWebSocketConnect() {
    final WebSocketClient webSocketClient = mock(WebSocketClient.class);
    final WebSocketSessionContext context = new WebSocketSessionContext(webSocketClient);
    final ScheduledFuture<?> scheduledFuture = mock(ScheduledFuture.class);
    doReturn(scheduledFuture).when(scheduledExecutorService).schedule(any(Runnable.class), anyLong(), any());

    provisioningConnectListener.onWebSocketConnect(context);
    context.notifyClosed(1000, "Test");

    final ArgumentCaptor<String> addListenerProvisioningAddressCaptor = ArgumentCaptor.forClass(String.class);
    final ArgumentCaptor<String> removeListenerProvisioningAddressCaptor = ArgumentCaptor.forClass(String.class);

    @SuppressWarnings("unchecked") final ArgumentCaptor<Optional<byte[]>> sendAddressCaptor =
        ArgumentCaptor.forClass(Optional.class);

    verify(provisioningManager).addListener(addListenerProvisioningAddressCaptor.capture(), any());
    verify(provisioningManager).removeListener(removeListenerProvisioningAddressCaptor.capture());
    verify(webSocketClient).sendRequest(eq("PUT"), eq("/v1/address"), any(), sendAddressCaptor.capture());

    final String sentProvisioningAddress = sendAddressCaptor.getValue()
        .map(provisioningAddressBytes -> {
          try {
            return MessageProtos.ProvisioningAddress.parseFrom(provisioningAddressBytes);
          } catch (final InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
          }
        })
        .map(MessageProtos.ProvisioningAddress::getAddress)
        .orElseThrow();

    assertEquals(addListenerProvisioningAddressCaptor.getValue(), removeListenerProvisioningAddressCaptor.getValue());
    assertEquals(addListenerProvisioningAddressCaptor.getValue(), sentProvisioningAddress);
  }

  @Test
  void schedulesTimeout() {
    final WebSocketClient webSocketClient = mock(WebSocketClient.class);
    final WebSocketSessionContext context = new WebSocketSessionContext(webSocketClient);

    final ScheduledFuture<?> scheduledFuture = mock(ScheduledFuture.class);
    doReturn(scheduledFuture).when(scheduledExecutorService).schedule(any(Runnable.class), anyLong(), any());

    final ArgumentCaptor<Runnable> scheduleCaptor = ArgumentCaptor.forClass(Runnable.class);
    provisioningConnectListener.onWebSocketConnect(context);
    verify(scheduledExecutorService).schedule(scheduleCaptor.capture(), eq(TIMEOUT.getSeconds()), eq(TimeUnit.SECONDS));

    verify(webSocketClient, never()).close(anyInt(), any());
    scheduleCaptor.getValue().run();
    verify(webSocketClient, times(1)).close(eq(1000), anyString());
  }

  @Test
  void cancelsTimeout() {
    final WebSocketClient webSocketClient = mock(WebSocketClient.class);
    final WebSocketSessionContext context = new WebSocketSessionContext(webSocketClient);

    final ScheduledFuture<?> scheduledFuture = mock(ScheduledFuture.class);
    doReturn(scheduledFuture).when(scheduledExecutorService).schedule(any(Runnable.class), anyLong(), any());

    provisioningConnectListener.onWebSocketConnect(context);
    verify(scheduledExecutorService).schedule(any(Runnable.class), eq(TIMEOUT.getSeconds()), eq(TimeUnit.SECONDS));

    context.notifyClosed(1000, "Test");

    verify(scheduledFuture).cancel(false);
    verify(webSocketClient, never()).close(anyInt(), any());
  }
}
