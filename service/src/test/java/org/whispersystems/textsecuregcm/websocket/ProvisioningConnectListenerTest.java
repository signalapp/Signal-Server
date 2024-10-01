package org.whispersystems.textsecuregcm.websocket;

import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.push.ProvisioningManager;
import org.whispersystems.websocket.WebSocketClient;
import org.whispersystems.websocket.session.WebSocketSessionContext;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class ProvisioningConnectListenerTest {

  private ProvisioningManager provisioningManager;
  private ProvisioningConnectListener provisioningConnectListener;

  @BeforeEach
  void setUp() {
    provisioningManager = mock(ProvisioningManager.class);
    provisioningConnectListener = new ProvisioningConnectListener(provisioningManager);
  }

  @Test
  void onWebSocketConnect() {
    final WebSocketClient webSocketClient = mock(WebSocketClient.class);
    final WebSocketSessionContext context = new WebSocketSessionContext(webSocketClient);

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
}
