/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.integration;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import com.google.common.net.HttpHeaders;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import jakarta.ws.rs.core.MediaType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.IncomingMessage;
import org.whispersystems.textsecuregcm.entities.IncomingMessageList;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.entities.SendMessageResponse;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.websocket.messages.WebSocketResponseMessage;

@Timeout(value = 1, unit = TimeUnit.MINUTES, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
public class MessagingTest {
  TestUser userA;
  TestUser userB;

  @BeforeEach
  public void setup() {
    userA = Operations.newRegisteredUser("+19995550102");
    userB = Operations.newRegisteredUser("+19995550103");
  }

  @AfterEach
  public void teardown() {
    Operations.deleteUser(userA);
    Operations.deleteUser(userB);
  }

  @Test
  public void testSendMessageUnsealed() throws IOException {
      final byte[] expectedContent = "Hello, World!".getBytes(StandardCharsets.UTF_8);
      final IncomingMessage message = new IncomingMessage(1, Device.PRIMARY_ID, userB.registrationId(), expectedContent);
      final IncomingMessageList messages = new IncomingMessageList(List.of(message), false, true, System.currentTimeMillis());

      final WebsocketClientSession websocketA = Operations.authenticatedWebsocket(userA, Device.PRIMARY_ID);
      final WebSocketResponseMessage responseMessage = websocketA.sendRequest(
          "PUT",
          "/v1/messages/%s".formatted(userB.aciUuid().toString()),
          List.of(HttpHeaders.CONTENT_TYPE + ":" + MediaType.APPLICATION_JSON),
          messages);
      assertEquals(200, responseMessage.getStatus());
      assertDoesNotThrow(() -> WebsocketClientSession.decode(SendMessageResponse.class, responseMessage));

      final WebsocketClientSession websocketB = Operations.authenticatedWebsocket(userB, Device.PRIMARY_ID);
      assertTimeoutPreemptively(Duration.ofSeconds(5), websocketB::waitForQueueEmpty);

      assertEquals(1, websocketB.getReceivedEnvelopes().size());
      final MessageProtos.Envelope envelope = websocketB.getReceivedEnvelopes().getFirst();
      assertArrayEquals(expectedContent, envelope.getContent().toByteArray());

      websocketB.close(1000);
      websocketA.close(1000);
  }
}
