package org.whispersystems.textsecuregcm.tests.push;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.whispersystems.textsecuregcm.entities.ApnMessage;
import org.whispersystems.textsecuregcm.push.ApnFallbackManager;
import org.whispersystems.textsecuregcm.push.ApnFallbackManager.ApnFallbackTask;
import org.whispersystems.textsecuregcm.push.PushServiceClient;
import org.whispersystems.textsecuregcm.storage.PubSubManager;
import org.whispersystems.textsecuregcm.storage.PubSubProtos;
import org.whispersystems.textsecuregcm.util.Util;
import org.whispersystems.textsecuregcm.websocket.WebSocketConnectionInfo;
import org.whispersystems.textsecuregcm.websocket.WebsocketAddress;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class ApnFallbackManagerTest {

  @Test
  public void testFullFallback() throws Exception {
    PushServiceClient  pushServiceClient  = mock(PushServiceClient.class);
    PubSubManager      pubSubManager      = mock(PubSubManager.class);
    WebsocketAddress   address            = new WebsocketAddress("+14152222223", 1L);
    WebSocketConnectionInfo info         = new WebSocketConnectionInfo(address);
    ApnMessage         message            = new ApnMessage("bar", "123", 1, "hmm", true, 1111);
    ApnFallbackTask    task               = new ApnFallbackTask("foo", "voipfoo", message, 500, 0);

    ApnFallbackManager apnFallbackManager = new ApnFallbackManager(pushServiceClient, pubSubManager);
    apnFallbackManager.start();

    apnFallbackManager.schedule(address, task);

    Util.sleep(1100);

    ArgumentCaptor<ApnMessage> captor = ArgumentCaptor.forClass(ApnMessage.class);
    verify(pushServiceClient, times(2)).send(captor.capture());
    verify(pubSubManager).unsubscribe(eq(info), eq(apnFallbackManager));

    List<ApnMessage> arguments = captor.getAllValues();

    assertEquals(arguments.get(0).getMessage(), message.getMessage());
    assertEquals(arguments.get(0).getApnId(), task.getVoipApnId());
    assertTrue(arguments.get(0).isVoip());
//    assertEquals(arguments.get(0).getExpirationTime(), Integer.MAX_VALUE * 1000L);

    assertEquals(arguments.get(1).getMessage(), message.getMessage());
    assertEquals(arguments.get(1).getApnId(), task.getApnId());
    assertFalse(arguments.get(1).isVoip());
    assertEquals(arguments.get(1).getExpirationTime(), Integer.MAX_VALUE * 1000L);
  }

  @Test
  public void testNoFallback() throws Exception {
    PushServiceClient  pushServiceClient  = mock(PushServiceClient.class);
    PubSubManager      pubSubManager      = mock(PubSubManager.class);
    WebsocketAddress   address            = new WebsocketAddress("+14152222222", 1);
    WebSocketConnectionInfo info          = new WebSocketConnectionInfo(address);
    ApnMessage         message            = new ApnMessage("bar", "123", 1, "hmm", true, 5555);
    ApnFallbackTask    task               = new ApnFallbackTask   ("foo", "voipfoo", message, 500, 0);

    ApnFallbackManager apnFallbackManager = new ApnFallbackManager(pushServiceClient, pubSubManager);
    apnFallbackManager.start();

    apnFallbackManager.schedule(address, task);
    apnFallbackManager.onDispatchMessage(info.serialize(),
                                         PubSubProtos.PubSubMessage.newBuilder()
                                                                   .setType(PubSubProtos.PubSubMessage.Type.CONNECTED)
                                                                   .build().toByteArray());

    verify(pubSubManager).unsubscribe(eq(info), eq(apnFallbackManager));

    Util.sleep(1100);

    verifyNoMoreInteractions(pushServiceClient);
  }

}
