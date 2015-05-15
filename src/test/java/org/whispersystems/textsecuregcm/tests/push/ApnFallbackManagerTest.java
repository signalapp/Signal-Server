package org.whispersystems.textsecuregcm.tests.push;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.whispersystems.textsecuregcm.entities.ApnMessage;
import org.whispersystems.textsecuregcm.push.ApnFallbackManager;
import org.whispersystems.textsecuregcm.push.ApnFallbackManager.ApnFallbackTask;
import org.whispersystems.textsecuregcm.push.PushServiceClient;
import org.whispersystems.textsecuregcm.util.Util;
import org.whispersystems.textsecuregcm.websocket.WebsocketAddress;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class ApnFallbackManagerTest {

  @Test
  public void testFullFallback() throws Exception {
    PushServiceClient  pushServiceClient  = mock(PushServiceClient.class);
    WebsocketAddress   address            = mock(WebsocketAddress.class );
    ApnMessage         message            = new ApnMessage("bar", "123", 1, "hmm", true);
    ApnFallbackTask    task               = new ApnFallbackTask("foo", message, 0, 500);

    ApnFallbackManager apnFallbackManager = new ApnFallbackManager(pushServiceClient);
    apnFallbackManager.start();

    apnFallbackManager.schedule(address, task);

    Util.sleep(1100);

    ArgumentCaptor<ApnMessage> captor = ArgumentCaptor.forClass(ApnMessage.class);
    verify(pushServiceClient, times(2)).send(captor.capture());

    List<ApnMessage> messages = captor.getAllValues();
    assertEquals(messages.get(0), message);
    assertEquals(messages.get(1).getApnId(), task.getApnId());
    assertFalse(messages.get(1).isVoip());
  }

  @Test
  public void testPartialFallback() throws Exception {
    PushServiceClient  pushServiceClient  = mock(PushServiceClient.class);
    WebsocketAddress   address            = mock(WebsocketAddress.class );
    ApnMessage         message            = new ApnMessage("bar", "123", 1, "hmm", true);
    ApnFallbackTask    task               = new ApnFallbackTask   ("foo", message, 0, 500);

    ApnFallbackManager apnFallbackManager = new ApnFallbackManager(pushServiceClient);
    apnFallbackManager.start();

    apnFallbackManager.schedule(address, task);

    Util.sleep(600);

    apnFallbackManager.cancel(address);

    Util.sleep(600);

    verify(pushServiceClient, times(1)).send(eq(message));
  }

  @Test
  public void testNoFallback() throws Exception {
    PushServiceClient  pushServiceClient  = mock(PushServiceClient.class);
    WebsocketAddress   address            = mock(WebsocketAddress.class );
    ApnMessage         message            = new ApnMessage("bar", "123", 1, "hmm", true);
    ApnFallbackTask    task               = new ApnFallbackTask   ("foo", message, 0, 500);

    ApnFallbackManager apnFallbackManager = new ApnFallbackManager(pushServiceClient);
    apnFallbackManager.start();

    apnFallbackManager.schedule(address, task);
    apnFallbackManager.cancel(address);

    Util.sleep(1100);

    verifyNoMoreInteractions(pushServiceClient);
  }

}
