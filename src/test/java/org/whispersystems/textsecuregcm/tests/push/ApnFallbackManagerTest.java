package org.whispersystems.textsecuregcm.tests.push;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.whispersystems.textsecuregcm.entities.ApnMessage;
import org.whispersystems.textsecuregcm.push.ApnFallbackManager;
import org.whispersystems.textsecuregcm.push.ApnFallbackManager.ApnFallbackTask;
import org.whispersystems.textsecuregcm.push.PushServiceClient;
import org.whispersystems.textsecuregcm.util.Util;
import org.whispersystems.textsecuregcm.websocket.WebsocketAddress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.*;

public class ApnFallbackManagerTest {

  @Test
  public void testFullFallback() throws Exception {
    PushServiceClient  pushServiceClient  = mock(PushServiceClient.class);
    WebsocketAddress   address            = mock(WebsocketAddress.class );
    ApnMessage         message            = new ApnMessage("bar", "123", 1, "hmm", true, 1111);
    ApnFallbackTask    task               = new ApnFallbackTask("foo", message, 500);

    ApnFallbackManager apnFallbackManager = new ApnFallbackManager(pushServiceClient);
    apnFallbackManager.start();

    apnFallbackManager.schedule(address, task);

    Util.sleep(1100);

    ArgumentCaptor<ApnMessage> captor = ArgumentCaptor.forClass(ApnMessage.class);
    verify(pushServiceClient, times(1)).send(captor.capture());

    assertEquals(captor.getValue().getMessage(), message.getMessage());
    assertEquals(captor.getValue().getApnId(), task.getApnId());
    assertFalse(captor.getValue().isVoip());
    assertEquals(captor.getValue().getExpirationTime(), Integer.MAX_VALUE * 1000L);
  }

  @Test
  public void testNoFallback() throws Exception {
    PushServiceClient  pushServiceClient  = mock(PushServiceClient.class);
    WebsocketAddress   address            = mock(WebsocketAddress.class );
    ApnMessage         message            = new ApnMessage("bar", "123", 1, "hmm", true, 5555);
    ApnFallbackTask    task               = new ApnFallbackTask   ("foo", message, 500);

    ApnFallbackManager apnFallbackManager = new ApnFallbackManager(pushServiceClient);
    apnFallbackManager.start();

    apnFallbackManager.schedule(address, task);
    apnFallbackManager.cancel(address);

    Util.sleep(1100);

    verifyNoMoreInteractions(pushServiceClient);
  }

}
