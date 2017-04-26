package org.whispersystems.textsecuregcm.tests.push;

import com.google.common.base.Optional;
import com.notnoop.apns.ApnsService;
import org.junit.Before;
import org.junit.Test;
import org.whispersystems.textsecuregcm.push.APNSender;
import org.whispersystems.textsecuregcm.push.ApnMessage;
import org.whispersystems.textsecuregcm.push.TransientPushFailureException;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;

import java.util.Date;
import java.util.HashMap;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class APNSenderTest {

  private static final String DESTINATION_NUMBER = "+14151231234";
  private static final String DESTINATION_APN_ID = "foo";

  private final AccountsManager accountsManager = mock(AccountsManager.class);
  private final JedisPool       jedisPool       = mock(JedisPool.class);
  private final Jedis           jedis           = mock(Jedis.class);
  private final ApnsService     voipService     = mock(ApnsService.class);
  private final ApnsService     apnsService     = mock(ApnsService.class);

  private final Account destinationAccount = mock(Account.class);
  private final Device  destinationDevice  = mock(Device.class );

  @Before
  public void setup() {
    when(destinationAccount.getDevice(1)).thenReturn(Optional.of(destinationDevice));
    when(destinationDevice.getApnId()).thenReturn(DESTINATION_APN_ID);
    when(accountsManager.get(DESTINATION_NUMBER)).thenReturn(Optional.of(destinationAccount));

    when(jedisPool.getResource()).thenReturn(jedis);
    when(jedis.get("APN-" + DESTINATION_APN_ID)).thenReturn(DESTINATION_NUMBER + "." + 1);

    when(voipService.getInactiveDevices()).thenReturn(new HashMap<String, Date>() {{
      put(DESTINATION_APN_ID, new Date(System.currentTimeMillis()));
    }});
    when(apnsService.getInactiveDevices()).thenReturn(new HashMap<String, Date>());
  }

  @Test
  public void testSendVoip() throws TransientPushFailureException {
    APNSender apnSender = new APNSender(accountsManager, jedisPool, apnsService, voipService, false, false);

    ApnMessage message = new ApnMessage(DESTINATION_APN_ID, DESTINATION_NUMBER, 1, "message", true, 30);
    apnSender.sendMessage(message);


    verify(jedis, times(1)).set(eq("APN-" + DESTINATION_APN_ID.toLowerCase()), eq(DESTINATION_NUMBER + "." + 1));
    verify(voipService, times(1)).push(eq(DESTINATION_APN_ID), eq(message.getMessage()), eq(new Date(30)));
    verifyNoMoreInteractions(apnsService);
  }

  @Test
  public void testSendApns() throws TransientPushFailureException {
    APNSender apnSender = new APNSender(accountsManager, jedisPool, apnsService, voipService, false, false);

    ApnMessage message = new ApnMessage(DESTINATION_APN_ID, DESTINATION_NUMBER, 1, "message", false, 30);
    apnSender.sendMessage(message);

    verify(jedis, times(1)).set(eq("APN-" + DESTINATION_APN_ID.toLowerCase()), eq(DESTINATION_NUMBER + "." + 1));
    verify(apnsService, times(1)).push(eq(DESTINATION_APN_ID), eq(message.getMessage()), eq(new Date(30)));
    verifyNoMoreInteractions(voipService);
  }

  @Test
  public void testFeedbackUnregistered() {
    APNSender apnSender = new APNSender(accountsManager, jedisPool, apnsService, voipService, false, false);
    apnSender.checkFeedback();

    verify(jedis, times(1)).get(eq("APN-" +DESTINATION_APN_ID));
    verify(accountsManager, times(1)).get(eq(DESTINATION_NUMBER));
    verify(destinationDevice, times(1)).setApnId(eq((String)null));
    verify(accountsManager, times(1)).update(eq(destinationAccount));
  }

}
