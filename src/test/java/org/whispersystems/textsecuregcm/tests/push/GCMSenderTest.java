package org.whispersystems.textsecuregcm.tests.push;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.SettableFuture;
import org.junit.Test;
import org.mockito.Matchers;
import org.whispersystems.gcm.server.Message;
import org.whispersystems.gcm.server.Result;
import org.whispersystems.gcm.server.Sender;
import org.whispersystems.textsecuregcm.push.GCMSender;
import org.whispersystems.textsecuregcm.push.GcmMessage;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.tests.util.SynchronousExecutorService;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class GCMSenderTest {

  @Test
  public void testSendMessage() {
    AccountsManager            accountsManager = mock(AccountsManager.class);
    Sender                     sender          = mock(Sender.class         );
    Result                     successResult   = mock(Result.class         );
    SynchronousExecutorService executorService = new SynchronousExecutorService();

    when(successResult.isInvalidRegistrationId()).thenReturn(false);
    when(successResult.isUnregistered()).thenReturn(false);
    when(successResult.hasCanonicalRegistrationId()).thenReturn(false);
    when(successResult.isSuccess()).thenReturn(true);

    GcmMessage message = new GcmMessage("foo", "+12223334444", 1, false);
    GCMSender gcmSender = new GCMSender(accountsManager, sender, executorService);

    SettableFuture<Result> successFuture = SettableFuture.create();
    successFuture.set(successResult);

    when(sender.send(any(Message.class), Matchers.anyObject())).thenReturn(successFuture);
    when(successResult.getContext()).thenReturn(message);

    gcmSender.sendMessage(message);

    verify(sender, times(1)).send(any(Message.class), eq(message));
  }

  @Test
  public void testSendError() {
    String destinationNumber = "+12223334444";
    String gcmId             = "foo";

    AccountsManager            accountsManager = mock(AccountsManager.class);
    Sender                     sender          = mock(Sender.class         );
    Result                     invalidResult   = mock(Result.class         );
    SynchronousExecutorService executorService = new SynchronousExecutorService();

    Account destinationAccount = mock(Account.class);
    Device  destinationDevice  = mock(Device.class );

    when(destinationAccount.getDevice(1)).thenReturn(Optional.of(destinationDevice));
    when(accountsManager.get(destinationNumber)).thenReturn(Optional.of(destinationAccount));
    when(destinationDevice.getGcmId()).thenReturn(gcmId);

    when(invalidResult.isInvalidRegistrationId()).thenReturn(true);
    when(invalidResult.isUnregistered()).thenReturn(false);
    when(invalidResult.hasCanonicalRegistrationId()).thenReturn(false);
    when(invalidResult.isSuccess()).thenReturn(true);

    GcmMessage message = new GcmMessage(gcmId, destinationNumber, 1, false);
    GCMSender gcmSender = new GCMSender(accountsManager, sender, executorService);

    SettableFuture<Result> invalidFuture = SettableFuture.create();
    invalidFuture.set(invalidResult);

    when(sender.send(any(Message.class), Matchers.anyObject())).thenReturn(invalidFuture);
    when(invalidResult.getContext()).thenReturn(message);

    gcmSender.sendMessage(message);

    verify(sender, times(1)).send(any(Message.class), eq(message));
    verify(accountsManager, times(1)).get(eq(destinationNumber));
    verify(accountsManager, times(1)).update(eq(destinationAccount));
    verify(destinationDevice, times(1)).setGcmId(eq((String)null));
  }

  @Test
  public void testCanonicalId() {
    String destinationNumber = "+12223334444";
    String gcmId             = "foo";
    String canonicalId       = "bar";

    AccountsManager            accountsManager = mock(AccountsManager.class);
    Sender                     sender          = mock(Sender.class         );
    Result                     canonicalResult = mock(Result.class         );
    SynchronousExecutorService executorService = new SynchronousExecutorService();

    Account destinationAccount = mock(Account.class);
    Device  destinationDevice  = mock(Device.class );

    when(destinationAccount.getDevice(1)).thenReturn(Optional.of(destinationDevice));
    when(accountsManager.get(destinationNumber)).thenReturn(Optional.of(destinationAccount));
    when(destinationDevice.getGcmId()).thenReturn(gcmId);

    when(canonicalResult.isInvalidRegistrationId()).thenReturn(false);
    when(canonicalResult.isUnregistered()).thenReturn(false);
    when(canonicalResult.hasCanonicalRegistrationId()).thenReturn(true);
    when(canonicalResult.isSuccess()).thenReturn(false);
    when(canonicalResult.getCanonicalRegistrationId()).thenReturn(canonicalId);

    GcmMessage message = new GcmMessage(gcmId, destinationNumber, 1, false);
    GCMSender gcmSender = new GCMSender(accountsManager, sender, executorService);

    SettableFuture<Result> invalidFuture = SettableFuture.create();
    invalidFuture.set(canonicalResult);

    when(sender.send(any(Message.class), Matchers.anyObject())).thenReturn(invalidFuture);
    when(canonicalResult.getContext()).thenReturn(message);

    gcmSender.sendMessage(message);

    verify(sender, times(1)).send(any(Message.class), eq(message));
    verify(accountsManager, times(1)).get(eq(destinationNumber));
    verify(accountsManager, times(1)).update(eq(destinationAccount));
    verify(destinationDevice, times(1)).setGcmId(eq(canonicalId));
  }

}
