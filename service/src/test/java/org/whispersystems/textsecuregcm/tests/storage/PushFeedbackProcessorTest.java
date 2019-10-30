package org.whispersystems.textsecuregcm.tests.storage;

import org.junit.Before;
import org.junit.Test;
import org.whispersystems.textsecuregcm.sqs.DirectoryQueue;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountDatabaseCrawlerRestartException;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.PushFeedbackProcessor;
import org.whispersystems.textsecuregcm.util.Util;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

public class PushFeedbackProcessorTest {

  private AccountsManager accountsManager = mock(AccountsManager.class);
  private DirectoryQueue  directoryQueue  = mock(DirectoryQueue.class);

  private Account uninstalledAccount = mock(Account.class);
  private Account mixedAccount       = mock(Account.class);
  private Account freshAccount       = mock(Account.class);
  private Account cleanAccount       = mock(Account.class);
  private Account stillActiveAccount = mock(Account.class);

  private Device uninstalledDevice       = mock(Device.class);
  private Device uninstalledDeviceTwo    = mock(Device.class);
  private Device installedDevice         = mock(Device.class);
  private Device installedDeviceTwo      = mock(Device.class);
  private Device recentUninstalledDevice = mock(Device.class);
  private Device stillActiveDevice       = mock(Device.class);

  @Before
  public void setup() {
    when(uninstalledDevice.getUninstalledFeedbackTimestamp()).thenReturn(Util.todayInMillis() - TimeUnit.DAYS.toMillis(2));
    when(uninstalledDevice.getLastSeen()).thenReturn(Util.todayInMillis() - TimeUnit.DAYS.toMillis(2));
    when(uninstalledDeviceTwo.getUninstalledFeedbackTimestamp()).thenReturn(Util.todayInMillis() - TimeUnit.DAYS.toMillis(3));
    when(uninstalledDeviceTwo.getLastSeen()).thenReturn(Util.todayInMillis() - TimeUnit.DAYS.toMillis(3));

    when(installedDevice.getUninstalledFeedbackTimestamp()).thenReturn(0L);
    when(installedDeviceTwo.getUninstalledFeedbackTimestamp()).thenReturn(0L);

    when(recentUninstalledDevice.getUninstalledFeedbackTimestamp()).thenReturn(Util.todayInMillis() - TimeUnit.DAYS.toMillis(1));
    when(recentUninstalledDevice.getLastSeen()).thenReturn(Util.todayInMillis());

    when(stillActiveDevice.getUninstalledFeedbackTimestamp()).thenReturn(Util.todayInMillis() - TimeUnit.DAYS.toMillis(2));
    when(stillActiveDevice.getLastSeen()).thenReturn(Util.todayInMillis());

    when(uninstalledAccount.getDevices()).thenReturn(Set.of(uninstalledDevice));
    when(mixedAccount.getDevices()).thenReturn(Set.of(installedDevice, uninstalledDeviceTwo));
    when(freshAccount.getDevices()).thenReturn(Set.of(recentUninstalledDevice));
    when(cleanAccount.getDevices()).thenReturn(Set.of(installedDeviceTwo));
    when(stillActiveAccount.getDevices()).thenReturn(Set.of(stillActiveDevice));
  }


  @Test
  public void testEmpty() throws AccountDatabaseCrawlerRestartException {
    PushFeedbackProcessor processor = new PushFeedbackProcessor(accountsManager, directoryQueue);
    processor.timeAndProcessCrawlChunk(Optional.of(UUID.randomUUID()), Collections.emptyList());

    verifyZeroInteractions(accountsManager);
    verifyZeroInteractions(directoryQueue);
  }

  @Test
  public void testUpdate() throws AccountDatabaseCrawlerRestartException {
    PushFeedbackProcessor processor = new PushFeedbackProcessor(accountsManager, directoryQueue);
    processor.timeAndProcessCrawlChunk(Optional.of(UUID.randomUUID()), List.of(uninstalledAccount, mixedAccount, stillActiveAccount, freshAccount, cleanAccount));

    verify(uninstalledDevice).setApnId(isNull());
    verify(uninstalledDevice).setGcmId(isNull());
    verify(uninstalledDevice).setFetchesMessages(eq(false));

    verify(accountsManager).update(eq(uninstalledAccount));

    verify(uninstalledDeviceTwo).setApnId(isNull());
    verify(uninstalledDeviceTwo).setGcmId(isNull());
    verify(uninstalledDeviceTwo).setFetchesMessages(eq(false));

    verify(installedDevice, never()).setApnId(any());
    verify(installedDevice, never()).setGcmId(any());
    verify(installedDevice, never()).setFetchesMessages(anyBoolean());

    verify(accountsManager).update(eq(mixedAccount));

    verify(recentUninstalledDevice, never()).setApnId(any());
    verify(recentUninstalledDevice, never()).setGcmId(any());
    verify(recentUninstalledDevice, never()).setFetchesMessages(anyBoolean());

    verify(accountsManager, never()).update(eq(freshAccount));

    verify(installedDeviceTwo, never()).setApnId(any());
    verify(installedDeviceTwo, never()).setGcmId(any());
    verify(installedDeviceTwo, never()).setFetchesMessages(anyBoolean());

    verify(accountsManager, never()).update(eq(cleanAccount));

    verify(stillActiveDevice).setUninstalledFeedbackTimestamp(eq(0L));
    verify(stillActiveDevice, never()).setApnId(any());
    verify(stillActiveDevice, never()).setGcmId(any());
    verify(stillActiveDevice, never()).setFetchesMessages(anyBoolean());

    verify(accountsManager).update(eq(stillActiveAccount));
  }



}
