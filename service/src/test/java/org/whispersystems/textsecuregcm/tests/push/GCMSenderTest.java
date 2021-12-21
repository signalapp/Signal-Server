/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.push;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.whispersystems.gcm.server.Message;
import org.whispersystems.gcm.server.Result;
import org.whispersystems.gcm.server.Sender;
import org.whispersystems.textsecuregcm.push.GCMSender;
import org.whispersystems.textsecuregcm.push.GcmMessage;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.tests.util.SynchronousExecutorService;
import org.whispersystems.textsecuregcm.util.Util;

class GCMSenderTest {

  @Test
  void testSendMessage() {
    AccountsManager            accountsManager = mock(AccountsManager.class);
    Sender                     sender          = mock(Sender.class         );
    Result                     successResult   = mock(Result.class         );
    SynchronousExecutorService executorService = new SynchronousExecutorService();

    when(successResult.isInvalidRegistrationId()).thenReturn(false);
    when(successResult.isUnregistered()).thenReturn(false);
    when(successResult.hasCanonicalRegistrationId()).thenReturn(false);
    when(successResult.isSuccess()).thenReturn(true);

    AccountsHelper.setupMockUpdate(accountsManager);

    GcmMessage message = new GcmMessage("foo", UUID.randomUUID(), 1, GcmMessage.Type.NOTIFICATION, Optional.empty());
    GCMSender gcmSender = new GCMSender(executorService, accountsManager, sender);

    CompletableFuture<Result> successFuture = CompletableFuture.completedFuture(successResult);

    when(sender.send(any(Message.class))).thenReturn(successFuture);

    gcmSender.sendMessage(message);

    verify(sender, times(1)).send(any(Message.class));
  }

  @Test
  void testSendUninstalled() {
    UUID destinationUuid = UUID.randomUUID();
    String gcmId = "foo";

    AccountsManager            accountsManager = mock(AccountsManager.class);
    Sender                     sender          = mock(Sender.class         );
    Result                     invalidResult   = mock(Result.class         );
    SynchronousExecutorService executorService = new SynchronousExecutorService();

    Account destinationAccount = mock(Account.class);
    Device  destinationDevice  = mock(Device.class );

    AccountsHelper.setupMockUpdate(accountsManager);

    when(destinationAccount.getDevice(1)).thenReturn(Optional.of(destinationDevice));
    when(accountsManager.getByAccountIdentifier(destinationUuid)).thenReturn(Optional.of(destinationAccount));
    when(destinationDevice.getGcmId()).thenReturn(gcmId);

    when(invalidResult.isInvalidRegistrationId()).thenReturn(true);
    when(invalidResult.isUnregistered()).thenReturn(false);
    when(invalidResult.hasCanonicalRegistrationId()).thenReturn(false);
    when(invalidResult.isSuccess()).thenReturn(true);

    GcmMessage message = new GcmMessage(gcmId, destinationUuid, 1, GcmMessage.Type.NOTIFICATION, Optional.empty());
    GCMSender gcmSender = new GCMSender(executorService, accountsManager, sender);

    CompletableFuture<Result> invalidFuture = CompletableFuture.completedFuture(invalidResult);

    when(sender.send(any(Message.class))).thenReturn(invalidFuture);

    gcmSender.sendMessage(message);

    verify(sender, times(1)).send(any(Message.class));
    verify(accountsManager, times(1)).getByAccountIdentifier(eq(destinationUuid));
    verify(accountsManager, times(1)).updateDevice(eq(destinationAccount), eq(1L), any());
    verify(destinationDevice, times(1)).setUninstalledFeedbackTimestamp(eq(Util.todayInMillis()));
  }

  @Test
  void testCanonicalId() {
    UUID destinationUuid     = UUID.randomUUID();
    String gcmId             = "foo";
    String canonicalId       = "bar";

    AccountsManager            accountsManager = mock(AccountsManager.class);
    Sender                     sender          = mock(Sender.class         );
    Result                     canonicalResult = mock(Result.class         );
    SynchronousExecutorService executorService = new SynchronousExecutorService();

    Account        destinationAccount = mock(Account.class       );
    Device         destinationDevice  = mock(Device.class        );

    when(destinationAccount.getDevice(1)).thenReturn(Optional.of(destinationDevice));
    when(accountsManager.getByAccountIdentifier(destinationUuid)).thenReturn(Optional.of(destinationAccount));
    when(destinationDevice.getGcmId()).thenReturn(gcmId);

    AccountsHelper.setupMockUpdate(accountsManager);

    when(canonicalResult.isInvalidRegistrationId()).thenReturn(false);
    when(canonicalResult.isUnregistered()).thenReturn(false);
    when(canonicalResult.hasCanonicalRegistrationId()).thenReturn(true);
    when(canonicalResult.isSuccess()).thenReturn(false);
    when(canonicalResult.getCanonicalRegistrationId()).thenReturn(canonicalId);

    GcmMessage message = new GcmMessage(gcmId, destinationUuid, 1, GcmMessage.Type.NOTIFICATION, Optional.empty());
    GCMSender gcmSender = new GCMSender(executorService, accountsManager, sender);

    CompletableFuture<Result> invalidFuture = CompletableFuture.completedFuture(canonicalResult);

    when(sender.send(any(Message.class))).thenReturn(invalidFuture);

    gcmSender.sendMessage(message);

    verify(sender, times(1)).send(any(Message.class));
    verify(accountsManager, times(1)).getByAccountIdentifier(eq(destinationUuid));
    verify(accountsManager, times(1)).updateDevice(eq(destinationAccount), eq(1L), any());
    verify(destinationDevice, times(1)).setGcmId(eq(canonicalId));
  }

}
