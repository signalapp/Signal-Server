/**
 * Copyright (C) 2018 Open WhisperSystems
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.whispersystems.textsecuregcm.tests.storage;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.whispersystems.textsecuregcm.entities.ClientContact;
import org.whispersystems.textsecuregcm.entities.DirectoryReconciliationRequest;
import org.whispersystems.textsecuregcm.entities.DirectoryReconciliationResponse;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountDatabaseCrawlerRestartException;
import org.whispersystems.textsecuregcm.storage.DirectoryManager;
import org.whispersystems.textsecuregcm.storage.DirectoryManager.BatchOperationHandle;
import org.whispersystems.textsecuregcm.storage.DirectoryReconciler;
import org.whispersystems.textsecuregcm.storage.DirectoryReconciliationClient;
import org.whispersystems.textsecuregcm.util.Util;

import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class DirectoryReconcilerTest {
  private static final UUID   VALID_UUID       = UUID.randomUUID();
  private static final String VALID_NUMBERRR   = "+14152222222";
  private static final UUID   INACTIVE_UUID    = UUID.randomUUID();
  private static final String INACTIVE_NUMBERRR = "+14151111111";

  private final Account                       activeAccount        = mock(Account.class);
  private final Account                       inactiveAccount      = mock(Account.class);
  private final BatchOperationHandle          batchOperationHandle = mock(BatchOperationHandle.class);
  private final DirectoryManager              directoryManager     = mock(DirectoryManager.class);
  private final DirectoryReconciliationClient reconciliationClient = mock(DirectoryReconciliationClient.class);
  private final DirectoryReconciler           directoryReconciler  = new DirectoryReconciler(reconciliationClient, directoryManager);

  private final DirectoryReconciliationResponse successResponse = new DirectoryReconciliationResponse(DirectoryReconciliationResponse.Status.OK);
  private final DirectoryReconciliationResponse missingResponse = new DirectoryReconciliationResponse(DirectoryReconciliationResponse.Status.MISSING);

  @Before
  public void setup() {
    when(activeAccount.getUuid()).thenReturn(VALID_UUID);
    when(activeAccount.isEnabled()).thenReturn(true);
    when(activeAccount.getNumber()).thenReturn(VALID_NUMBERRR);
    when(inactiveAccount.getUuid()).thenReturn(INACTIVE_UUID);
    when(inactiveAccount.getNumber()).thenReturn(INACTIVE_NUMBERRR);
    when(inactiveAccount.isEnabled()).thenReturn(false);
    when(directoryManager.startBatchOperation()).thenReturn(batchOperationHandle);
  }

  @Test
  public void testCrawlChunkValid() throws AccountDatabaseCrawlerRestartException {
    when(reconciliationClient.sendChunk(any())).thenReturn(successResponse);
    directoryReconciler.timeAndProcessCrawlChunk(Optional.of(VALID_UUID), Arrays.asList(activeAccount, inactiveAccount));

    verify(activeAccount, atLeastOnce()).getUuid();
    verify(activeAccount, atLeastOnce()).getNumber();
    verify(activeAccount, atLeastOnce()).isEnabled();
    verify(inactiveAccount, atLeastOnce()).getUuid();
    verify(inactiveAccount, atLeastOnce()).getNumber();
    verify(inactiveAccount, atLeastOnce()).isEnabled();

    ArgumentCaptor<DirectoryReconciliationRequest> request = ArgumentCaptor.forClass(DirectoryReconciliationRequest.class);
    verify(reconciliationClient, times(1)).sendChunk(request.capture());

    assertThat(request.getValue().getFromUuid()).isEqualTo(VALID_UUID);
    assertThat(request.getValue().getToUuid()).isEqualTo(INACTIVE_UUID);
    assertThat(request.getValue().getUsers()).isEqualTo(Arrays.asList(new DirectoryReconciliationRequest.User(VALID_UUID, VALID_NUMBERRR)));

    ArgumentCaptor<ClientContact> addedContact = ArgumentCaptor.forClass(ClientContact.class);
    verify(directoryManager, times(1)).startBatchOperation();
    verify(directoryManager, times(1)).add(eq(batchOperationHandle), addedContact.capture());
    verify(directoryManager, times(1)).remove(eq(batchOperationHandle), eq(INACTIVE_NUMBERRR));
    verify(directoryManager, times(1)).stopBatchOperation(eq(batchOperationHandle));

    assertThat(addedContact.getValue().getToken()).isEqualTo(Util.getContactToken(VALID_NUMBERRR));

    verifyNoMoreInteractions(activeAccount);
    verifyNoMoreInteractions(inactiveAccount);
    verifyNoMoreInteractions(batchOperationHandle);
    verifyNoMoreInteractions(directoryManager);
    verifyNoMoreInteractions(reconciliationClient);
  }

}
