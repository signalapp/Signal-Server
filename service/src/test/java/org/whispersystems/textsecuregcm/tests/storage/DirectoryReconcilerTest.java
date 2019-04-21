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

import org.whispersystems.textsecuregcm.entities.ClientContact;
import org.whispersystems.textsecuregcm.entities.DirectoryReconciliationRequest;
import org.whispersystems.textsecuregcm.entities.DirectoryReconciliationResponse;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountDatabaseCrawlerRestartException;
import org.whispersystems.textsecuregcm.storage.DirectoryManager.BatchOperationHandle;
import org.whispersystems.textsecuregcm.storage.DirectoryManager;
import org.whispersystems.textsecuregcm.storage.DirectoryReconciler;
import org.whispersystems.textsecuregcm.storage.DirectoryReconciliationClient;
import org.whispersystems.textsecuregcm.util.Util;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class DirectoryReconcilerTest {
  private static final String VALID_NUMBER    = "valid";
  private static final String INACTIVE_NUMBER = "inactive";

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
    when(activeAccount.getNumber()).thenReturn(VALID_NUMBER);
    when(activeAccount.isActive()).thenReturn(true);
    when(inactiveAccount.getNumber()).thenReturn(INACTIVE_NUMBER);
    when(inactiveAccount.isActive()).thenReturn(false);
    when(directoryManager.startBatchOperation()).thenReturn(batchOperationHandle);
  }

  @Test
  public void testCrawlChunkValid() throws AccountDatabaseCrawlerRestartException {
    when(reconciliationClient.sendChunk(any())).thenReturn(successResponse);
    directoryReconciler.onCrawlChunk(Optional.of(VALID_NUMBER), Arrays.asList(activeAccount, inactiveAccount));

    verify(activeAccount, times(2)).getNumber();
    verify(activeAccount, times(2)).isActive();
    verify(inactiveAccount, times(2)).getNumber();
    verify(inactiveAccount, times(2)).isActive();

    ArgumentCaptor<DirectoryReconciliationRequest> request = ArgumentCaptor.forClass(DirectoryReconciliationRequest.class);
    verify(reconciliationClient, times(1)).sendChunk(request.capture());

    assertThat(request.getValue().getFromNumber()).isEqualTo(VALID_NUMBER);
    assertThat(request.getValue().getToNumber()).isEqualTo(INACTIVE_NUMBER);
    assertThat(request.getValue().getNumbers()).isEqualTo(Arrays.asList(VALID_NUMBER));

    ArgumentCaptor<ClientContact> addedContact = ArgumentCaptor.forClass(ClientContact.class);
    verify(directoryManager, times(1)).startBatchOperation();
    verify(directoryManager, times(1)).add(eq(batchOperationHandle), addedContact.capture());
    verify(directoryManager, times(1)).remove(eq(batchOperationHandle), eq(INACTIVE_NUMBER));
    verify(directoryManager, times(1)).stopBatchOperation(eq(batchOperationHandle));

    assertThat(addedContact.getValue().getToken()).isEqualTo(Util.getContactToken(VALID_NUMBER));

    verifyNoMoreInteractions(activeAccount);
    verifyNoMoreInteractions(inactiveAccount);
    verifyNoMoreInteractions(batchOperationHandle);
    verifyNoMoreInteractions(directoryManager);
    verifyNoMoreInteractions(reconciliationClient);
  }

}
