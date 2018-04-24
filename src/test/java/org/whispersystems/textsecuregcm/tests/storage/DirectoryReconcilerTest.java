package org.whispersystems.textsecuregcm.tests.storage;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.whispersystems.textsecuregcm.entities.ClientContact;
import org.whispersystems.textsecuregcm.entities.DirectoryReconciliationRequest;
import org.whispersystems.textsecuregcm.entities.DirectoryReconciliationResponse;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Accounts;
import org.whispersystems.textsecuregcm.storage.DirectoryManager;
import org.whispersystems.textsecuregcm.storage.DirectoryManager.BatchOperationHandle;
import org.whispersystems.textsecuregcm.storage.DirectoryReconciler;
import org.whispersystems.textsecuregcm.storage.DirectoryReconciliationCache;
import org.whispersystems.textsecuregcm.storage.DirectoryReconciliationClient;
import org.whispersystems.textsecuregcm.util.Util;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class DirectoryReconcilerTest {

  private static final String VALID_NUMBER    = "valid";
  private static final String INACTIVE_NUMBER = "inactive";

  private static final long INTERVAL_MS   = 30_000L;

  private final Account                       account              = mock(Account.class);
  private final Account                       inactiveAccount      = mock(Account.class);
  private final Accounts                      accounts             = mock(Accounts.class);
  private final BatchOperationHandle          batchOperationHandle = mock(BatchOperationHandle.class);
  private final DirectoryManager              directoryManager     = mock(DirectoryManager.class);
  private final DirectoryReconciliationClient reconciliationClient = mock(DirectoryReconciliationClient.class);
  private final DirectoryReconciliationCache  reconciliationCache  = mock(DirectoryReconciliationCache.class);
  private final DirectoryReconciler           directoryReconciler  = new DirectoryReconciler(reconciliationClient, reconciliationCache, directoryManager, accounts, 1000, INTERVAL_MS);

  private final DirectoryReconciliationResponse successResponse  = new DirectoryReconciliationResponse(DirectoryReconciliationResponse.Status.OK);
  private final DirectoryReconciliationResponse notFoundResponse = new DirectoryReconciliationResponse(DirectoryReconciliationResponse.Status.MISSING);

  @Before
  public void setup() {
    when(account.getNumber()).thenReturn(VALID_NUMBER);
    when(account.isActive()).thenReturn(true);
    when(inactiveAccount.getNumber()).thenReturn(INACTIVE_NUMBER);
    when(inactiveAccount.isActive()).thenReturn(false);

    when(directoryManager.startBatchOperation()).thenReturn(batchOperationHandle);

    when(accounts.getAllFrom(anyInt())).thenReturn(Arrays.asList(account, inactiveAccount));
    when(accounts.getAllFrom(eq(VALID_NUMBER), anyInt())).thenReturn(Arrays.asList(inactiveAccount));
    when(accounts.getAllFrom(eq(INACTIVE_NUMBER), anyInt())).thenReturn(Collections.emptyList());

    when(reconciliationClient.sendChunk(any())).thenReturn(successResponse);

    when(reconciliationCache.getLastNumber()).thenReturn(Optional.empty());
    when(reconciliationCache.claimActiveWork(any(), anyLong())).thenReturn(true);
    when(reconciliationCache.isAccelerated()).thenReturn(false);
  }

  @Test
  public void testValid() {
    long delayMs = directoryReconciler.doPeriodicWork(INTERVAL_MS);

    assertThat(delayMs).isLessThanOrEqualTo(INTERVAL_MS);

    verify(accounts, times(1)).getAllFrom(anyInt());

    ArgumentCaptor<DirectoryReconciliationRequest> request = ArgumentCaptor.forClass(DirectoryReconciliationRequest.class);
    verify(reconciliationClient, times(1)).sendChunk(request.capture());

    assertThat(request.getValue().getFromNumber()).isNull();
    assertThat(request.getValue().getToNumber()).isEqualTo(INACTIVE_NUMBER);
    assertThat(request.getValue().getNumbers()).isEqualTo(Arrays.asList(VALID_NUMBER));

    ArgumentCaptor<ClientContact> addedContact = ArgumentCaptor.forClass(ClientContact.class);
    verify(directoryManager, times(1)).startBatchOperation();
    verify(directoryManager, times(1)).add(eq(batchOperationHandle), addedContact.capture());
    verify(directoryManager, times(1)).remove(eq(batchOperationHandle), eq(INACTIVE_NUMBER));
    verify(directoryManager, times(1)).stopBatchOperation(eq(batchOperationHandle));

    assertThat(addedContact.getValue().getToken()).isEqualTo(Util.getContactToken(VALID_NUMBER));

    verify(reconciliationCache, times(1)).getLastNumber();
    verify(reconciliationCache, times(1)).setLastNumber(eq(Optional.of(INACTIVE_NUMBER)));
    verify(reconciliationCache, times(1)).isAccelerated();
    verify(reconciliationCache, times(2)).claimActiveWork(any(), anyLong());

    verifyNoMoreInteractions(accounts);
    verifyNoMoreInteractions(directoryManager);
    verifyNoMoreInteractions(reconciliationClient);
    verifyNoMoreInteractions(reconciliationCache);
  }

  @Test
  public void testInProgress() {
    when(reconciliationCache.getLastNumber()).thenReturn(Optional.of(VALID_NUMBER));

    long delayMs = directoryReconciler.doPeriodicWork(INTERVAL_MS);

    assertThat(delayMs).isLessThanOrEqualTo(INTERVAL_MS);

    verify(accounts, times(1)).getAllFrom(eq(VALID_NUMBER), anyInt());

    ArgumentCaptor<DirectoryReconciliationRequest> request = ArgumentCaptor.forClass(DirectoryReconciliationRequest.class);
    verify(reconciliationClient, times(1)).sendChunk(request.capture());

    assertThat(request.getValue().getFromNumber()).isEqualTo(VALID_NUMBER);
    assertThat(request.getValue().getToNumber()).isEqualTo(INACTIVE_NUMBER);
    assertThat(request.getValue().getNumbers()).isEqualTo(Collections.emptyList());

    verify(directoryManager, times(1)).startBatchOperation();
    verify(directoryManager, times(1)).remove(eq(batchOperationHandle), eq(INACTIVE_NUMBER));
    verify(directoryManager, times(1)).stopBatchOperation(eq(batchOperationHandle));

    verify(reconciliationCache, times(1)).getLastNumber();
    verify(reconciliationCache, times(1)).setLastNumber(eq(Optional.of(INACTIVE_NUMBER)));
    verify(reconciliationCache, times(1)).isAccelerated();
    verify(reconciliationCache, times(2)).claimActiveWork(any(), anyLong());

    verifyNoMoreInteractions(accounts);
    verifyNoMoreInteractions(directoryManager);
    verifyNoMoreInteractions(reconciliationClient);
    verifyNoMoreInteractions(reconciliationCache);
  }

  @Test
  public void testLastChunk() {
    when(reconciliationCache.getLastNumber()).thenReturn(Optional.of(INACTIVE_NUMBER));

    long delayMs = directoryReconciler.doPeriodicWork(INTERVAL_MS);

    assertThat(delayMs).isLessThanOrEqualTo(INTERVAL_MS);

    verify(accounts, times(1)).getAllFrom(eq(INACTIVE_NUMBER), anyInt());

    ArgumentCaptor<DirectoryReconciliationRequest> request = ArgumentCaptor.forClass(DirectoryReconciliationRequest.class);
    verify(reconciliationClient, times(1)).sendChunk(request.capture());

    assertThat(request.getValue().getFromNumber()).isEqualTo(INACTIVE_NUMBER);
    assertThat(request.getValue().getToNumber()).isNull();
    assertThat(request.getValue().getNumbers()).isEqualTo(Collections.emptyList());

    verify(reconciliationCache, times(1)).getLastNumber();
    verify(reconciliationCache, times(1)).setLastNumber(eq(Optional.empty()));
    verify(reconciliationCache, times(1)).clearAccelerate();
    verify(reconciliationCache, times(1)).isAccelerated();
    verify(reconciliationCache, times(2)).claimActiveWork(any(), anyLong());

    verifyNoMoreInteractions(accounts);
    verifyNoMoreInteractions(directoryManager);
    verifyNoMoreInteractions(reconciliationClient);
    verifyNoMoreInteractions(reconciliationCache);
  }

  @Test
  public void testNotFound() {
    when(reconciliationClient.sendChunk(any())).thenReturn(notFoundResponse);

    long delayMs = directoryReconciler.doPeriodicWork(INTERVAL_MS);

    assertThat(delayMs).isLessThanOrEqualTo(INTERVAL_MS);

    verify(accounts, times(1)).getAllFrom(anyInt());

    ArgumentCaptor<DirectoryReconciliationRequest> request = ArgumentCaptor.forClass(DirectoryReconciliationRequest.class);
    verify(reconciliationClient, times(1)).sendChunk(request.capture());

    assertThat(request.getValue().getFromNumber()).isNull();
    assertThat(request.getValue().getToNumber()).isEqualTo(INACTIVE_NUMBER);
    assertThat(request.getValue().getNumbers()).isEqualTo(Arrays.asList(VALID_NUMBER));

    ArgumentCaptor<ClientContact> addedContact = ArgumentCaptor.forClass(ClientContact.class);
    verify(directoryManager, times(1)).startBatchOperation();
    verify(directoryManager, times(1)).add(eq(batchOperationHandle), addedContact.capture());
    verify(directoryManager, times(1)).remove(eq(batchOperationHandle), eq(INACTIVE_NUMBER));
    verify(directoryManager, times(1)).stopBatchOperation(eq(batchOperationHandle));

    assertThat(addedContact.getValue().getToken()).isEqualTo(Util.getContactToken(VALID_NUMBER));

    verify(reconciliationCache, times(1)).getLastNumber();
    verify(reconciliationCache, times(1)).setLastNumber(eq(Optional.empty()));
    verify(reconciliationCache, times(1)).clearAccelerate();
    verify(reconciliationCache, times(1)).claimActiveWork(any(), anyLong());

    verifyNoMoreInteractions(accounts);
    verifyNoMoreInteractions(directoryManager);
    verifyNoMoreInteractions(reconciliationClient);
    verifyNoMoreInteractions(reconciliationCache);
  }

}
