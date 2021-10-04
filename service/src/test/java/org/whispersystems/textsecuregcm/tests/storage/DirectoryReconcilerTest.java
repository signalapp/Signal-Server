/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.entities.DirectoryReconciliationRequest;
import org.whispersystems.textsecuregcm.entities.DirectoryReconciliationRequest.User;
import org.whispersystems.textsecuregcm.entities.DirectoryReconciliationResponse;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountDatabaseCrawlerRestartException;
import org.whispersystems.textsecuregcm.storage.DirectoryReconciler;
import org.whispersystems.textsecuregcm.storage.DirectoryReconciliationClient;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;

class DirectoryReconcilerTest {

  private static final UUID VALID_UUID = UUID.randomUUID();
  private static final String VALID_NUMBER = "+14152222222";
  private static final UUID UNDISCOVERABLE_UUID = UUID.randomUUID();
  private static final String UNDISCOVERABLE_NUMBER = "+14153333333";

  private final Account visibleAccount = mock(Account.class);
  private final Account undiscoverableAccount = mock(Account.class);
  private final DirectoryReconciliationClient reconciliationClient = mock(DirectoryReconciliationClient.class);
  private final DynamicConfigurationManager dynamicConfigurationManager = mock(DynamicConfigurationManager.class);
  private final DirectoryReconciler directoryReconciler = new DirectoryReconciler("test", reconciliationClient,
      dynamicConfigurationManager);

  private final DirectoryReconciliationResponse successResponse = new DirectoryReconciliationResponse(
      DirectoryReconciliationResponse.Status.OK);

  @BeforeEach
  void setup() {
    when(dynamicConfigurationManager.getConfiguration()).thenReturn(new DynamicConfiguration());

    when(visibleAccount.getUuid()).thenReturn(VALID_UUID);
    when(visibleAccount.getNumber()).thenReturn(VALID_NUMBER);
    when(visibleAccount.shouldBeVisibleInDirectory()).thenReturn(true);
    when(undiscoverableAccount.getUuid()).thenReturn(UNDISCOVERABLE_UUID);
    when(undiscoverableAccount.getNumber()).thenReturn(UNDISCOVERABLE_NUMBER);
    when(undiscoverableAccount.shouldBeVisibleInDirectory()).thenReturn(false);
  }

  @Test
  void testCrawlChunkValid() throws AccountDatabaseCrawlerRestartException {

    when(reconciliationClient.add(any())).thenReturn(successResponse);
    when(reconciliationClient.delete(any())).thenReturn(successResponse);

    directoryReconciler.timeAndProcessCrawlChunk(Optional.of(VALID_UUID),
        Arrays.asList(visibleAccount, undiscoverableAccount));

    ArgumentCaptor<DirectoryReconciliationRequest> chunkRequest = ArgumentCaptor.forClass(
        DirectoryReconciliationRequest.class);
    verify(reconciliationClient, times(1)).add(chunkRequest.capture());

    assertThat(chunkRequest.getValue().getUsers()).isEqualTo(List.of(new User(VALID_UUID, VALID_NUMBER)));

    ArgumentCaptor<DirectoryReconciliationRequest> deletesRequest = ArgumentCaptor.forClass(
        DirectoryReconciliationRequest.class);
    verify(reconciliationClient, times(1)).delete(deletesRequest.capture());

    assertThat(deletesRequest.getValue().getUsers()).isEqualTo(
        List.of(new User(UNDISCOVERABLE_UUID, UNDISCOVERABLE_NUMBER)));

    verifyNoMoreInteractions(reconciliationClient);
  }

}
