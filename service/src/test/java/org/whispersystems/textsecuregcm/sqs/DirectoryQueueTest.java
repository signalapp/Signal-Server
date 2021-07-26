/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.sqs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.whispersystems.textsecuregcm.storage.Account;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

public class DirectoryQueueTest {

  private SqsAsyncClient sqsAsyncClient;

  @BeforeEach
  void setUp() {
    sqsAsyncClient = mock(SqsAsyncClient.class);

    when(sqsAsyncClient.sendMessage(any(SendMessageRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(SendMessageResponse.builder().build()));
  }

  @ParameterizedTest
  @MethodSource("argumentsForTestRefreshRegisteredUser")
  void testRefreshRegisteredUser(final boolean accountEnabled, final boolean accountDiscoverableByPhoneNumber, final String expectedAction) {
    final DirectoryQueue directoryQueue = new DirectoryQueue(List.of("sqs://test"), sqsAsyncClient);

    final Account account = mock(Account.class);
    when(account.getNumber()).thenReturn("+18005556543");
    when(account.getUuid()).thenReturn(UUID.randomUUID());
    when(account.isEnabled()).thenReturn(accountEnabled);
    when(account.isDiscoverableByPhoneNumber()).thenReturn(accountDiscoverableByPhoneNumber);

    directoryQueue.refreshAccount(account);

    final ArgumentCaptor<SendMessageRequest> requestCaptor = ArgumentCaptor.forClass(SendMessageRequest.class);
    verify(sqsAsyncClient).sendMessage(requestCaptor.capture());

    assertEquals(MessageAttributeValue.builder().dataType("String").stringValue(expectedAction).build(),
        requestCaptor.getValue().messageAttributes().get("action"));
  }

  @SuppressWarnings("unused")
  private static Stream<Arguments> argumentsForTestRefreshRegisteredUser() {
    return Stream.of(
        Arguments.of(true, true, "add"),
        Arguments.of(true, false, "delete"),
        Arguments.of(false, true, "delete"),
        Arguments.of(false, false, "delete"));
  }

  @Test
  void testSendMessageMultipleQueues() {
    final DirectoryQueue directoryQueue = new DirectoryQueue(List.of("sqs://first", "sqs://second"), sqsAsyncClient);

    final Account account = mock(Account.class);
    when(account.getNumber()).thenReturn("+18005556543");
    when(account.getUuid()).thenReturn(UUID.randomUUID());
    when(account.isEnabled()).thenReturn(true);
    when(account.isDiscoverableByPhoneNumber()).thenReturn(true);

    directoryQueue.refreshAccount(account);

    final ArgumentCaptor<SendMessageRequest> requestCaptor = ArgumentCaptor.forClass(SendMessageRequest.class);
    verify(sqsAsyncClient, times(2)).sendMessage(requestCaptor.capture());

    for (final SendMessageRequest sendMessageRequest : requestCaptor.getAllValues()) {
      assertEquals(MessageAttributeValue.builder().dataType("String").stringValue("add").build(),
          sendMessageRequest.messageAttributes().get("action"));
    }
  }
}
