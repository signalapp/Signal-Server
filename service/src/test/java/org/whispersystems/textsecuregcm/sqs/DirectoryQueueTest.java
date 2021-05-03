/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.sqs;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.whispersystems.textsecuregcm.storage.Account;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(JUnitParamsRunner.class)
public class DirectoryQueueTest {

    @Test
    @Parameters(method = "argumentsForTestRefreshRegisteredUser")
    public void testRefreshRegisteredUser(final boolean accountEnabled, final boolean accountDiscoverableByPhoneNumber, final String expectedAction) {
        final AmazonSQS      sqs            = mock(AmazonSQS.class);
        final DirectoryQueue directoryQueue = new DirectoryQueue(List.of("sqs://test"), sqs);

        final Account account = mock(Account.class);
        when(account.getNumber()).thenReturn("+18005556543");
        when(account.getUuid()).thenReturn(UUID.randomUUID());
        when(account.isEnabled()).thenReturn(accountEnabled);
        when(account.isDiscoverableByPhoneNumber()).thenReturn(accountDiscoverableByPhoneNumber);

        directoryQueue.refreshRegisteredUser(account);

        final ArgumentCaptor<SendMessageBatchRequest> requestCaptor = ArgumentCaptor.forClass(SendMessageBatchRequest.class);
        verify(sqs).sendMessageBatch(requestCaptor.capture());

        assertEquals(1, requestCaptor.getValue().getEntries().size());

        final Map<String, MessageAttributeValue> messageAttributes = requestCaptor.getValue().getEntries().get(0).getMessageAttributes();
        assertEquals(new MessageAttributeValue().withDataType("String").withStringValue(expectedAction), messageAttributes.get("action"));
    }

    @Test
    public void testRefreshBatch() {
        final AmazonSQS sqs = mock(AmazonSQS.class);
        final DirectoryQueue directoryQueue = new DirectoryQueue(List.of("sqs://test"), sqs);

        final Account discoverableAccount = mock(Account.class);
        when(discoverableAccount.getNumber()).thenReturn("+18005556543");
        when(discoverableAccount.getUuid()).thenReturn(UUID.randomUUID());
        when(discoverableAccount.isEnabled()).thenReturn(true);
        when(discoverableAccount.isDiscoverableByPhoneNumber()).thenReturn(true);

        final Account undiscoverableAccount = mock(Account.class);
        when(undiscoverableAccount.getNumber()).thenReturn("+18005550987");
        when(undiscoverableAccount.getUuid()).thenReturn(UUID.randomUUID());
        when(undiscoverableAccount.isEnabled()).thenReturn(true);
        when(undiscoverableAccount.isDiscoverableByPhoneNumber()).thenReturn(false);

        directoryQueue.refreshRegisteredUsers(List.of(discoverableAccount, undiscoverableAccount));

        final ArgumentCaptor<SendMessageBatchRequest> requestCaptor = ArgumentCaptor.forClass(SendMessageBatchRequest.class);
        verify(sqs).sendMessageBatch(requestCaptor.capture());

        assertEquals(2, requestCaptor.getValue().getEntries().size());

        final Map<String, MessageAttributeValue> discoverableAccountAttributes = requestCaptor.getValue().getEntries().get(0).getMessageAttributes();
        assertEquals(new MessageAttributeValue().withDataType("String").withStringValue(discoverableAccount.getNumber()), discoverableAccountAttributes.get("id"));
        assertEquals(new MessageAttributeValue().withDataType("String").withStringValue(discoverableAccount.getUuid().toString()), discoverableAccountAttributes.get("uuid"));
        assertEquals(new MessageAttributeValue().withDataType("String").withStringValue("add"), discoverableAccountAttributes.get("action"));

        final Map<String, MessageAttributeValue> undiscoverableAccountAttributes = requestCaptor.getValue().getEntries().get(1).getMessageAttributes();
        assertEquals(new MessageAttributeValue().withDataType("String").withStringValue(undiscoverableAccount.getNumber()), undiscoverableAccountAttributes.get("id"));
        assertEquals(new MessageAttributeValue().withDataType("String").withStringValue(undiscoverableAccount.getUuid().toString()), undiscoverableAccountAttributes.get("uuid"));
        assertEquals(new MessageAttributeValue().withDataType("String").withStringValue("delete"), undiscoverableAccountAttributes.get("action"));
    }

    @Test
    public void testSendMessageMultipleQueues() {
        final AmazonSQS      sqs            = mock(AmazonSQS.class);
        final DirectoryQueue directoryQueue = new DirectoryQueue(List.of("sqs://first", "sqs://second"), sqs);

        final Account account = mock(Account.class);
        when(account.getNumber()).thenReturn("+18005556543");
        when(account.getUuid()).thenReturn(UUID.randomUUID());
        when(account.isEnabled()).thenReturn(true);
        when(account.isDiscoverableByPhoneNumber()).thenReturn(true);

        directoryQueue.refreshRegisteredUser(account);

        final ArgumentCaptor<SendMessageBatchRequest> requestCaptor = ArgumentCaptor.forClass(SendMessageBatchRequest.class);
        verify(sqs, times(2)).sendMessageBatch(requestCaptor.capture());

        for (final SendMessageBatchRequest sendMessageBatchRequest : requestCaptor.getAllValues()) {
            assertEquals(1, requestCaptor.getValue().getEntries().size());

            final Map<String, MessageAttributeValue> messageAttributes = sendMessageBatchRequest.getEntries().get(0).getMessageAttributes();
            assertEquals(new MessageAttributeValue().withDataType("String").withStringValue("add"), messageAttributes.get("action"));
        }
    }

    @SuppressWarnings("unused")
    private Object argumentsForTestRefreshRegisteredUser() {
        return new Object[] {
                new Object[] { true,  true,  "add"    },
                new Object[] { true,  false, "delete" },
                new Object[] { false, true,  "delete" },
                new Object[] { false, false, "delete" }
        };
    }
}
