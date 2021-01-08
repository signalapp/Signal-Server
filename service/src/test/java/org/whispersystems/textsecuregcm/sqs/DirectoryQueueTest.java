/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.sqs;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
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

        final ArgumentCaptor<SendMessageRequest> requestCaptor = ArgumentCaptor.forClass(SendMessageRequest.class);
        verify(sqs).sendMessage(requestCaptor.capture());

        final Map<String, MessageAttributeValue> messageAttributes = requestCaptor.getValue().getMessageAttributes();
        assertEquals(new MessageAttributeValue().withDataType("String").withStringValue(expectedAction), messageAttributes.get("action"));
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

        final ArgumentCaptor<SendMessageRequest> requestCaptor = ArgumentCaptor.forClass(SendMessageRequest.class);
        verify(sqs, times(2)).sendMessage(requestCaptor.capture());

        for (final SendMessageRequest sendMessageRequest : requestCaptor.getAllValues()) {
            final Map<String, MessageAttributeValue> messageAttributes = sendMessageRequest.getMessageAttributes();
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
