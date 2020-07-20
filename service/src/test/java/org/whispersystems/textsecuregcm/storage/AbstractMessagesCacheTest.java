package org.whispersystems.textsecuregcm.storage;

import com.google.protobuf.ByteString;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntity;
import org.whispersystems.textsecuregcm.redis.AbstractRedisClusterTest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(JUnitParamsRunner.class)
public abstract class AbstractMessagesCacheTest extends AbstractRedisClusterTest {

    private static final String DESTINATION_ACCOUNT   = "+18005551234";
    private static final UUID   DESTINATION_UUID      = UUID.randomUUID();
    private static final int    DESTINATION_DEVICE_ID = 7;

    private final Random random          = new Random();
    private       long   serialTimestamp = 0;

    protected abstract UserMessagesCache getMessagesCache();

    @Test
    @Parameters({"true", "false"})
    public void testInsert(final boolean sealedSender) {
        final UUID messageGuid = UUID.randomUUID();
        assertTrue(getMessagesCache().insert(messageGuid, DESTINATION_ACCOUNT, DESTINATION_UUID, DESTINATION_DEVICE_ID, generateRandomMessage(messageGuid, sealedSender)) > 0);
    }

    @Test
    @Parameters({"true", "false"})
    public void testRemoveById(final boolean sealedSender) {
        final UUID                   messageGuid = UUID.randomUUID();
        final MessageProtos.Envelope message     = generateRandomMessage(messageGuid, sealedSender);

        final long                            messageId           = getMessagesCache().insert(messageGuid, DESTINATION_ACCOUNT, DESTINATION_UUID, DESTINATION_DEVICE_ID, message);
        final Optional<OutgoingMessageEntity> maybeRemovedMessage = getMessagesCache().remove(DESTINATION_ACCOUNT, DESTINATION_UUID, DESTINATION_DEVICE_ID, messageId);

        assertTrue(maybeRemovedMessage.isPresent());
        assertEquals(UserMessagesCache.constructEntityFromEnvelope(messageId, message), maybeRemovedMessage.get());
        assertEquals(Optional.empty(), getMessagesCache().remove(DESTINATION_ACCOUNT, DESTINATION_UUID, DESTINATION_DEVICE_ID, messageId));
    }

    @Test
    public void testRemoveBySender() {
        final UUID                   messageGuid = UUID.randomUUID();
        final MessageProtos.Envelope message     = generateRandomMessage(messageGuid, false);

        getMessagesCache().insert(messageGuid, DESTINATION_ACCOUNT, DESTINATION_UUID, DESTINATION_DEVICE_ID, message);
        final Optional<OutgoingMessageEntity> maybeRemovedMessage = getMessagesCache().remove(DESTINATION_ACCOUNT, DESTINATION_UUID, DESTINATION_DEVICE_ID, message.getSource(), message.getTimestamp());

        assertTrue(maybeRemovedMessage.isPresent());
        assertEquals(UserMessagesCache.constructEntityFromEnvelope(0, message), maybeRemovedMessage.get());
        assertEquals(Optional.empty(), getMessagesCache().remove(DESTINATION_ACCOUNT, DESTINATION_UUID, DESTINATION_DEVICE_ID, message.getSource(), message.getTimestamp()));
    }

    @Test
    @Parameters({"true", "false"})
    public void testRemoveByUUID(final boolean sealedSender) {
        final UUID messageGuid = UUID.randomUUID();

        assertEquals(Optional.empty(), getMessagesCache().remove(DESTINATION_ACCOUNT, DESTINATION_UUID, DESTINATION_DEVICE_ID, messageGuid));

        final MessageProtos.Envelope message = generateRandomMessage(messageGuid, sealedSender);

        getMessagesCache().insert(messageGuid, DESTINATION_ACCOUNT, DESTINATION_UUID, DESTINATION_DEVICE_ID, message);
        final Optional<OutgoingMessageEntity> maybeRemovedMessage = getMessagesCache().remove(DESTINATION_ACCOUNT, DESTINATION_UUID, DESTINATION_DEVICE_ID, messageGuid);

        assertTrue(maybeRemovedMessage.isPresent());
        assertEquals(UserMessagesCache.constructEntityFromEnvelope(0, message), maybeRemovedMessage.get());
    }

    @Test
    @Parameters({"true", "false"})
    public void testGetMessages(final boolean sealedSender) {
        final int messageCount = 100;

        final List<OutgoingMessageEntity> expectedMessages = new ArrayList<>(messageCount);

        for (int i = 0; i < messageCount; i++) {
            final UUID                   messageGuid = UUID.randomUUID();
            final MessageProtos.Envelope message     = generateRandomMessage(messageGuid, sealedSender);
            final long                   messageId   = getMessagesCache().insert(messageGuid, DESTINATION_ACCOUNT, DESTINATION_UUID, DESTINATION_DEVICE_ID, message);

            expectedMessages.add(UserMessagesCache.constructEntityFromEnvelope(messageId, message));
        }

        assertEquals(expectedMessages, getMessagesCache().get(DESTINATION_ACCOUNT, DESTINATION_UUID, DESTINATION_DEVICE_ID, messageCount));
    }

    @Test
    @Parameters({"true", "false"})
    public void testClearQueueForDevice(final boolean sealedSender) {
        final int messageCount = 100;

        for (final int deviceId : new int[] { DESTINATION_DEVICE_ID, DESTINATION_DEVICE_ID + 1 }) {
            for (int i = 0; i < messageCount; i++) {
                final UUID                   messageGuid = UUID.randomUUID();
                final MessageProtos.Envelope message     = generateRandomMessage(messageGuid, sealedSender);

                getMessagesCache().insert(messageGuid, DESTINATION_ACCOUNT, DESTINATION_UUID, deviceId, message);
            }
        }

        getMessagesCache().clear(DESTINATION_ACCOUNT, DESTINATION_UUID, DESTINATION_DEVICE_ID);

        assertEquals(Collections.emptyList(), getMessagesCache().get(DESTINATION_ACCOUNT, DESTINATION_UUID, DESTINATION_DEVICE_ID, messageCount));
        assertEquals(messageCount, getMessagesCache().get(DESTINATION_ACCOUNT, DESTINATION_UUID, DESTINATION_DEVICE_ID + 1, messageCount).size());
    }

    @Test
    @Parameters({"true", "false"})
    public void testClearQueueForAccount(final boolean sealedSender) {
        final int messageCount = 100;

        for (final int deviceId : new int[] { DESTINATION_DEVICE_ID, DESTINATION_DEVICE_ID + 1 }) {
            for (int i = 0; i < messageCount; i++) {
                final UUID                   messageGuid = UUID.randomUUID();
                final MessageProtos.Envelope message     = generateRandomMessage(messageGuid, sealedSender);

                getMessagesCache().insert(messageGuid, DESTINATION_ACCOUNT, DESTINATION_UUID, deviceId, message);
            }
        }

        getMessagesCache().clear(DESTINATION_ACCOUNT, DESTINATION_UUID);

        assertEquals(Collections.emptyList(), getMessagesCache().get(DESTINATION_ACCOUNT, DESTINATION_UUID, DESTINATION_DEVICE_ID, messageCount));
        assertEquals(Collections.emptyList(), getMessagesCache().get(DESTINATION_ACCOUNT, DESTINATION_UUID, DESTINATION_DEVICE_ID + 1, messageCount));
    }

    protected MessageProtos.Envelope generateRandomMessage(final UUID messageGuid, final boolean sealedSender) {
        final MessageProtos.Envelope.Builder envelopeBuilder = MessageProtos.Envelope.newBuilder()
                .setTimestamp(serialTimestamp++)
                .setServerTimestamp(serialTimestamp++)
                .setContent(ByteString.copyFromUtf8(RandomStringUtils.randomAlphanumeric(256)))
                .setType(MessageProtos.Envelope.Type.CIPHERTEXT)
                .setServerGuid(messageGuid.toString());

        if (!sealedSender) {
            envelopeBuilder.setSourceDevice(random.nextInt(256))
                    .setSource("+1" + RandomStringUtils.randomNumeric(10));
        }

        return envelopeBuilder.build();
    }
}
