package org.whispersystems.textsecuregcm.push;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import com.google.protobuf.ByteString;
import java.util.UUID;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.ArgumentCaptor;
import org.whispersystems.textsecuregcm.redis.RedisServerExtension;
import org.whispersystems.textsecuregcm.storage.PubSubProtos;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

class ProvisioningManagerTest {

  private ProvisioningManager provisioningManager;

  @RegisterExtension
  static final RedisServerExtension REDIS_EXTENSION = RedisServerExtension.builder().build();

  private static final long PUBSUB_TIMEOUT_MILLIS = 1_000;

  @BeforeEach
  void setUp() throws Exception {
    provisioningManager = new ProvisioningManager(REDIS_EXTENSION.getRedisClient());
    provisioningManager.start();
  }

  @AfterEach
  void tearDown() throws Exception {
    provisioningManager.stop();
  }

  @Test
  void sendProvisioningMessage() {
    final String provisioningAddress = UUID.randomUUID().toString();
    final byte[] content = TestRandomUtil.nextBytes(16);

    @SuppressWarnings("unchecked") final Consumer<PubSubProtos.PubSubMessage> subscribedConsumer = mock(Consumer.class);

    provisioningManager.addListener(provisioningAddress, subscribedConsumer);
    provisioningManager.sendProvisioningMessage(provisioningAddress, content);

    final ArgumentCaptor<PubSubProtos.PubSubMessage> messageCaptor =
        ArgumentCaptor.forClass(PubSubProtos.PubSubMessage.class);

    verify(subscribedConsumer, timeout(PUBSUB_TIMEOUT_MILLIS)).accept(messageCaptor.capture());

    assertEquals(PubSubProtos.PubSubMessage.Type.DELIVER, messageCaptor.getValue().getType());
    assertEquals(ByteString.copyFrom(content), messageCaptor.getValue().getContent());
  }

  @Test
  void removeListener() {
    final String provisioningAddress = UUID.randomUUID().toString();
    final byte[] content = TestRandomUtil.nextBytes(16);

    @SuppressWarnings("unchecked") final Consumer<PubSubProtos.PubSubMessage> subscribedConsumer = mock(Consumer.class);

    provisioningManager.addListener(provisioningAddress, subscribedConsumer);
    provisioningManager.removeListener(provisioningAddress);
    provisioningManager.sendProvisioningMessage(provisioningAddress, content);

    // Make sure that we give the message enough time to show up (if it was going to) before declaring victory
    verify(subscribedConsumer, after(PUBSUB_TIMEOUT_MILLIS).never()).accept(any());
  }
}
