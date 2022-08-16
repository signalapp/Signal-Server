package org.whispersystems.textsecuregcm.push;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.ArgumentCaptor;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.redis.RedisSingletonExtension;
import org.whispersystems.textsecuregcm.storage.PubSubProtos;
import org.whispersystems.textsecuregcm.websocket.ProvisioningAddress;

import java.time.Duration;
import java.util.Random;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

class ProvisioningManagerTest {

  private ProvisioningManager provisioningManager;

  @RegisterExtension
  static final RedisSingletonExtension REDIS_EXTENSION = RedisSingletonExtension.builder().build();

  private static final long PUBSUB_TIMEOUT_MILLIS = 1_000;

  @BeforeEach
  void setUp() throws Exception {
    provisioningManager = new ProvisioningManager(REDIS_EXTENSION.getRedisClient(), Duration.ofSeconds(1), new CircuitBreakerConfiguration());
    provisioningManager.start();
  }

  @AfterEach
  void tearDown() throws Exception {
    provisioningManager.stop();
  }

  @Test
  void sendProvisioningMessage() {
    final ProvisioningAddress address = new ProvisioningAddress("address", 0);

    final byte[] content = new byte[16];
    new Random().nextBytes(content);

    @SuppressWarnings("unchecked") final Consumer<PubSubProtos.PubSubMessage> subscribedConsumer = mock(Consumer.class);

    provisioningManager.addListener(address, subscribedConsumer);
    provisioningManager.sendProvisioningMessage(address, content);

    final ArgumentCaptor<PubSubProtos.PubSubMessage> messageCaptor =
        ArgumentCaptor.forClass(PubSubProtos.PubSubMessage.class);

    verify(subscribedConsumer, timeout(PUBSUB_TIMEOUT_MILLIS)).accept(messageCaptor.capture());

    assertEquals(PubSubProtos.PubSubMessage.Type.DELIVER, messageCaptor.getValue().getType());
    assertEquals(ByteString.copyFrom(content), messageCaptor.getValue().getContent());
  }

  @Test
  void removeListener() {
    final ProvisioningAddress address = new ProvisioningAddress("address", 0);

    final byte[] content = new byte[16];
    new Random().nextBytes(content);

    @SuppressWarnings("unchecked") final Consumer<PubSubProtos.PubSubMessage> subscribedConsumer = mock(Consumer.class);

    provisioningManager.addListener(address, subscribedConsumer);
    provisioningManager.removeListener(address);
    provisioningManager.sendProvisioningMessage(address, content);

    // Make sure that we give the message enough time to show up (if it was going to) before declaring victory
    verify(subscribedConsumer, after(PUBSUB_TIMEOUT_MILLIS).never()).accept(any());
  }
}
