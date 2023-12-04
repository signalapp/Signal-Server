package org.whispersystems.textsecuregcm.push;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.ArgumentCaptor;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.redis.RedisSingletonExtension;
import org.whispersystems.textsecuregcm.storage.PubSubProtos;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import org.whispersystems.textsecuregcm.websocket.ProvisioningAddress;

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
    final ProvisioningAddress address = ProvisioningAddress.create("address");

    final byte[] content = TestRandomUtil.nextBytes(16);

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
    final ProvisioningAddress address = ProvisioningAddress.create("address");

    final byte[] content = TestRandomUtil.nextBytes(16);

    @SuppressWarnings("unchecked") final Consumer<PubSubProtos.PubSubMessage> subscribedConsumer = mock(Consumer.class);

    provisioningManager.addListener(address, subscribedConsumer);
    provisioningManager.removeListener(address);
    provisioningManager.sendProvisioningMessage(address, content);

    // Make sure that we give the message enough time to show up (if it was going to) before declaring victory
    verify(subscribedConsumer, after(PUBSUB_TIMEOUT_MILLIS).never()).accept(any());
  }
}
