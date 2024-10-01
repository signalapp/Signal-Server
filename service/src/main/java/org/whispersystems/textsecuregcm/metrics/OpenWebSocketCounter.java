package org.whispersystems.textsecuregcm.metrics;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import org.whispersystems.textsecuregcm.util.ua.ClientPlatform;
import org.whispersystems.textsecuregcm.util.ua.UnrecognizedUserAgentException;
import org.whispersystems.textsecuregcm.util.ua.UserAgentUtil;
import org.whispersystems.websocket.session.WebSocketSessionContext;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class OpenWebSocketCounter {

  private final Map<ClientPlatform, AtomicInteger> openWebsocketsByClientPlatform;
  private final AtomicInteger openWebsocketsFromUnknownPlatforms;

  public OpenWebSocketCounter(final String openWebSocketGaugeName) {
    this(openWebSocketGaugeName, Tags.empty());
  }

  public OpenWebSocketCounter(final String openWebSocketGaugeName, final Tags tags) {
    openWebsocketsByClientPlatform = Arrays.stream(ClientPlatform.values())
            .collect(Collectors.toMap(
                clientPlatform -> clientPlatform,
                clientPlatform -> buildGauge(openWebSocketGaugeName, clientPlatform.name().toLowerCase(), tags),
                (a, b) -> {
                  throw new AssertionError("Duplicate client platform enumeration key");
                },
                () -> new EnumMap<>(ClientPlatform.class)
            ));

    openWebsocketsFromUnknownPlatforms = buildGauge(openWebSocketGaugeName, "unknown", tags);
  }

  private static AtomicInteger buildGauge(final String gaugeName, final String clientPlatformName, final Tags tags) {
    return Metrics.gauge(gaugeName,
        tags.and(Tag.of(UserAgentTagUtil.PLATFORM_TAG, clientPlatformName)),
        new AtomicInteger(0));
  }

  public void countOpenWebSocket(final WebSocketSessionContext context) {
    final AtomicInteger openWebSocketCounter = getOpenWebsocketCounter(context.getClient().getUserAgent());

    openWebSocketCounter.incrementAndGet();
    context.addWebsocketClosedListener((context1, statusCode, reason) -> openWebSocketCounter.decrementAndGet());
  }

  private AtomicInteger getOpenWebsocketCounter(final String userAgentString) {
    try {
      return openWebsocketsByClientPlatform.get(UserAgentUtil.parseUserAgentString(userAgentString).getPlatform());
    } catch (final UnrecognizedUserAgentException e) {
      return openWebsocketsFromUnknownPlatforms;
    }
  }
}
