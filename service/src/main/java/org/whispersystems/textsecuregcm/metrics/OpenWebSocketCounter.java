package org.whispersystems.textsecuregcm.metrics;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.whispersystems.textsecuregcm.util.EnumMapUtil;
import org.whispersystems.textsecuregcm.util.ua.ClientPlatform;
import org.whispersystems.textsecuregcm.util.ua.UnrecognizedUserAgentException;
import org.whispersystems.textsecuregcm.util.ua.UserAgentUtil;
import org.whispersystems.websocket.session.WebSocketSessionContext;

public class OpenWebSocketCounter {

  private static final String WEBSOCKET_CLOSED_COUNTER_NAME = name(OpenWebSocketCounter.class, "websocketClosed");

  private final String newConnectionCounterName;
  private final String durationTimerName;

  private final Tags tags;

  private final Map<ClientPlatform, AtomicInteger> openWebsocketsByClientPlatform;
  private final AtomicInteger openWebsocketsFromUnknownPlatforms;

  public OpenWebSocketCounter(final String openWebSocketGaugeName,
      final String newConnectionCounterName,
      final String durationTimerName) {

    this(openWebSocketGaugeName, newConnectionCounterName, durationTimerName, Tags.empty());
  }

  public OpenWebSocketCounter(final String openWebSocketGaugeName,
      final String newConnectionCounterName,
      final String durationTimerName,
      final Tags tags) {

    this.newConnectionCounterName = newConnectionCounterName;
    this.durationTimerName = durationTimerName;

    this.tags = tags;

    openWebsocketsByClientPlatform = EnumMapUtil.toEnumMap(ClientPlatform.class,
        clientPlatform -> buildGauge(openWebSocketGaugeName, clientPlatform.name().toLowerCase(), tags));

    openWebsocketsFromUnknownPlatforms = buildGauge(openWebSocketGaugeName, "unknown", tags);
  }

  private static AtomicInteger buildGauge(final String gaugeName, final String clientPlatformName, final Tags tags) {
    return Metrics.gauge(gaugeName,
        tags.and(Tag.of(UserAgentTagUtil.PLATFORM_TAG, clientPlatformName)),
        new AtomicInteger(0));
  }

  public void countOpenWebSocket(final WebSocketSessionContext context) {
    final Timer.Sample sample = Timer.start();

    // We have to jump through some hoops here to have something "effectively final" for the close listener, but
    // assignable from a `catch` block.
    final AtomicInteger openWebSocketCounter;

    {
      AtomicInteger calculatedOpenWebSocketCounter;

      try {
        final ClientPlatform clientPlatform =
            UserAgentUtil.parseUserAgentString(context.getClient().getUserAgent()).platform();

        calculatedOpenWebSocketCounter = openWebsocketsByClientPlatform.get(clientPlatform);
      } catch (final UnrecognizedUserAgentException e) {
        calculatedOpenWebSocketCounter = openWebsocketsFromUnknownPlatforms;
      }

      openWebSocketCounter = calculatedOpenWebSocketCounter;
    }

    openWebSocketCounter.incrementAndGet();

    final Tags tagsWithClientPlatform = tags.and(UserAgentTagUtil.getPlatformTag(context.getClient().getUserAgent()));

    Metrics.counter(newConnectionCounterName, tagsWithClientPlatform).increment();

    context.addWebsocketClosedListener((_, statusCode, _) -> {
      sample.stop(Timer.builder(durationTimerName)
          .publishPercentileHistogram(true)
          .tags(tagsWithClientPlatform)
          .register(Metrics.globalRegistry));

      openWebSocketCounter.decrementAndGet();

      Metrics.counter(WEBSOCKET_CLOSED_COUNTER_NAME, tagsWithClientPlatform.and("status", String.valueOf(statusCode)))
          .increment();
    });
  }
}
