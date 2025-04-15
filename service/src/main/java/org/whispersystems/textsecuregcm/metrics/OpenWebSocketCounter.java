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

  private final Map<ClientPlatform, AtomicInteger> openWebsocketsByClientPlatform;
  private final AtomicInteger openWebsocketsFromUnknownPlatforms;

  private final Map<ClientPlatform, Timer> durationTimersByClientPlatform;
  private final Timer durationTimerForUnknownPlatforms;

  public OpenWebSocketCounter(final String openWebSocketGaugeName, final String durationTimerName) {
    this(openWebSocketGaugeName, durationTimerName, Tags.empty());
  }

  public OpenWebSocketCounter(final String openWebSocketGaugeName, final String durationTimerName, final Tags tags) {
    openWebsocketsByClientPlatform = EnumMapUtil.toEnumMap(ClientPlatform.class,
        clientPlatform -> buildGauge(openWebSocketGaugeName, clientPlatform.name().toLowerCase(), tags));

    openWebsocketsFromUnknownPlatforms = buildGauge(openWebSocketGaugeName, "unknown", tags);

    durationTimersByClientPlatform = EnumMapUtil.toEnumMap(ClientPlatform.class,
        clientPlatform -> buildTimer(durationTimerName, clientPlatform.name().toLowerCase(), tags));

    durationTimerForUnknownPlatforms = buildTimer(durationTimerName, "unknown", tags);
  }

  private static AtomicInteger buildGauge(final String gaugeName, final String clientPlatformName, final Tags tags) {
    return Metrics.gauge(gaugeName,
        tags.and(Tag.of(UserAgentTagUtil.PLATFORM_TAG, clientPlatformName)),
        new AtomicInteger(0));
  }

  private static Timer buildTimer(final String timerName, final String clientPlatformName, final Tags tags) {
    return Timer.builder(timerName)
        .publishPercentileHistogram(true)
        .tags(tags.and(Tag.of(UserAgentTagUtil.PLATFORM_TAG, clientPlatformName)))
        .register(Metrics.globalRegistry);
  }

  public void countOpenWebSocket(final WebSocketSessionContext context) {
    final Timer.Sample sample = Timer.start();

    // We have to jump through some hoops here to have something "effectively final" for the close listener, but
    // assignable from a `catch` block.
    final AtomicInteger openWebSocketCounter;
    final Timer durationTimer;

    {
      AtomicInteger calculatedOpenWebSocketCounter;
      Timer calculatedDurationTimer;

      try {
        final ClientPlatform clientPlatform =
            UserAgentUtil.parseUserAgentString(context.getClient().getUserAgent()).platform();

        calculatedOpenWebSocketCounter = openWebsocketsByClientPlatform.get(clientPlatform);
        calculatedDurationTimer = durationTimersByClientPlatform.get(clientPlatform);
      } catch (final UnrecognizedUserAgentException e) {
        calculatedOpenWebSocketCounter = openWebsocketsFromUnknownPlatforms;
        calculatedDurationTimer = durationTimerForUnknownPlatforms;
      }

      openWebSocketCounter = calculatedOpenWebSocketCounter;
      durationTimer = calculatedDurationTimer;
    }

    openWebSocketCounter.incrementAndGet();

    context.addWebsocketClosedListener((context1, statusCode, reason) -> {
      sample.stop(durationTimer);
      openWebSocketCounter.decrementAndGet();

      Metrics.counter(WEBSOCKET_CLOSED_COUNTER_NAME, "status", String.valueOf(statusCode))
          .increment();
    });
  }
}
