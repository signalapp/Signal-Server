package org.whispersystems.textsecuregcm.metrics;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.storage.ClientReleaseManager;
import org.whispersystems.textsecuregcm.util.ua.UnrecognizedUserAgentException;
import org.whispersystems.textsecuregcm.util.ua.UserAgent;
import org.whispersystems.textsecuregcm.util.ua.UserAgentUtil;
import org.whispersystems.websocket.session.WebSocketSessionContext;

public class OpenWebSocketCounter {

  private final ClientReleaseManager clientReleaseManager;

  private final Tags baseTags;

  private final Map<Tags, AtomicInteger> openWebsocketsByTags;
  private final AtomicInteger totalConnections;

  private static final int MAX_COUNTERS = 4096;

  private static final String OPEN_WEBSOCKET_GAUGE_NAME = name(OpenWebSocketCounter.class, "openWebsockets");
  private static final String TOTAL_CONNECTIONS_GAUGE_NAME = name(OpenWebSocketCounter.class, "totalOpenWebsockets");
  private static final String NEW_CONNECTION_COUNTER_NAME = name(OpenWebSocketCounter.class, "newConnections");
  private static final String WEB_SOCKET_CLOSED_COUNTER_NAME = name(OpenWebSocketCounter.class, "websocketClosed");
  private static final String SESSION_DURATION_TIMER_NAME = name(OpenWebSocketCounter.class, "sessionDuration");
  private static final String GAUGE_COUNT_GAUGE_NAME = name(OpenWebSocketCounter.class, "gaugeCount");

  public OpenWebSocketCounter(final String webSocketType,
      final ClientReleaseManager clientReleaseManager) {

    this.clientReleaseManager = clientReleaseManager;

    this.baseTags = Tags.of("webSocketType", webSocketType);
    this.openWebsocketsByTags = Metrics.gaugeMapSize(GAUGE_COUNT_GAUGE_NAME, baseTags, new ConcurrentHashMap<>());

    this.totalConnections = Metrics.gauge(TOTAL_CONNECTIONS_GAUGE_NAME, baseTags, new AtomicInteger(0));
  }

  public void countOpenWebSocket(final WebSocketSessionContext context) {
    final Timer.Sample sample = Timer.start();

    @Nullable final UserAgent userAgent;
    {
      UserAgent parsedUserAgent;

      try {
        parsedUserAgent = UserAgentUtil.parseUserAgentString(context.getClient().getUserAgent());
      } catch (final UnrecognizedUserAgentException e) {
        parsedUserAgent = null;
      }

      userAgent = parsedUserAgent;
    }

    final Tags tagsWithClientPlatform = baseTags.and(UserAgentTagUtil.getPlatformTag(userAgent));

    final Optional<AtomicInteger> maybeOpenWebSocketCounter;
    {
      final Tags tagsWithAdditionalSpecifiers = tagsWithClientPlatform
          .and(UserAgentTagUtil.getClientVersionTag(userAgent, clientReleaseManager)
              .map(Tags::of)
              .orElseGet(Tags::empty))
          .and(UserAgentTagUtil.getAdditionalSpecifierTags(userAgent));

      maybeOpenWebSocketCounter = getCounter(tagsWithAdditionalSpecifiers);
    }

    maybeOpenWebSocketCounter.ifPresent(AtomicInteger::incrementAndGet);
    totalConnections.incrementAndGet();

    Metrics.counter(NEW_CONNECTION_COUNTER_NAME, tagsWithClientPlatform).increment();

    context.addWebsocketClosedListener((_, statusCode, _) -> {
      sample.stop(Timer.builder(SESSION_DURATION_TIMER_NAME)
          .tags(tagsWithClientPlatform)
          .register(Metrics.globalRegistry));

      maybeOpenWebSocketCounter.ifPresent(AtomicInteger::decrementAndGet);
      totalConnections.decrementAndGet();

      Metrics.counter(WEB_SOCKET_CLOSED_COUNTER_NAME, tagsWithClientPlatform.and("status", String.valueOf(statusCode)))
          .increment();
    });
  }

  private Optional<AtomicInteger> getCounter(final Tags tags) {
    // Make a reasonable effort to avoid creating new counters if we're already full
    return openWebsocketsByTags.size() >= MAX_COUNTERS
        ? Optional.ofNullable(openWebsocketsByTags.get(tags))
        : Optional.of(openWebsocketsByTags.computeIfAbsent(tags,
            t -> Metrics.gauge(OPEN_WEBSOCKET_GAUGE_NAME, t, new AtomicInteger(0))));
  }
}
