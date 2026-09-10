package org.whispersystems.textsecuregcm.storage.foundationdb;

import com.apple.foundationdb.Database;
import io.dropwizard.lifecycle.Managed;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;

public class FoundationDBWarmup implements Managed {

  private final Map<String, FaultTolerantDatabase> databasesByName;

  private static final Duration RETRY_DELAY = Duration.ofMillis(500);
  private static final int MAX_ATTEMPTS = 3;

  private static final Logger LOGGER = LoggerFactory.getLogger(FoundationDBWarmup.class);

  private static final Timer WARMUP_TIMER = Metrics.timer(MetricsUtil.name(FoundationDBWarmup.class, "warmupTimer"));

  private static final byte[] STATUS_JSON_KEY;

  static {
    final byte[] suffix = "/status/json".getBytes(StandardCharsets.US_ASCII);
    STATUS_JSON_KEY = new byte[suffix.length + 2];
    STATUS_JSON_KEY[0] = (byte) 0xff;
    STATUS_JSON_KEY[1] = (byte) 0xff;
    System.arraycopy(suffix, 0, STATUS_JSON_KEY, 2, suffix.length);
  }

  public FoundationDBWarmup(final Map<String, FaultTolerantDatabase> databasesByName) {
    this.databasesByName = databasesByName;
  }

  @Override
  public void start() throws Exception {
    databasesByName.forEach(this::readStatusKey);
  }

  /// Warm up the connection to a FoundationDB database by reading its status key. We retry a limited number of times so
  /// that we don't block the readiness probe in case a single shard is unavailable.
  ///
  /// @param databaseName the name of the database to read from
  /// @param database     the FoundationDB [Database] instance
  private void readStatusKey(final String databaseName, final FaultTolerantDatabase database) {
    int attempts = 0;
    final Timer.Sample sample = Timer.start();
    while (true) {
      try {
        database.readAsync(transaction -> transaction.get(STATUS_JSON_KEY), FaultTolerantDatabase.Context.READ_STATUS).join();
        sample.stop(WARMUP_TIMER);
        return;
      } catch (final Exception e) {
        attempts++;
        if (attempts == MAX_ATTEMPTS) {
          LOGGER.warn("Failed to read status key at startup for shard {}", databaseName, e);
          return;
        }
        try {
          Thread.sleep(RETRY_DELAY);
        } catch (final InterruptedException ex) {
          throw new RuntimeException(ex);
        }
      }
    }
  }

}
