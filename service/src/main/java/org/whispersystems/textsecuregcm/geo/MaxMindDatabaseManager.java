/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.geo;

import com.maxmind.db.CHMCache;
import com.maxmind.geoip2.DatabaseReader;
import io.dropwizard.lifecycle.Managed;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.S3ObjectMonitorFactory;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.s3.S3ObjectMonitor;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

public class MaxMindDatabaseManager implements Supplier<DatabaseReader>, Managed {

  private final S3ObjectMonitor databaseMonitor;

  private final AtomicReference<DatabaseReader> databaseReader = new AtomicReference<>();

  private final String databaseTag;

  private final Timer refreshTimer;

  private static final Logger log = LoggerFactory.getLogger(MaxMindDatabaseManager.class);

  public MaxMindDatabaseManager(final ScheduledExecutorService executorService,
      final AwsCredentialsProvider awsCredentialsProvider, final S3ObjectMonitorFactory configuration,
      final String databaseTag) {

    this.databaseMonitor = configuration.build(awsCredentialsProvider, executorService);
    this.databaseTag = databaseTag;
    this.refreshTimer = Metrics.timer(MetricsUtil.name(MaxMindDatabaseManager.class, "refresh"), "db", databaseTag);
  }

  private void handleDatabaseChanged(final InputStream inputStream) {
    refreshTimer.record(() -> {
      boolean foundDatabaseEntry = false;

      try (final InputStream bufferedInputStream = new BufferedInputStream(inputStream);
          final GzipCompressorInputStream gzipInputStream = new GzipCompressorInputStream(bufferedInputStream);
          final TarArchiveInputStream tarInputStream = new TarArchiveInputStream(gzipInputStream)) {

        ArchiveEntry nextEntry;

        while ((nextEntry = tarInputStream.getNextEntry()) != null) {
          if (nextEntry.getName().toLowerCase().endsWith(".mmdb")) {
            foundDatabaseEntry = true;

            final DatabaseReader oldReader = databaseReader.getAndSet(
                new DatabaseReader.Builder(tarInputStream).withCache(new CHMCache()).build()
            );
            if (oldReader != null) {
              oldReader.close();
            }
            break;
          }
        }
      } catch (final IOException e) {
        log.error(String.format("Failed to load MaxMind database, tag %s", databaseTag));
      }

      if (!foundDatabaseEntry) {
        log.warn(String.format("No .mmdb entry loaded from input stream, tag %s", databaseTag));
      }
    });
  }

  @Override
  public void start() throws Exception {
    databaseMonitor.start(this::handleDatabaseChanged);
  }

  @Override
  public void stop() throws Exception {
    databaseMonitor.stop();

    final DatabaseReader reader = databaseReader.getAndSet(null);
    if (reader != null) {
      reader.close();
    }
  }

  @Override
  public DatabaseReader get() {
    return this.databaseReader.get();
  }
}
