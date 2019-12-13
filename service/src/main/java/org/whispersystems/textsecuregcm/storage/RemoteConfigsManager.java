package org.whispersystems.textsecuregcm.storage;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.Util;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.dropwizard.lifecycle.Managed;

public class RemoteConfigsManager implements Managed {

  private final Logger logger = LoggerFactory.getLogger(RemoteConfigsManager.class);

  private final RemoteConfigs remoteConfigs;
  private final long          sleepInterval;

  private AtomicReference<List<RemoteConfig>> cachedConfigs = new AtomicReference<>(new LinkedList<>());

  public RemoteConfigsManager(RemoteConfigs remoteConfigs) {
    this(remoteConfigs, TimeUnit.SECONDS.toMillis(10));
  }

  @VisibleForTesting
  public RemoteConfigsManager(RemoteConfigs remoteConfigs, long sleepInterval) {
    this.remoteConfigs = remoteConfigs;
    this.sleepInterval = sleepInterval;
  }

  @Override
  public void start() {
    this.cachedConfigs.set(remoteConfigs.getAll());

    new Thread(() -> {
      while (true) {
        try {
          this.cachedConfigs.set(remoteConfigs.getAll());
        } catch (Throwable t) {
          logger.warn("Error updating remote configs cache", t);
        }

        Util.sleep(sleepInterval);
      }
    }).start();
  }

  public List<RemoteConfig> getAll() {
    return cachedConfigs.get();
  }

  public void set(RemoteConfig config) {
    remoteConfigs.set(config);
  }

  public void delete(String name) {
    remoteConfigs.delete(name);
  }

  @Override
  public void stop() throws Exception {

  }
}
