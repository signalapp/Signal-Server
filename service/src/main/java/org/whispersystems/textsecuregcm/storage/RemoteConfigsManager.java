/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.Util;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class RemoteConfigsManager implements Managed {

  private final Logger logger = LoggerFactory.getLogger(RemoteConfigsManager.class);

  private final RemoteConfigs remoteConfigs;

  private final AtomicReference<List<RemoteConfig>> cachedConfigs = new AtomicReference<>(new LinkedList<>());

  private final Thread refreshThread;

  public RemoteConfigsManager(RemoteConfigs remoteConfigs) {
    this(remoteConfigs, TimeUnit.SECONDS.toMillis(10));
  }

  @VisibleForTesting
  public RemoteConfigsManager(RemoteConfigs remoteConfigs, long sleepInterval) {
    this.remoteConfigs = remoteConfigs;
    this.refreshThread = new Thread(() -> {
        while (!Thread.currentThread().isInterrupted()) {
          try {
            refreshCache();
          } catch (Throwable t) {
            logger.warn("Error updating remote configs cache", t);
          }

          Util.sleepQuietly(sleepInterval);
        }
      });
    this.refreshThread.setName(getClass().getSimpleName());
    this.refreshThread.setDaemon(true);
  }

  @Override
  public void start() {
    refreshCache();

    refreshThread.start();
  }

  private void refreshCache() {
    this.cachedConfigs.set(remoteConfigs.getAll());

    synchronized (this.cachedConfigs) {
      this.cachedConfigs.notifyAll();
    }
  }

  @VisibleForTesting
  void waitForCacheRefresh() throws InterruptedException {
    synchronized (this.cachedConfigs) {
      this.cachedConfigs.wait();
    }
  }

  @VisibleForTesting
  Thread getRefreshThread() {
    return refreshThread;
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
  public void stop() {
    refreshThread.interrupt();
  }
}
