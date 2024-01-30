/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.lifecycle.Managed;
import io.micrometer.core.instrument.Metrics;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordedFrame;
import jdk.jfr.consumer.RecordingStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;

/**
 * Watches for JFR events indicating that a virtual thread was pinned
 */
public class VirtualThreadPinEventMonitor implements Managed {

  private static final Logger logger = LoggerFactory.getLogger(VirtualThreadPinEventMonitor.class);
  private static final String PIN_COUNTER_NAME = MetricsUtil.name(VirtualThreadPinEventMonitor.class, "virtualThreadPinned");
  private static final String JFR_THREAD_PINNED_EVENT_NAME = "jdk.VirtualThreadPinned";
  private static final long MAX_JFR_REPOSITORY_SIZE = 1024 * 1024 * 4L; // 4MiB

  private final ExecutorService executorService;
  private final Supplier<Set<String>> allowList;
  private final Duration pinEventThreshold;
  private final RecordingStream recordingStream;

  private BiConsumer<RecordedEvent, Boolean> pinEventConsumer;

  @VisibleForTesting
  VirtualThreadPinEventMonitor(
      final ExecutorService executorService,
      final Supplier<Set<String>> allowList,
      final Duration pinEventThreshold,
      final BiConsumer<RecordedEvent, Boolean> pinEventConsumer) {
    this.executorService = executorService;
    this.allowList = allowList;
    this.pinEventThreshold = pinEventThreshold;
    this.pinEventConsumer = pinEventConsumer;
    this.recordingStream = new RecordingStream();
  }
  public VirtualThreadPinEventMonitor(
      final ExecutorService executorService,
      final Supplier<Set<String>> allowList,
      final Duration pinEventThreshold) {
    this(executorService, allowList, pinEventThreshold, VirtualThreadPinEventMonitor::processPinEvent);
  }

  @Override
  public void start() {
    recordingStream.setMaxSize(MAX_JFR_REPOSITORY_SIZE);
    recordingStream.enable(JFR_THREAD_PINNED_EVENT_NAME).withThreshold(pinEventThreshold).withStackTrace();
    recordingStream.onEvent(event -> pinEventConsumer.accept(event, allowed(event)));
    executorService.submit(() -> recordingStream.start());
  }

  @Override
  public void stop() throws InterruptedException {
    // flushes events and waits for callbacks to finish
    recordingStream.stop();
    // immediately frees all resources
    recordingStream.close();
  }

  private static void processPinEvent(final RecordedEvent event, final boolean allowedPinEvent) {
    if (allowedPinEvent) {
      logger.info("Long allowed virtual thread pin event detected", event);
    } else {
      logger.error("Long forbidden virtual thread pin event detected", event);
    }
    Metrics.counter(PIN_COUNTER_NAME, "allowed", String.valueOf(allowedPinEvent)).increment();
  }

  private boolean allowed(final RecordedEvent event) {
    final Set<String> allowedMethodFrames = allowList.get();
    for (RecordedFrame st : event.getStackTrace().getFrames()) {
      if (!st.isJavaFrame()) {
        continue;
      }
      final String qualifiedName = "%s.%s".formatted(st.getMethod().getType().getName(), st.getMethod().getName());
      if (allowedMethodFrames.stream().anyMatch(qualifiedName::contains)) {
        return true;
      }
    }
    return false;
  }
}
