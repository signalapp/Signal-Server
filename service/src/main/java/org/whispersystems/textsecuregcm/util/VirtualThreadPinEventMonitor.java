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
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordedFrame;
import jdk.jfr.consumer.RecordedStackTrace;
import jdk.jfr.consumer.RecordedThread;
import jdk.jfr.consumer.RecordingStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;

/**
 * Watches for JFR events indicating that a virtual thread was pinned
 */
public class VirtualThreadPinEventMonitor implements Managed {

  private static final Logger logger = LoggerFactory.getLogger(VirtualThreadPinEventMonitor.class);
  private static final String PIN_COUNTER_NAME = MetricsUtil.name(VirtualThreadPinEventMonitor.class,
      "virtualThreadPinned");
  private static final String JFR_THREAD_PINNED_EVENT_NAME = "jdk.VirtualThreadPinned";
  private static final long MAX_JFR_REPOSITORY_SIZE = 1024 * 1024 * 4L; // 4MiB

  private final ExecutorService executorService;
  private final Duration pinEventThreshold;
  private final RecordingStream recordingStream;

  private final Consumer<RecordedEvent> pinEventConsumer;

  @VisibleForTesting
  VirtualThreadPinEventMonitor(
      final ExecutorService executorService,
      final Duration pinEventThreshold,
      final Consumer<RecordedEvent> pinEventConsumer) {
    this.executorService = executorService;
    this.pinEventThreshold = pinEventThreshold;
    this.pinEventConsumer = pinEventConsumer;
    this.recordingStream = new RecordingStream();
  }
  public VirtualThreadPinEventMonitor(
      final ExecutorService executorService,
      final Duration pinEventThreshold) {
    this(executorService, pinEventThreshold, VirtualThreadPinEventMonitor::processPinEvent);
  }

  @Override
  public void start() {
    recordingStream.setMaxSize(MAX_JFR_REPOSITORY_SIZE);
    recordingStream.enable(JFR_THREAD_PINNED_EVENT_NAME).withThreshold(pinEventThreshold).withStackTrace();
    recordingStream.onEvent(JFR_THREAD_PINNED_EVENT_NAME, pinEventConsumer);
    executorService.submit(recordingStream::start);
  }

  @Override
  public void stop() throws InterruptedException {
    // flushes events and waits for callbacks to finish
    recordingStream.stop();
    // immediately frees all resources
    recordingStream.close();
  }

  private static void processPinEvent(final RecordedEvent event) {
    logger.info("Long virtual thread pin event detected {}", prettyEventString(event));
    Metrics.counter(PIN_COUNTER_NAME).increment();
  }

  private static String prettyEventString(final RecordedEvent event) {
    // event.toString() hard codes a stack depth of 5, which is not enough to
    // determine the source of the event in most cases

    return """
        %s {
          startTime = %s
          duration = %s
          eventThread = %s
          stackTrace = %s
        }""".formatted(event.getEventType().getName(),
        event.getStartTime(),
        event.getDuration(),
        prettyThreadString(event.getThread()),
        prettyStackTraceString(event.getStackTrace(), "  "));
  }

  private static String prettyStackTraceString(final RecordedStackTrace st, final String indent) {
    if (st == null) {
      return "n/a";
    }
    // No need to put a limit, by default JFR stack traces are limited to 64 frames. They can be increased at jvm start
    // with the FlightRecorderOptions stackdepth option
    return "[\n" + indent + indent + st.getFrames().stream()
        .filter(RecordedFrame::isJavaFrame)
        .map(frame -> "%s.%s:%s".formatted(frame.getMethod().getType().getName(), frame.getMethod().getName(), frame.getLineNumber()))
        .collect(Collectors.joining(",\n" + indent + indent))
        + "\n" + indent + "]";
  }

  private static String prettyThreadString(final RecordedThread thread) {
    if (thread == null) {
      return "n/a";
    }
    return "%s (javaThreadId = %s)".formatted(thread.getJavaName(), thread.getJavaThreadId()) ;
  }

}
