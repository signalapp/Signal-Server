/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dropwizard.logging.common.AbstractAppenderFactory;
import io.dropwizard.logging.common.async.AsyncAppenderFactory;
import io.dropwizard.logging.common.filter.LevelFilterFactory;
import io.dropwizard.logging.common.layout.LayoutFactory;
import io.opentelemetry.instrumentation.logback.appender.v1_0.OpenTelemetryAppender;
import jakarta.validation.constraints.NotEmpty;

@JsonTypeName("otlp")
public class OpenTelemetryAppenderFactory extends AbstractAppenderFactory<ILoggingEvent> {

  @JsonProperty
  private String destination;

  @Override
  public Appender<ILoggingEvent> build(
      final LoggerContext context,
      final String applicationName,
      final LayoutFactory<ILoggingEvent> layoutFactory,
      final LevelFilterFactory<ILoggingEvent> levelFilterFactory,
      final AsyncAppenderFactory<ILoggingEvent> asyncAppenderFactory) {

    final OpenTelemetryAppender appender = new OpenTelemetryAppender();
    appender.setCaptureCodeAttributes(true);
    appender.setCaptureLoggerContext(true);

    // The installation of an OpenTelemetry configuration happens in
    // WhisperServerService (or CommandDependencies), in order to let us tie
    // into Dropwizard's lifecycle management; this allows us to buffer any
    // logs emitted between now and that happening.
    appender.setNumLogsCapturedBeforeOtelInstall(1000);

    appender.addFilter(levelFilterFactory.build(threshold));
    getFilterFactories().forEach(f -> appender.addFilter(f.build()));
    appender.start();

    return wrapAsync(appender, asyncAppenderFactory);
  }
}
