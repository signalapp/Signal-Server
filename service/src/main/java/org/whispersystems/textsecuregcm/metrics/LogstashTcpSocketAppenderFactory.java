/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.PatternLayout;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.encoder.LayoutWrappingEncoder;
import ch.qos.logback.core.net.ssl.SSLConfiguration;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dropwizard.logging.AbstractAppenderFactory;
import io.dropwizard.logging.async.AsyncAppenderFactory;
import io.dropwizard.logging.filter.LevelFilterFactory;
import io.dropwizard.logging.layout.LayoutFactory;
import java.time.Duration;
import javax.validation.constraints.NotEmpty;
import net.logstash.logback.appender.LogstashTcpSocketAppender;
import net.logstash.logback.encoder.LogstashEncoder;

@JsonTypeName("logstashtcpsocket")
public class LogstashTcpSocketAppenderFactory extends AbstractAppenderFactory<ILoggingEvent> {

  @NotEmpty
  private String destination;

  private Duration keepAlive = Duration.ofSeconds(20);

  @NotEmpty
  private String apiKey;

  @JsonProperty
  public String getDestination() {
    return destination;
  }

  @JsonProperty
  public Duration getKeepAlive() {
    return keepAlive;
  }

  @JsonProperty
  public String getApiKey() {
    return apiKey;
  }

  @Override
  public Appender<ILoggingEvent> build(
      final LoggerContext context,
      final String applicationName,
      final LayoutFactory<ILoggingEvent> layoutFactory,
      final LevelFilterFactory<ILoggingEvent> levelFilterFactory,
      final AsyncAppenderFactory<ILoggingEvent> asyncAppenderFactory) {

    final SSLConfiguration sslConfiguration = new SSLConfiguration();
    final LogstashTcpSocketAppender appender = new LogstashTcpSocketAppender();
    appender.setName("logstashtcpsocket-appender");
    appender.setContext(context);
    appender.setSsl(sslConfiguration);
    appender.addDestination(destination);
    appender.setKeepAliveDuration(new ch.qos.logback.core.util.Duration(keepAlive.toMillis()));

    final LogstashEncoder encoder = new LogstashEncoder();
    final LayoutWrappingEncoder<ILoggingEvent> prefix = new LayoutWrappingEncoder<>();
    final PatternLayout layout = new PatternLayout();
    layout.setPattern(String.format("%s ", apiKey));
    prefix.setLayout(layout);
    encoder.setPrefix(prefix);
    appender.setEncoder(encoder);

    appender.addFilter(levelFilterFactory.build(threshold));
    getFilterFactories().forEach(f -> appender.addFilter(f.build()));
    appender.start();

    return wrapAsync(appender, asyncAppenderFactory);
  }
}
