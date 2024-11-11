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
import ch.qos.logback.core.helpers.NOPAppender;
import ch.qos.logback.core.net.ssl.SSLConfiguration;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.dropwizard.logging.common.AbstractAppenderFactory;
import io.dropwizard.logging.common.async.AsyncAppenderFactory;
import io.dropwizard.logging.common.filter.LevelFilterFactory;
import io.dropwizard.logging.common.layout.LayoutFactory;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import java.util.Optional;
import net.logstash.logback.appender.LogstashTcpSocketAppender;
import net.logstash.logback.encoder.LogstashEncoder;
import org.whispersystems.textsecuregcm.WhisperServerVersion;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretString;
import org.whispersystems.textsecuregcm.util.HostnameUtil;

@JsonTypeName("logstashtcpsocket")
public class LogstashTcpSocketAppenderFactory extends AbstractAppenderFactory<ILoggingEvent> {

  @JsonProperty
  private String destination;

  @JsonProperty
  private Duration keepAlive = Duration.ofSeconds(20);

  @JsonProperty
  @NotNull
  private SecretString apiKey;

  @JsonProperty
  private String environment;

  @JsonProperty
  @NotEmpty
  public String getDestination() {
    return destination;
  }

  @JsonProperty
  public Duration getKeepAlive() {
    return keepAlive;
  }

  @JsonProperty
  public SecretString getApiKey() {
    return apiKey;
  }

  @JsonProperty
  @NotEmpty
  public String getEnvironment() {
    return environment;
  }

  @Override
  public Appender<ILoggingEvent> build(
      final LoggerContext context,
      final String applicationName,
      final LayoutFactory<ILoggingEvent> layoutFactory,
      final LevelFilterFactory<ILoggingEvent> levelFilterFactory,
      final AsyncAppenderFactory<ILoggingEvent> asyncAppenderFactory) {

    final boolean disableLogstashTcpSocketAppender = Optional.ofNullable(
            System.getenv("SIGNAL_DISABLE_LOGSTASH_TCP_SOCKET_APPENDER"))
        .isPresent();

    if (disableLogstashTcpSocketAppender) {
      return new NOPAppender<>();
    }

    final SSLConfiguration sslConfiguration = new SSLConfiguration();
    final LogstashTcpSocketAppender appender = new LogstashTcpSocketAppender();
    appender.setName("logstashtcpsocket-appender");
    appender.setContext(context);
    appender.setSsl(sslConfiguration);
    appender.addDestination(destination);
    appender.setKeepAliveDuration(new ch.qos.logback.core.util.Duration(keepAlive.toMillis()));

    final LogstashEncoder encoder = new LogstashEncoder();
    final ObjectNode customFieldsNode = new ObjectNode(JsonNodeFactory.instance);
    customFieldsNode.set("host", TextNode.valueOf(HostnameUtil.getLocalHostname()));
    customFieldsNode.set("service", TextNode.valueOf("chat"));
    customFieldsNode.set("ddsource", TextNode.valueOf("logstash"));
    customFieldsNode.set("ddtags", TextNode.valueOf("env:" + environment + ",version:" + WhisperServerVersion.getServerVersion()));

    encoder.setCustomFields(customFieldsNode.toString());
    final LayoutWrappingEncoder<ILoggingEvent> prefix = new LayoutWrappingEncoder<>();
    final PatternLayout layout = new PatternLayout();
    layout.setPattern(String.format("%s ", apiKey.value()));
    prefix.setLayout(layout);
    encoder.setPrefix(prefix);
    appender.setEncoder(encoder);

    appender.addFilter(levelFilterFactory.build(threshold));
    getFilterFactories().forEach(f -> appender.addFilter(f.build()));
    appender.start();

    return wrapAsync(appender, asyncAppenderFactory);
  }
}
