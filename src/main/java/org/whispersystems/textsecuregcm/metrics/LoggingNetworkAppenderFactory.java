package org.whispersystems.textsecuregcm.metrics;


import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.papertrailapp.logback.Syslog4jAppender;
import io.dropwizard.logging.AbstractAppenderFactory;
import io.dropwizard.logging.async.AsyncAppenderFactory;
import io.dropwizard.logging.filter.LevelFilterFactory;
import io.dropwizard.logging.layout.LayoutFactory;
import io.dropwizard.validation.PortRange;
import org.hibernate.validator.constraints.NotEmpty;
import org.productivity.java.syslog4j.impl.net.tcp.ssl.SSLTCPNetSyslogConfig;

import javax.validation.constraints.NotNull;
import java.util.TimeZone;

@JsonTypeName("papertrail")
public class LoggingNetworkAppenderFactory extends AbstractAppenderFactory<ILoggingEvent> {

  @JsonProperty
  @NotEmpty
  private String ident;

  @JsonProperty
  @NotEmpty
  private String host;

  @JsonProperty
  @PortRange(min = 1)
  @NotNull
  private Integer port;

  @JsonProperty
  private TimeZone timeZone = TimeZone.getTimeZone("UTC");

  @JsonProperty
  @NotEmpty
  private String facility = "USER";

  @JsonProperty
  private boolean sendLocalName = true;

  public String getIdent() {
    return ident;
  }

  public void setIdent(final String ident) {
    this.ident = ident;
  }

  public String getHost() {
    return host;
  }

  public void setHost(final String host) {
    this.host = host;
  }

  public Integer getPort() {
    return port;
  }

  public void setPort(final Integer port) {
    this.port = port;
  }

  public TimeZone getTimeZone() {
    return timeZone;
  }

  public void setTimeZone(final TimeZone timeZone) {
    this.timeZone = timeZone;
  }

  public String getFacility() {
    return facility;
  }

  public void setFacility(final String facility) {
    this.facility = facility;
  }

  public boolean isSendLocalName() {
    return sendLocalName;
  }

  public void setSendLocalName(final boolean sendLocalName) {
    this.sendLocalName = sendLocalName;
  }

  @Override
  public Appender<ILoggingEvent> build(LoggerContext context,
                                       String applicationName,
                                       LayoutFactory<ILoggingEvent> layoutFactory,
                                       LevelFilterFactory<ILoggingEvent> levelFilterFactory,
                                       AsyncAppenderFactory<ILoggingEvent> asyncAppenderFactory)
  {
    final Syslog4jAppender<ILoggingEvent> syslogAppender = new Syslog4jAppender<>();
    syslogAppender.setContext(context);
    syslogAppender.setLayout(buildLayout(context, layoutFactory));
    syslogAppender.setName("SYSLOG-TLS");
    final SSLTCPNetSyslogConfig syslogConfig = new SSLTCPNetSyslogConfig();

    syslogConfig.setIdent(ident);
    syslogConfig.setHost(host);
    syslogConfig.setPort(port);
    syslogConfig.setFacility(facility);
    syslogConfig.setSendLocalName(sendLocalName);

    syslogAppender.setSyslogConfig(syslogConfig);

    syslogAppender.addFilter(levelFilterFactory.build(threshold));
    syslogAppender.start();

    return syslogAppender;
  }
}