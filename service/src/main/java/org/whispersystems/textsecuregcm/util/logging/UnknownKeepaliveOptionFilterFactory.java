package org.whispersystems.textsecuregcm.util.logging;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dropwizard.logging.common.filter.FilterFactory;

@JsonTypeName("unknownKeepaliveOption")
public class UnknownKeepaliveOptionFilterFactory implements FilterFactory<ILoggingEvent> {

  @Override
  public Filter<ILoggingEvent> build() {
    return new UnknownKeepaliveOptionFilter();
  }
}
