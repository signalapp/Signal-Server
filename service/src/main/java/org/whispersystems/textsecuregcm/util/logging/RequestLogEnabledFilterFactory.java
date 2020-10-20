package org.whispersystems.textsecuregcm.util.logging;

import ch.qos.logback.access.spi.IAccessEvent;
import ch.qos.logback.core.filter.Filter;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dropwizard.logging.filter.FilterFactory;

@JsonTypeName("requestLogEnabled")
class RequestLogEnabledFilterFactory implements FilterFactory<IAccessEvent> {

    @Override
    public Filter<IAccessEvent> build() {
        return RequestLogManager.getHttpRequestLogFilter();
    }
}
