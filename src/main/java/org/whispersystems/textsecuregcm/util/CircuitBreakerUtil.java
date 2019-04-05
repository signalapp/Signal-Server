package org.whispersystems.textsecuregcm.util;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

import static com.codahale.metrics.MetricRegistry.name;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;

public class CircuitBreakerUtil {

  public static void registerMetrics(MetricRegistry metricRegistry, CircuitBreaker circuitBreaker, Class<?> clazz) {
    Meter successMeter     = metricRegistry.meter(name(clazz, circuitBreaker.getName(), "success"    ));
    Meter failureMeter     = metricRegistry.meter(name(clazz, circuitBreaker.getName(), "failure"    ));
    Meter unpermittedMeter = metricRegistry.meter(name(clazz, circuitBreaker.getName(), "unpermitted"));

    metricRegistry.gauge(name(clazz, circuitBreaker.getName(), "state"), () -> ()-> circuitBreaker.getState().getOrder());

    circuitBreaker.getEventPublisher().onSuccess(event -> successMeter.mark());
    circuitBreaker.getEventPublisher().onError(event -> failureMeter.mark());
    circuitBreaker.getEventPublisher().onCallNotPermitted(event -> unpermittedMeter.mark());
  }

}
