package org.whispersystems.textsecuregcm.spam;

import java.util.function.Function;
import javax.inject.Singleton;
import javax.ws.rs.core.Feature;
import javax.ws.rs.core.FeatureContext;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.model.Parameter;
import org.glassfish.jersey.server.spi.internal.ValueParamProvider;

/**
 * Parses a {@link ScoreThreshold} out of a {@link ContainerRequest} to provide to jersey resources.
 *
 * A request filter may enrich a ContainerRequest with a scoreThreshold by providing a float property with the name
 * {@link ScoreThreshold#PROPERTY_NAME}. This indicates the desired scoreThreshold to use when evaluating whether a
 * request should proceed.
 *
 * A resource can consume a ScoreThreshold with by annotating a ScoreThreshold parameter with {@link Extract}
 */
public class ScoreThresholdProvider implements ValueParamProvider {

  /**
   * Configures the ScoreThresholdProvider
   */
  public static class ScoreThresholdFeature implements Feature {
    @Override
    public boolean configure(FeatureContext context) {
      context.register(new AbstractBinder() {
        @Override
        protected void configure() {
          bind(ScoreThresholdProvider.class)
              .to(ValueParamProvider.class)
              .in(Singleton.class);
        }
      });
      return true;
    }
  }

  @Override
  public Function<ContainerRequest, ?> getValueProvider(final Parameter parameter) {
    if (parameter.getRawType().equals(ScoreThreshold.class)
        && parameter.isAnnotationPresent(Extract.class)) {
      return ScoreThreshold::new;
    }
    return null;

  }

  @Override
  public PriorityType getPriority() {
    return Priority.HIGH;
  }
}
