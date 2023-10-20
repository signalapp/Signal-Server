/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

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
 * Parses a {@link PushChallengeConfig} out of a {@link ContainerRequest} to provide to jersey resources.
 *
 * A request filter may enrich a ContainerRequest with a PushChallengeConfig by providing a float
 * property with the name {@link PushChallengeConfig#PROPERTY_NAME}. This indicates whether push
 * challenges may be considered when evaluating whether a request should proceed.
 *
 * A resource can consume a PushChallengeConfig with by annotating a PushChallengeConfig parameter with {@link Extract}
 */
public class PushChallengeConfigProvider implements ValueParamProvider {

  /**
   * Configures the PushChallengeConfigProvider
   */
  public static class PushChallengeConfigFeature implements Feature {
    @Override
    public boolean configure(FeatureContext context) {
      context.register(new AbstractBinder() {
        @Override
        protected void configure() {
          bind(PushChallengeConfigProvider.class)
              .to(ValueParamProvider.class)
              .in(Singleton.class);
        }
      });
      return true;
    }
  }

  @Override
  public Function<ContainerRequest, ?> getValueProvider(final Parameter parameter) {
    if (parameter.getRawType().equals(PushChallengeConfig.class)
        && parameter.isAnnotationPresent(Extract.class)) {
      return PushChallengeConfig::new;
    }
    return null;

  }

  @Override
  public PriorityType getPriority() {
    return Priority.HIGH;
  }
}
