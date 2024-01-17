/*
 * Copyright 2024 Signal Messenger, LLC
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
 * Parses a {@link SenderOverride} out of a {@link ContainerRequest} to provide to jersey resources.
 * <p>
 * A request filter may enrich a ContainerRequest with senderOverrides by providing a string property names defined in
 * {@link SenderOverride}. This indicates the desired senderOverride to use when sending verification codes.
 * <p>
 * A resource can consume a SenderOverride with by annotating a SenderOverride parameter with {@link Extract}
 */
public class SenderOverrideProvider implements ValueParamProvider {

  /**
   * Configures the SenderOverrideProvider
   */
  public static class SenderOverrideFeature implements Feature {

    @Override
    public boolean configure(FeatureContext context) {
      context.register(new AbstractBinder() {
        @Override
        protected void configure() {
          bind(SenderOverrideProvider.class)
              .to(ValueParamProvider.class)
              .in(Singleton.class);
        }
      });
      return true;
    }
  }

  @Override
  public Function<ContainerRequest, ?> getValueProvider(final Parameter parameter) {
    if (parameter.getRawType().equals(SenderOverride.class)
        && parameter.isAnnotationPresent(Extract.class)) {
      return SenderOverride::new;
    }
    return null;

  }

  @Override
  public PriorityType getPriority() {
    return Priority.HIGH;
  }
}
