/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.websocket.auth;

import io.dropwizard.auth.Auth;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.WebApplicationException;
import java.lang.reflect.ParameterizedType;
import java.security.Principal;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.internal.inject.AbstractValueParamProvider;
import org.glassfish.jersey.server.internal.inject.MultivaluedParameterExtractorProvider;
import org.glassfish.jersey.server.model.Parameter;
import org.glassfish.jersey.server.spi.internal.ValueParamProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.websocket.WebSocketResourceProvider;

@Singleton
public class WebsocketAuthValueFactoryProvider<T extends Principal> extends AbstractValueParamProvider  {
  private static final Logger logger = LoggerFactory.getLogger(WebsocketAuthValueFactoryProvider.class);

  private final Class<T> principalClass;

  @Inject
  public WebsocketAuthValueFactoryProvider(MultivaluedParameterExtractorProvider mpep, WebsocketPrincipalClassProvider<T> principalClassProvider) {
    super(() -> mpep, Parameter.Source.UNKNOWN);
    this.principalClass = principalClassProvider.clazz;
  }

  @Nullable
  @Override
  protected Function<ContainerRequest, ?> createValueProvider(Parameter parameter) {
    if (!parameter.isAnnotationPresent(Auth.class)) {
      return null;
    }

    final boolean readOnly = true;

    if (parameter.getRawType() == Optional.class
        && ParameterizedType.class.isAssignableFrom(parameter.getType().getClass())
        && principalClass == ((ParameterizedType) parameter.getType()).getActualTypeArguments()[0]) {
      return this::createPrincipal;
    } else if (principalClass.equals(parameter.getRawType())) {
      return containerRequest ->
          createPrincipal(containerRequest)
              .orElseThrow(() -> new WebApplicationException("Authenticated resource", 401));
    } else {
      throw new IllegalStateException("Can't inject unassignable principal: " + principalClass + " for parameter: " + parameter);
    }
  }

  private Optional<? extends Principal> createPrincipal(final ContainerRequest request) {
    final Object obj = request.getProperty(WebSocketResourceProvider.REUSABLE_AUTH_PROPERTY);
    if (!(obj instanceof Optional<?>)) {
      logger.warn("Unexpected reusable auth property type {} : {}", obj.getClass(), obj);
      return Optional.empty();
    }

    //noinspection unchecked
    return (Optional<T>) obj;
  }

  @Singleton
  static class WebsocketPrincipalClassProvider<T extends Principal> {

    private final Class<T> clazz;

    WebsocketPrincipalClassProvider(Class<T> clazz) {
      this.clazz = clazz;
    }
  }

  /**
   * Injection binder for {@link io.dropwizard.auth.AuthValueFactoryProvider}.
   *
   * @param <T> the type of the principal
   */
  public static class Binder<T extends Principal> extends AbstractBinder {

    private final Class<T> principalClass;

    public Binder(Class<T> principalClass) {
      this.principalClass = principalClass;
    }

    @Override
    protected void configure() {
      bind(new WebsocketPrincipalClassProvider<>(principalClass)).to(WebsocketPrincipalClassProvider.class);
      bind(WebsocketAuthValueFactoryProvider.class).to(ValueParamProvider.class).in(Singleton.class);
    }
  }
}
