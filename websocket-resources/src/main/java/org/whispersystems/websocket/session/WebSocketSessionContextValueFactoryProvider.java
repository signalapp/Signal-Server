package org.whispersystems.websocket.session;

import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.internal.inject.AbstractValueParamProvider;
import org.glassfish.jersey.server.internal.inject.MultivaluedParameterExtractorProvider;
import org.glassfish.jersey.server.model.Parameter;
import org.glassfish.jersey.server.spi.internal.ValueParamProvider;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.function.Function;


@Singleton
public class WebSocketSessionContextValueFactoryProvider extends AbstractValueParamProvider {

  @Inject
  public WebSocketSessionContextValueFactoryProvider(MultivaluedParameterExtractorProvider mpep) {
    super(() -> mpep, Parameter.Source.UNKNOWN);
  }

  @Nullable
  @Override
  protected Function<ContainerRequest, ?> createValueProvider(Parameter parameter) {
    if (!parameter.isAnnotationPresent(WebSocketSession.class)) {
      return null;
    } else if (WebSocketSessionContext.class.equals(parameter.getRawType())) {
      return request -> new WebSocketSessionContainerRequestValueFactory(request).provide();
    } else {
      throw new IllegalArgumentException("Can't inject custom type");
    }
  }

  public static class Binder extends AbstractBinder {

    public Binder() { }

    @Override
    protected void configure() {
      bind(WebSocketSessionContextValueFactoryProvider.class).to(ValueParamProvider.class).in(Singleton.class);
    }
  }
}