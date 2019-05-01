package org.whispersystems.websocket.session;

import org.glassfish.hk2.api.InjectionResolver;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.hk2.api.TypeLiteral;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.internal.inject.AbstractContainerRequestValueFactory;
import org.glassfish.jersey.server.internal.inject.AbstractValueFactoryProvider;
import org.glassfish.jersey.server.internal.inject.MultivaluedParameterExtractorProvider;
import org.glassfish.jersey.server.internal.inject.ParamInjectionResolver;
import org.glassfish.jersey.server.model.Parameter;
import org.glassfish.jersey.server.spi.internal.ValueFactoryProvider;
import org.whispersystems.websocket.servlet.WebSocketServletRequest;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.security.Principal;


@Singleton
public class WebSocketSessionContextValueFactoryProvider extends AbstractValueFactoryProvider {

  @Inject
  public WebSocketSessionContextValueFactoryProvider(MultivaluedParameterExtractorProvider mpep,
                                                     ServiceLocator injector)
  {
    super(mpep, injector, Parameter.Source.UNKNOWN);
  }

  @Override
  public AbstractContainerRequestValueFactory<WebSocketSessionContext> createValueFactory(Parameter parameter) {
    if (parameter.getAnnotation(WebSocketSession.class) == null) {
      return null;
    }

    return new AbstractContainerRequestValueFactory<WebSocketSessionContext>() {

      public WebSocketSessionContext provide() {
        Principal principal = getContainerRequest().getSecurityContext().getUserPrincipal();

        if (principal == null) {
          throw new IllegalStateException("Cannot inject a custom principal into unauthenticated request");
        }

        if (!(principal instanceof WebSocketServletRequest.ContextPrincipal)) {
          throw new IllegalArgumentException("Cannot inject a non-WebSocket AuthPrincipal into request");
        }

        return ((WebSocketServletRequest.ContextPrincipal)principal).getContext();
      }
    };
  }

  @Singleton
  private static class WebSocketSessionInjectionResolver extends ParamInjectionResolver<WebSocketSession> {
    public WebSocketSessionInjectionResolver() {
      super(WebSocketSessionContextValueFactoryProvider.class);
    }
  }

  public static class Binder extends AbstractBinder {

    public Binder() {
    }

    @Override
    protected void configure() {
      bind(WebSocketSessionContextValueFactoryProvider.class).to(ValueFactoryProvider.class).in(Singleton.class);
      bind(WebSocketSessionInjectionResolver.class).to(new TypeLiteral<InjectionResolver<WebSocketSession>>() {
      }).in(Singleton.class);
    }
  }
}