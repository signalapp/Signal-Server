package org.whispersystems.websocket.auth.internal;

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
import javax.ws.rs.WebApplicationException;
import java.security.Principal;
import java.util.Optional;

import io.dropwizard.auth.Auth;

@Singleton
public class WebSocketAuthValueFactoryProvider extends AbstractValueFactoryProvider {

  @Inject
  public WebSocketAuthValueFactoryProvider(MultivaluedParameterExtractorProvider mpep,
                                           ServiceLocator injector)
  {
    super(mpep, injector, Parameter.Source.UNKNOWN);
  }

  @Override
  public AbstractContainerRequestValueFactory<?> createValueFactory(final Parameter parameter) {
    if (parameter.getAnnotation(Auth.class) == null) {
      return null;
    }

    if (parameter.getRawType() == Optional.class) {
      return new OptionalContainerRequestValueFactory(parameter);
    } else {
      return new StandardContainerRequestValueFactory(parameter);
    }
  }

  private static class OptionalContainerRequestValueFactory extends AbstractContainerRequestValueFactory {
    private final Parameter parameter;

    private OptionalContainerRequestValueFactory(Parameter parameter) {
      this.parameter = parameter;
    }

    @Override
    public Object provide() {
      Principal principal = getContainerRequest().getSecurityContext().getUserPrincipal();

      if (principal != null && !(principal instanceof WebSocketServletRequest.ContextPrincipal)) {
        throw new IllegalArgumentException("Can't inject non-ContextPrincipal into request");
      }

      if (principal == null) return Optional.empty();
      else                   return Optional.ofNullable(((WebSocketServletRequest.ContextPrincipal)principal).getContext().getAuthenticated());

    }
  }

  private static class StandardContainerRequestValueFactory extends AbstractContainerRequestValueFactory {
    private final Parameter parameter;

    private StandardContainerRequestValueFactory(Parameter parameter) {
      this.parameter = parameter;
    }

    @Override
    public Object provide() {
      Principal principal = getContainerRequest().getSecurityContext().getUserPrincipal();

      if (principal == null) {
        throw new IllegalStateException("Cannot inject a custom principal into unauthenticated request");
      }

      if (!(principal instanceof WebSocketServletRequest.ContextPrincipal)) {
        throw new IllegalArgumentException("Cannot inject a non-WebSocket AuthPrincipal into request");
      }

      Object authenticated = ((WebSocketServletRequest.ContextPrincipal)principal).getContext().getAuthenticated();

      if (authenticated == null) {
        throw new WebApplicationException("Authenticated resource", 401);
      }

      if (!parameter.getRawType().isAssignableFrom(authenticated.getClass())) {
        throw new IllegalArgumentException("Authenticated principal is of the wrong type: " + authenticated.getClass() + " looking for: " + parameter.getRawType());
      }

      return parameter.getRawType().cast(authenticated);
    }
  }

  @Singleton
  private static class AuthInjectionResolver extends ParamInjectionResolver<Auth> {
    public AuthInjectionResolver() {
      super(WebSocketAuthValueFactoryProvider.class);
    }
  }

  public static class Binder extends AbstractBinder {


    public Binder() {
    }

    @Override
    protected void configure() {
      bind(WebSocketAuthValueFactoryProvider.class).to(ValueFactoryProvider.class).in(Singleton.class);
      bind(AuthInjectionResolver.class).to(new TypeLiteral<InjectionResolver<Auth>>() {
      }).in(Singleton.class);
    }
  }
}