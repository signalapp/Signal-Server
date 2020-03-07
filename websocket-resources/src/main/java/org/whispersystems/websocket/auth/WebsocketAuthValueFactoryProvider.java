package org.whispersystems.websocket.auth;

import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.internal.inject.AbstractValueParamProvider;
import org.glassfish.jersey.server.internal.inject.MultivaluedParameterExtractorProvider;
import org.glassfish.jersey.server.model.Parameter;
import org.glassfish.jersey.server.spi.internal.ValueParamProvider;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.WebApplicationException;
import java.lang.reflect.ParameterizedType;
import java.security.Principal;
import java.util.Optional;
import java.util.function.Function;

import io.dropwizard.auth.Auth;

@Singleton
public class WebsocketAuthValueFactoryProvider<T extends Principal> extends AbstractValueParamProvider  {

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

    if (parameter.getRawType() == Optional.class                                 &&
        ParameterizedType.class.isAssignableFrom(parameter.getType().getClass()) &&
        principalClass == ((ParameterizedType)parameter.getType()).getActualTypeArguments()[0])
    {
      return request -> new OptionalContainerRequestValueFactory(request).provide();
    } else if (principalClass.equals(parameter.getRawType())) {
      return request -> new StandardContainerRequestValueFactory(request).provide();
    } else {
      throw new IllegalStateException("Can't inject unassignable principal: " + principalClass + " for parameter: " + parameter);
    }
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

  private static class StandardContainerRequestValueFactory {

    private final ContainerRequest request;

    public StandardContainerRequestValueFactory(ContainerRequest request) {
      this.request = request;
    }

    public Principal provide() {
      final Principal principal = request.getSecurityContext().getUserPrincipal();

      if (principal == null) {
        throw new WebApplicationException("Authenticated resource", 401);
      }

      return principal;
    }

  }

  private static class OptionalContainerRequestValueFactory {

    private final ContainerRequest request;

    public OptionalContainerRequestValueFactory(ContainerRequest request) {
      this.request = request;
    }

    public Optional<Principal> provide() {
      return Optional.ofNullable(request.getSecurityContext().getUserPrincipal());
    }
  }

}
