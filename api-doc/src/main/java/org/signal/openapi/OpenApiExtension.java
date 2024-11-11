/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.openapi;

import com.fasterxml.jackson.annotation.JsonView;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.type.SimpleType;
import io.dropwizard.auth.Auth;
import io.swagger.v3.jaxrs2.ResolvedParameter;
import io.swagger.v3.jaxrs2.ext.AbstractOpenAPIExtension;
import io.swagger.v3.jaxrs2.ext.OpenAPIExtension;
import io.swagger.v3.oas.models.Components;
import jakarta.ws.rs.Consumes;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;

/**
 * One of the extension mechanisms of Swagger Core library (OpenAPI processor) is via custom implementations
 * of the {@link AbstractOpenAPIExtension} class.
 * <p/>
 * The purpose of this extension is to customize certain aspects of the OpenAPI model generation on a lower level.
 * This extension works in coordination with {@link OpenApiReader} that has access to the model on a higher level.
 * <p/>
 * The extension is enabled by being listed in {@code META-INF/services/io.swagger.v3.jaxrs2.ext.OpenAPIExtension} file.
 * @see ServiceLoader
 * @see OpenApiReader
 * @see <a href="https://github.com/swagger-api/swagger-core/wiki/Swagger-2.X---Extensions">Swagger 2.X Extensions</a>
 */
public class OpenApiExtension extends AbstractOpenAPIExtension {

  public static final ResolvedParameter AUTHENTICATED_ACCOUNT = new ResolvedParameter();

  public static final ResolvedParameter OPTIONAL_AUTHENTICATED_ACCOUNT = new ResolvedParameter();

  /**
   * When parsing endpoint methods, Swagger will treat the first parameter not annotated as header/path/query param
   * as a request body (and will ignore other not annotated parameters). In our case, this behavior conflicts with
   * the {@code @Auth}-annotated parameters. Here we're checking if parameters are known to be anything other than
   * a body and return an appropriate {@link ResolvedParameter} representation.
   */
  @Override
  public ResolvedParameter extractParameters(
      final List<Annotation> annotations,
      final Type type,
      final Set<Type> typesToSkip,
      final Components components,
      final Consumes classConsumes,
      final Consumes methodConsumes,
      final boolean includeRequestBody,
      final JsonView jsonViewAnnotation,
      final Iterator<OpenAPIExtension> chain) {

    if (annotations.stream().anyMatch(a -> a.annotationType().equals(Auth.class))) {
      // this is the case of authenticated endpoint,
      if (type instanceof SimpleType simpleType
          && simpleType.getRawClass().equals(AuthenticatedDevice.class)) {
        return AUTHENTICATED_ACCOUNT;
      }
      if (type instanceof SimpleType simpleType
          && isOptionalOfType(simpleType, AuthenticatedDevice.class)) {
        return OPTIONAL_AUTHENTICATED_ACCOUNT;
      }
    }

    return super.extractParameters(
        annotations,
        type,
        typesToSkip,
        components,
        classConsumes,
        methodConsumes,
        includeRequestBody,
        jsonViewAnnotation,
        chain);
  }

  private static boolean isOptionalOfType(final SimpleType simpleType, final Class<?> expectedType) {
    if (!simpleType.getRawClass().equals(Optional.class)) {
      return false;
    }
    final List<JavaType> typeParameters = simpleType.getBindings().getTypeParameters();
    if (typeParameters.isEmpty()) {
      return false;
    }
    return typeParameters.get(0) instanceof SimpleType optionalParameterType
        && optionalParameterType.getRawClass().equals(expectedType);
  }
}
