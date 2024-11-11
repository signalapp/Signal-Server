/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.openapi;

import static com.google.common.base.MoreObjects.firstNonNull;
import static org.signal.openapi.OpenApiExtension.AUTHENTICATED_ACCOUNT;
import static org.signal.openapi.OpenApiExtension.OPTIONAL_AUTHENTICATED_ACCOUNT;

import com.fasterxml.jackson.annotation.JsonView;
import com.google.common.collect.ImmutableList;
import io.swagger.v3.jaxrs2.Reader;
import io.swagger.v3.jaxrs2.ResolvedParameter;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import jakarta.ws.rs.Consumes;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;

/**
 * One of the extension mechanisms of Swagger Core library (OpenAPI processor) is via custom implementations
 * of the {@link Reader} class.
 * <p/>
 * The purpose of this extension is to customize certain aspects of the OpenAPI model generation on a higher level.
 * This extension works in coordination with {@link OpenApiExtension} that has access to the model on a lower level.
 * <p/>
 * The extension is enabled by being listed in {@code resources/openapi/openapi-configuration.yaml} file.
 * @see OpenApiExtension
 * @see <a href="https://github.com/swagger-api/swagger-core/wiki/Swagger-2.X---Extensions">Swagger 2.X Extensions</a>
 */
public class OpenApiReader extends Reader {

  private static final String AUTHENTICATED_ACCOUNT_AUTH_SCHEMA = "authenticatedAccount";


  /**
   * Overriding this method allows converting a resolved parameter into other operation entities,
   * in this case, into security requirements.
   */
  @Override
  protected ResolvedParameter getParameters(
      final Type type,
      final List<Annotation> annotations,
      final Operation operation,
      final Consumes classConsumes,
      final Consumes methodConsumes,
      final JsonView jsonViewAnnotation) {
    final ResolvedParameter resolved = super.getParameters(
        type, annotations, operation, classConsumes, methodConsumes, jsonViewAnnotation);

    if (resolved == AUTHENTICATED_ACCOUNT) {
      operation.setSecurity(ImmutableList.<SecurityRequirement>builder()
          .addAll(firstNonNull(operation.getSecurity(), Collections.emptyList()))
          .add(new SecurityRequirement().addList(AUTHENTICATED_ACCOUNT_AUTH_SCHEMA))
          .build());
    }
    if (resolved == OPTIONAL_AUTHENTICATED_ACCOUNT) {
      operation.setSecurity(ImmutableList.<SecurityRequirement>builder()
          .addAll(firstNonNull(operation.getSecurity(), Collections.emptyList()))
          .add(new SecurityRequirement().addList(AUTHENTICATED_ACCOUNT_AUTH_SCHEMA))
          .add(new SecurityRequirement())
          .build());
    }

    return resolved;
  }
}
