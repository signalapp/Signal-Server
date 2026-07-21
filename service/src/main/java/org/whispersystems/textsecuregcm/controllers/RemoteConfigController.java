/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import io.dropwizard.auth.Auth;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.headers.Header;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.EntityTag;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.HexFormat;
import java.util.Map;
import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.entities.RemoteConfigurationResponse;
import org.whispersystems.textsecuregcm.storage.RemoteConfigsManager;

@Path("/v2/config")
@Tag(name = "Remote Config")
public class RemoteConfigController {

  private final RemoteConfigsManager remoteConfigsManager;

  public RemoteConfigController(RemoteConfigsManager remoteConfigsManager) {
    this.remoteConfigsManager = remoteConfigsManager;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Fetch remote configuration",
      description = "Remote configuration is a list of namespaced keys that clients may use for consistent configuration or behavior. Configuration values change over time, and the list should be refreshed periodically, typically at client launch and every few hours thereafter. Some values depend on the authenticated user, so the list should be refreshed immediately if the user changes."
  )
  @ApiResponse(
      responseCode = "200",
      description = "Remote configuration values for the authenticated user",
      content = @Content(schema = @Schema(implementation = RemoteConfigurationResponse.class)),
      headers = @Header(name = "ETag", description = "A hash of the configuration content which can be supplied in an If-None-Match header on future requests"))
  @ApiResponse(responseCode = "304", description = "There is no change since the last fetch", content = {})
  @ApiResponse(responseCode = "401", description = "This request requires authentication", content = {})

  public Response getAll(
      @Auth AuthenticatedDevice auth,

      @Parameter(description = "The ETag header supplied with a previous response from this endpoint. Optional.")
      @HeaderParam(HttpHeaders.IF_NONE_MATCH)
      @Nullable EntityTag eTag,

      @Parameter(description = "The user agent in standard form.")
      @HeaderParam(HttpHeaders.USER_AGENT)
      String userAgent
  ) {

    final Map<String, String> configs = remoteConfigsManager.getConfigForAccount(auth.accountIdentifier(), userAgent);

    final EntityTag newETag = new EntityTag(HexFormat.of().toHexDigits(configs.hashCode()));
    if (newETag.equals(eTag)) {
      return Response.notModified(eTag).build();
    }

    return Response.ok(new RemoteConfigurationResponse(configs))
        .tag(newETag)
        .build();
  }

}
