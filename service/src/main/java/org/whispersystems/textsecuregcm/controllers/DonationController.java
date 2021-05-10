/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.auth.Auth;
import io.dropwizard.util.Strings;
import java.net.URI;
import java.net.http.HttpClient.Redirect;
import java.net.http.HttpClient.Version;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.DonationConfiguration;
import org.whispersystems.textsecuregcm.entities.ApplePayAuthorizationRequest;
import org.whispersystems.textsecuregcm.entities.ApplePayAuthorizationResponse;
import org.whispersystems.textsecuregcm.http.FaultTolerantHttpClient;
import org.whispersystems.textsecuregcm.http.FormDataBodyPublisher;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.util.SystemMapper;

@Path("/v1/donation")
public class DonationController {

  private final Logger logger = LoggerFactory.getLogger(DonationController.class);

  private final URI uri;
  private final String apiKey;
  private final Set<String> supportedCurrencies;
  private final FaultTolerantHttpClient httpClient;

  public DonationController(final Executor executor, final DonationConfiguration configuration) {
    this.uri = URI.create(configuration.getUri());
    this.apiKey = configuration.getApiKey();
    this.supportedCurrencies = configuration.getSupportedCurrencies();
    this.httpClient = FaultTolerantHttpClient.newBuilder()
        .withCircuitBreaker(configuration.getCircuitBreaker())
        .withRetry(configuration.getRetry())
        .withVersion(Version.HTTP_2)
        .withConnectTimeout(Duration.ofSeconds(10))
        .withRedirect(Redirect.NEVER)
        .withExecutor(executor)
        .withName("donation")
        .withSecurityProtocol(FaultTolerantHttpClient.SECURITY_PROTOCOL_TLS_1_3)
        .build();
  }

  @Timed
  @POST
  @Path("/authorize-apple-pay")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public CompletableFuture<Response> getApplePayAuthorization(@Auth Account account, @Valid ApplePayAuthorizationRequest request) {
    if (!supportedCurrencies.contains(request.getCurrency())) {
      return CompletableFuture.completedFuture(Response.status(422).build());
    }

    final HttpRequest httpRequest = HttpRequest.newBuilder()
        .uri(uri)
        .POST(FormDataBodyPublisher.of(Map.of(
            "amount", Long.toString(request.getAmount()),
            "currency", request.getCurrency())))
        .header("Authorization", "Basic " + Base64.getEncoder().encodeToString(
            (apiKey + ":").getBytes(StandardCharsets.UTF_8)))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .build();
    return httpClient.sendAsync(httpRequest, BodyHandlers.ofString())
        .thenApply(this::processApplePayAuthorizationRemoteResponse);
  }

  private Response processApplePayAuthorizationRemoteResponse(HttpResponse<String> response) {
    ObjectMapper mapper = SystemMapper.getMapper();

    if (response.statusCode() >= 200 && response.statusCode() < 300 &&
        MediaType.APPLICATION_JSON.equalsIgnoreCase(response.headers().firstValue("Content-Type").orElse(null))) {
      try {
        final JsonNode jsonResponse = mapper.readTree(response.body());
        final String id = jsonResponse.get("id").asText(null);
        final String clientSecret = jsonResponse.get("client_secret").asText(null);
        if (Strings.isNullOrEmpty(id) || Strings.isNullOrEmpty(clientSecret)) {
          logger.warn("missing fields in json response in donation controller");
          return Response.status(500).build();
        }
        final String responseJson = mapper.writeValueAsString(new ApplePayAuthorizationResponse(id, clientSecret));
        return Response.ok(responseJson, MediaType.APPLICATION_JSON_TYPE).build();
      } catch (JsonProcessingException e) {
        logger.warn("json processing error in donation controller", e);
        return Response.status(500).build();
      }
    } else {
      logger.warn("unexpected response code returned to donation controller");
      return Response.status(500).build();
    }
  }
}
