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
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinPool.ManagedBlocker;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialPresentation;
import org.signal.libsignal.zkgroup.receipts.ReceiptSerial;
import org.signal.libsignal.zkgroup.receipts.ServerZkReceiptOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.configuration.BadgesConfiguration;
import org.whispersystems.textsecuregcm.configuration.DonationConfiguration;
import org.whispersystems.textsecuregcm.configuration.StripeConfiguration;
import org.whispersystems.textsecuregcm.entities.ApplePayAuthorizationRequest;
import org.whispersystems.textsecuregcm.entities.ApplePayAuthorizationResponse;
import org.whispersystems.textsecuregcm.entities.RedeemReceiptRequest;
import org.whispersystems.textsecuregcm.http.FaultTolerantHttpClient;
import org.whispersystems.textsecuregcm.http.FormDataBodyPublisher;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountBadge;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.RedeemedReceiptsManager;
import org.whispersystems.textsecuregcm.util.SystemMapper;

@Path("/v1/donation")
public class DonationController {

  public interface ReceiptCredentialPresentationFactory {
    ReceiptCredentialPresentation build(byte[] bytes) throws InvalidInputException;
  }

  private static final Logger logger = LoggerFactory.getLogger(DonationController.class);

  private final Clock clock;
  private final ServerZkReceiptOperations serverZkReceiptOperations;
  private final RedeemedReceiptsManager redeemedReceiptsManager;
  private final AccountsManager accountsManager;
  private final BadgesConfiguration badgesConfiguration;
  private final ReceiptCredentialPresentationFactory receiptCredentialPresentationFactory;
  private final URI uri;
  private final String apiKey;
  private final String description;
  private final Set<String> supportedCurrencies;
  private final FaultTolerantHttpClient httpClient;

  public DonationController(
      @Nonnull final Clock clock,
      @Nonnull final ServerZkReceiptOperations serverZkReceiptOperations,
      @Nonnull final RedeemedReceiptsManager redeemedReceiptsManager,
      @Nonnull final AccountsManager accountsManager,
      @Nonnull final BadgesConfiguration badgesConfiguration,
      @Nonnull final ReceiptCredentialPresentationFactory receiptCredentialPresentationFactory,
      @Nonnull final Executor httpClientExecutor,
      @Nonnull final DonationConfiguration configuration,
      @Nonnull final StripeConfiguration stripeConfiguration) {
    this.clock = Objects.requireNonNull(clock);
    this.serverZkReceiptOperations = Objects.requireNonNull(serverZkReceiptOperations);
    this.redeemedReceiptsManager = Objects.requireNonNull(redeemedReceiptsManager);
    this.accountsManager = Objects.requireNonNull(accountsManager);
    this.badgesConfiguration = Objects.requireNonNull(badgesConfiguration);
    this.receiptCredentialPresentationFactory = Objects.requireNonNull(receiptCredentialPresentationFactory);
    this.uri = URI.create(configuration.getUri());
    this.apiKey = stripeConfiguration.getApiKey();
    this.description = configuration.getDescription();
    this.supportedCurrencies = configuration.getSupportedCurrencies();
    this.httpClient = FaultTolerantHttpClient.newBuilder()
        .withCircuitBreaker(configuration.getCircuitBreaker())
        .withRetry(configuration.getRetry())
        .withVersion(Version.HTTP_2)
        .withConnectTimeout(Duration.ofSeconds(10))
        .withRedirect(Redirect.NEVER)
        .withExecutor(Objects.requireNonNull(httpClientExecutor))
        .withName("donation")
        .withSecurityProtocol(FaultTolerantHttpClient.SECURITY_PROTOCOL_TLS_1_3)
        .build();
  }

  @Timed
  @POST
  @Path("/redeem-receipt")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
  public CompletionStage<Response> redeemReceipt(
      @Auth final AuthenticatedAccount auth,
      @NotNull @Valid final RedeemReceiptRequest request) {
    return CompletableFuture.supplyAsync(() -> {
      ReceiptCredentialPresentation receiptCredentialPresentation;
      try {
        receiptCredentialPresentation = receiptCredentialPresentationFactory.build(
            request.getReceiptCredentialPresentation());
      } catch (InvalidInputException e) {
        return CompletableFuture.completedFuture(Response.status(Status.BAD_REQUEST).entity("invalid receipt credential presentation").type(MediaType.TEXT_PLAIN_TYPE).build());
      }
      try {
        serverZkReceiptOperations.verifyReceiptCredentialPresentation(receiptCredentialPresentation);
      } catch (VerificationFailedException e) {
        return CompletableFuture.completedFuture(Response.status(Status.BAD_REQUEST).entity("receipt credential presentation verification failed").type(MediaType.TEXT_PLAIN_TYPE).build());
      }

      final ReceiptSerial receiptSerial = receiptCredentialPresentation.getReceiptSerial();
      final Instant receiptExpiration = Instant.ofEpochSecond(receiptCredentialPresentation.getReceiptExpirationTime());
      final long receiptLevel = receiptCredentialPresentation.getReceiptLevel();
      final String badgeId = badgesConfiguration.getReceiptLevels().get(receiptLevel);
      if (badgeId == null) {
        return CompletableFuture.completedFuture(Response.serverError().entity("server does not recognize the requested receipt level").type(MediaType.TEXT_PLAIN_TYPE).build());
      }
      final CompletionStage<Boolean> putStage = redeemedReceiptsManager.put(
          receiptSerial, receiptExpiration.getEpochSecond(), receiptLevel, auth.getAccount().getUuid());
      return putStage.thenApplyAsync(receiptMatched -> {
        if (!receiptMatched) {
          return Response.status(Status.BAD_REQUEST).entity("receipt serial is already redeemed").type(MediaType.TEXT_PLAIN_TYPE).build();
        }

        try {
          ForkJoinPool.managedBlock(new ManagedBlocker() {
            boolean done = false;

            @Override
            public boolean block() {
              final Optional<Account> optionalAccount = accountsManager.getByAccountIdentifier(auth.getAccount().getUuid());
              optionalAccount.ifPresent(account -> {
                accountsManager.update(account, a -> {
                  a.addBadge(clock, new AccountBadge(badgeId, receiptExpiration, request.isVisible()));
                  if (request.isPrimary()) {
                    a.makeBadgePrimaryIfExists(clock, badgeId);
                  }
                });
              });
              done = true;
              return true;
            }

            @Override
            public boolean isReleasable() {
              return done;
            }
          });
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return Response.serverError().build();
        }

        return Response.ok().build();
      });
    }).thenCompose(Function.identity());
  }

  @Timed
  @POST
  @Path("/authorize-apple-pay")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public CompletableFuture<Response> getApplePayAuthorization(@Auth AuthenticatedAccount auth, @NotNull @Valid ApplePayAuthorizationRequest request) {
    if (!supportedCurrencies.contains(request.getCurrency())) {
      return CompletableFuture.completedFuture(Response.status(422).build());
    }

    final Map<String, String> formData = new HashMap<>();
    formData.put("amount", Long.toString(request.getAmount()));
    formData.put("currency", request.getCurrency());
    if (!Strings.isNullOrEmpty(description)) {
      formData.put("description", description);
    }
    final HttpRequest httpRequest = HttpRequest.newBuilder()
        .uri(uri)
        .POST(FormDataBodyPublisher.of(formData))
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
