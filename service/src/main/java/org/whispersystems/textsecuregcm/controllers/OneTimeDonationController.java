/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.controllers;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.net.HttpHeaders;
import io.dropwizard.auth.Auth;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.StringToClassMapItem;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialRequest;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialResponse;
import org.signal.libsignal.zkgroup.receipts.ServerZkReceiptOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.configuration.OneTimeDonationConfiguration;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.storage.IssuedReceiptsManager;
import org.whispersystems.textsecuregcm.storage.OneTimeDonationsManager;
import org.whispersystems.textsecuregcm.subscriptions.BraintreeManager;
import org.whispersystems.textsecuregcm.subscriptions.ChargeFailure;
import org.whispersystems.textsecuregcm.subscriptions.CustomerAwareSubscriptionPaymentProcessor;
import org.whispersystems.textsecuregcm.subscriptions.PaymentDetails;
import org.whispersystems.textsecuregcm.subscriptions.PaymentMethod;
import org.whispersystems.textsecuregcm.subscriptions.PaymentProvider;
import org.whispersystems.textsecuregcm.subscriptions.PaymentStatus;
import org.whispersystems.textsecuregcm.subscriptions.StripeManager;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionCurrencyUtil;
import org.whispersystems.textsecuregcm.util.ExactlySize;
import org.whispersystems.textsecuregcm.util.HeaderUtils;
import org.whispersystems.textsecuregcm.util.ua.ClientPlatform;
import org.whispersystems.textsecuregcm.util.ua.UnrecognizedUserAgentException;
import org.whispersystems.textsecuregcm.util.ua.UserAgentUtil;


/**
 * Endpoints for making one-time donation payments (boost and gift)
 * <p>
 * Note that these siblings of the endpoints at /v1/subscription on {@link SubscriptionController}. One-time payments do
 * not require the subscription management methods on that controller, though the configuration at
 * /v1/subscription/configuration is shared between subscription and one-time payments.
 */
@Path("/v1/subscription/boost")
@io.swagger.v3.oas.annotations.tags.Tag(name = "OneTimeDonations")
public class OneTimeDonationController {

  private static final Logger logger = LoggerFactory.getLogger(OneTimeDonationController.class);

  private static final String EURO_CURRENCY_CODE = "EUR";

  private final Clock clock;
  private final OneTimeDonationConfiguration oneTimeDonationConfiguration;
  private final StripeManager stripeManager;
  private final BraintreeManager braintreeManager;
  private final ServerZkReceiptOperations zkReceiptOperations;
  private final IssuedReceiptsManager issuedReceiptsManager;
  private final OneTimeDonationsManager oneTimeDonationsManager;

  public OneTimeDonationController(
      @Nonnull Clock clock,
      @Nonnull OneTimeDonationConfiguration oneTimeDonationConfiguration,
      @Nonnull StripeManager stripeManager,
      @Nonnull BraintreeManager braintreeManager,
      @Nonnull ServerZkReceiptOperations zkReceiptOperations,
      @Nonnull IssuedReceiptsManager issuedReceiptsManager,
      @Nonnull OneTimeDonationsManager oneTimeDonationsManager) {
    this.clock = Objects.requireNonNull(clock);
    this.oneTimeDonationConfiguration = Objects.requireNonNull(oneTimeDonationConfiguration);
    this.stripeManager = Objects.requireNonNull(stripeManager);
    this.braintreeManager = Objects.requireNonNull(braintreeManager);
    this.zkReceiptOperations = Objects.requireNonNull(zkReceiptOperations);
    this.issuedReceiptsManager = Objects.requireNonNull(issuedReceiptsManager);
    this.oneTimeDonationsManager = Objects.requireNonNull(oneTimeDonationsManager);
  }

  public static class CreateBoostRequest {

    @Schema(required = true, maxLength = 3, minLength = 3)
    @NotEmpty
    @ExactlySize(3)
    public String currency;

    @Schema(required = true, minimum = "1", description = "The amount to pay in the [currency's minor unit](https://docs.stripe.com/currencies#minor-units)")
    @Min(1)
    public long amount;

    @Schema(description = "The level for the boost payment. Assumed to be the boost level if missing")
    public Long level;

    @Schema(description = "The payment method", defaultValue = "CARD")
    public PaymentMethod paymentMethod = PaymentMethod.CARD;
  }

  public record CreateBoostResponse(
      @Schema(description = "A client secret that can be used to complete a stripe PaymentIntent")
      String clientSecret) {}

  @POST
  @Path("/create")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Create a Stripe payment intent", description = """
  Create a Stripe PaymentIntent and return a client secret that can be used to complete the payment.

  Once the payment is complete, the paymentIntentId can be used at /v1/subscriptions/receipt_credentials
  """)
  @ApiResponse(responseCode = "200", description = "Payment Intent created", content = @Content(schema = @Schema(implementation = CreateBoostResponse.class)))
  @ApiResponse(responseCode = "403", description = "The request was made on an authenticated channel")
  @ApiResponse(responseCode = "400", description = """
      Invalid argument. The response body may include an error code with more specific information. If the error code
      is `amount_below_currency_minimum` the body will also include the `minimum` field indicating the minimum amount
      for the currency. If the error code is `amount_above_sepa_limit` the body will also include the `maximum`
      field indicating the maximum amount for a SEPA transaction.
      """,
      content = @Content(schema = @Schema(
          type = "object",
          properties = {
              @StringToClassMapItem(key = "error", value = String.class)
          })))
  @ApiResponse(responseCode = "409", description = "Provided level does not match the currency/amount combination",
      content = @Content(schema = @Schema(
          type = "object",
          properties = {
              @StringToClassMapItem(key = "error", value = String.class)
          })))
  public CompletableFuture<Response> createBoostPaymentIntent(
      @Auth Optional<AuthenticatedDevice> authenticatedAccount,
      @NotNull @Valid CreateBoostRequest request,
      @HeaderParam(HttpHeaders.USER_AGENT) final String userAgent) {

    if (authenticatedAccount.isPresent()) {
      throw new ForbiddenException("must not use authenticated connection for one-time donation operations");
    }

    return CompletableFuture.runAsync(() -> {
          if (request.level == null) {
            request.level = oneTimeDonationConfiguration.boost().level();
          }
          BigDecimal amount = BigDecimal.valueOf(request.amount);
          if (request.level == oneTimeDonationConfiguration.gift().level()) {
            BigDecimal amountConfigured = oneTimeDonationConfiguration.currencies()
                .get(request.currency.toLowerCase(Locale.ROOT)).gift();
            if (amountConfigured == null ||
                SubscriptionCurrencyUtil.convertConfiguredAmountToStripeAmount(request.currency, amountConfigured)
                    .compareTo(amount) != 0) {
              throw new WebApplicationException(
                  Response.status(Response.Status.CONFLICT).entity(Map.of("error", "level_amount_mismatch")).build());
            }
          }
          validateRequestCurrencyAmount(request, amount, stripeManager);
        })
        .thenCompose(unused -> stripeManager.createPaymentIntent(request.currency, request.amount, request.level,
            getClientPlatform(userAgent)))
        .thenApply(paymentIntent -> Response.ok(new CreateBoostResponse(paymentIntent.getClientSecret())).build());
  }

  /**
   * Validates that the currency is supported by the {@code manager} and {@code request.paymentMethod} and that the
   * amount meets minimum and maximum constraints.
   *
   * @throws BadRequestException indicates validation failed. Inspect {@code response.error} for details
   */
  private void validateRequestCurrencyAmount(CreateBoostRequest request, BigDecimal amount,
      CustomerAwareSubscriptionPaymentProcessor manager) {
    if (!manager.getSupportedCurrenciesForPaymentMethod(request.paymentMethod)
        .contains(request.currency.toLowerCase(Locale.ROOT))) {
      throw new BadRequestException(Response.status(Response.Status.BAD_REQUEST)
          .entity(Map.of("error", "unsupported_currency")).build());
    }

    BigDecimal minCurrencyAmountMajorUnits = oneTimeDonationConfiguration.currencies()
        .get(request.currency.toLowerCase(Locale.ROOT)).minimum();
    BigDecimal minCurrencyAmountMinorUnits = SubscriptionCurrencyUtil.convertConfiguredAmountToApiAmount(
        request.currency,
        minCurrencyAmountMajorUnits);
    if (minCurrencyAmountMinorUnits.compareTo(amount) > 0) {
      throw new BadRequestException(Response.status(Response.Status.BAD_REQUEST)
          .entity(Map.of(
              "error", "amount_below_currency_minimum",
              "minimum", minCurrencyAmountMajorUnits.toString())).build());
    }

    if (request.paymentMethod == PaymentMethod.SEPA_DEBIT &&
        amount.compareTo(SubscriptionCurrencyUtil.convertConfiguredAmountToApiAmount(
            EURO_CURRENCY_CODE,
            oneTimeDonationConfiguration.sepaMaximumEuros())) > 0) {
      throw new BadRequestException(Response.status(Response.Status.BAD_REQUEST)
          .entity(Map.of(
              "error", "amount_above_sepa_limit",
              "maximum", oneTimeDonationConfiguration.sepaMaximumEuros().toString())).build());
    }
  }

  public static class CreatePayPalBoostRequest extends CreateBoostRequest {

    @NotEmpty
    public String returnUrl;
    @NotEmpty
    public String cancelUrl;

    public CreatePayPalBoostRequest() {
      super.paymentMethod = PaymentMethod.PAYPAL;
    }
  }

  record CreatePayPalBoostResponse(String approvalUrl, String paymentId) {}

  @POST
  @Path("/paypal/create")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public CompletableFuture<Response> createPayPalBoost(
      @Auth Optional<AuthenticatedDevice> authenticatedAccount,
      @NotNull @Valid CreatePayPalBoostRequest request,
      @HeaderParam(HttpHeaders.USER_AGENT) final String userAgent,
      @Context ContainerRequestContext containerRequestContext) {

    if (authenticatedAccount.isPresent()) {
      throw new ForbiddenException("must not use authenticated connection for one-time donation operations");
    }

    return CompletableFuture.runAsync(() -> {
          if (request.level == null) {
            request.level = oneTimeDonationConfiguration.boost().level();
          }

          validateRequestCurrencyAmount(request, BigDecimal.valueOf(request.amount), braintreeManager);
        })
        .thenCompose(unused -> {
          final Locale locale = HeaderUtils.getAcceptableLanguagesForRequest(containerRequestContext).stream()
              .filter(l -> !"*".equals(l.getLanguage()))
              .findFirst()
              .orElse(Locale.US);

          return braintreeManager.createOneTimePayment(request.currency.toUpperCase(Locale.ROOT), request.amount,
              locale.toLanguageTag(),
              request.returnUrl, request.cancelUrl);
        })
        .thenApply(approvalDetails -> Response.ok(
            new CreatePayPalBoostResponse(approvalDetails.approvalUrl(), approvalDetails.paymentId())).build());
  }

  public static class ConfirmPayPalBoostRequest extends CreateBoostRequest {

    @NotEmpty
    public String payerId;
    @NotEmpty
    public String paymentId; // PAYID-…
    @NotEmpty
    public String paymentToken; // EC-…
  }

  record ConfirmPayPalBoostResponse(String paymentId) {}

  @POST
  @Path("/paypal/confirm")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public CompletableFuture<Response> confirmPayPalBoost(
      @Auth Optional<AuthenticatedDevice> authenticatedAccount,
      @NotNull @Valid ConfirmPayPalBoostRequest request,
      @HeaderParam(HttpHeaders.USER_AGENT) final String userAgent) {

    if (authenticatedAccount.isPresent()) {
      throw new ForbiddenException("must not use authenticated connection for one-time donation operations");
    }

    return CompletableFuture.runAsync(() -> {
          if (request.level == null) {
            request.level = oneTimeDonationConfiguration.boost().level();
          }
        })
        .thenCompose(unused -> braintreeManager.captureOneTimePayment(request.payerId, request.paymentId,
            request.paymentToken, request.currency, request.amount, request.level, getClientPlatform(userAgent)))
        .thenCompose(
            chargeSuccessDetails -> oneTimeDonationsManager.putPaidAt(chargeSuccessDetails.paymentId(), Instant.now()))
        .thenApply(paymentId -> Response.ok(
            new ConfirmPayPalBoostResponse(paymentId)).build());
  }

  public static class CreateBoostReceiptCredentialsRequest {

    /**
     * a payment ID from {@link #processor}
     */
    @NotNull
    public String paymentIntentId;
    @NotNull
    public byte[] receiptCredentialRequest;

    @NotNull
    public PaymentProvider processor = PaymentProvider.STRIPE;
  }

  public record CreateBoostReceiptCredentialsSuccessResponse(byte[] receiptCredentialResponse) {
  }

  public record CreateBoostReceiptCredentialsErrorResponse(
      @JsonInclude(JsonInclude.Include.NON_NULL) ChargeFailure chargeFailure) {}

  @POST
  @Path("/receipt_credentials")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public CompletableFuture<Response> createBoostReceiptCredentials(
      @Auth Optional<AuthenticatedDevice> authenticatedAccount,
      @NotNull @Valid final CreateBoostReceiptCredentialsRequest request,
      @HeaderParam(HttpHeaders.USER_AGENT) final String userAgent) {

    if (authenticatedAccount.isPresent()) {
      throw new ForbiddenException("must not use authenticated connection for one-time donation operations");
    }

    final CompletableFuture<PaymentDetails> paymentDetailsFut = switch (request.processor) {
      case STRIPE -> stripeManager.getPaymentDetails(request.paymentIntentId);
      case BRAINTREE -> braintreeManager.getPaymentDetails(request.paymentIntentId);
      case GOOGLE_PLAY_BILLING -> throw new BadRequestException("cannot use play billing for one-time donations");
      case APPLE_APP_STORE -> throw new BadRequestException("cannot use app store purchases for one-time donations");
    };

    return paymentDetailsFut.thenCompose(paymentDetails -> {
      if (paymentDetails == null) {
        throw new WebApplicationException(Response.Status.NOT_FOUND);
      } else if (paymentDetails.status() == PaymentStatus.PROCESSING) {
        return CompletableFuture.completedFuture(Response.noContent().build());
      } else if (paymentDetails.status() != PaymentStatus.SUCCEEDED) {
        throw new WebApplicationException(Response.status(Response.Status.PAYMENT_REQUIRED)
            .entity(new CreateBoostReceiptCredentialsErrorResponse(paymentDetails.chargeFailure())).build());
      }

      // The payment was successful, try to issue the receipt credential

      long level = oneTimeDonationConfiguration.boost().level();
      if (paymentDetails.customMetadata() != null) {
        String levelMetadata = paymentDetails.customMetadata()
            .getOrDefault("level", Long.toString(oneTimeDonationConfiguration.boost().level()));
        try {
          level = Long.parseLong(levelMetadata);
        } catch (NumberFormatException e) {
          logger.error("failed to parse level metadata ({}) on payment intent {}", levelMetadata,
              paymentDetails.id(), e);
          throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
        }
      }
      Duration levelExpiration;
      if (oneTimeDonationConfiguration.boost().level() == level) {
        levelExpiration = oneTimeDonationConfiguration.boost().expiration();
      } else if (oneTimeDonationConfiguration.gift().level() == level) {
        levelExpiration = oneTimeDonationConfiguration.gift().expiration();
      } else {
        logger.error("level ({}) returned from payment intent that is unknown to the server", level);
        throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
      }
      ReceiptCredentialRequest receiptCredentialRequest;
      try {
        receiptCredentialRequest = new ReceiptCredentialRequest(request.receiptCredentialRequest);
      } catch (InvalidInputException e) {
        throw new BadRequestException("invalid receipt credential request", e);
      }
      final long finalLevel = level;
      return issuedReceiptsManager.recordIssuance(paymentDetails.id(), request.processor,
              receiptCredentialRequest, clock.instant())
          .thenCompose(unused -> oneTimeDonationsManager.getPaidAt(paymentDetails.id(), paymentDetails.created()))
          .thenApply(paidAt -> {
            Instant expiration = paidAt
                .plus(levelExpiration)
                .truncatedTo(ChronoUnit.DAYS)
                .plus(1, ChronoUnit.DAYS);
            ReceiptCredentialResponse receiptCredentialResponse;
            try {
              receiptCredentialResponse = zkReceiptOperations.issueReceiptCredential(
                  receiptCredentialRequest, expiration.getEpochSecond(), finalLevel);
            } catch (VerificationFailedException e) {
              throw new BadRequestException("receipt credential request failed verification", e);
            }
            Metrics.counter(SubscriptionController.RECEIPT_ISSUED_COUNTER_NAME,
                    Tags.of(
                        Tag.of(SubscriptionController.PROCESSOR_TAG_NAME, request.processor.toString()),
                        Tag.of(SubscriptionController.TYPE_TAG_NAME, "boost"),
                        UserAgentTagUtil.getPlatformTag(userAgent)))
                .increment();
            return Response.ok(
                    new CreateBoostReceiptCredentialsSuccessResponse(receiptCredentialResponse.serialize()))
                .build();
          });
    });
  }

  @Nullable
  private static ClientPlatform getClientPlatform(@Nullable final String userAgentString) {
    try {
      return UserAgentUtil.parseUserAgentString(userAgentString).platform();
    } catch (final UnrecognizedUserAgentException e) {
      return null;
    }
  }
}
