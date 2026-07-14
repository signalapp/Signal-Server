/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.controllers;

import static org.whispersystems.textsecuregcm.grpc.SubscriptionsUtil.getClientPlatform;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.net.HttpHeaders;
import com.stripe.model.PaymentIntent;
import io.dropwizard.auth.Auth;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
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
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.glassfish.jersey.server.ManagedAsync;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialRequest;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialResponse;
import org.signal.libsignal.zkgroup.receipts.ServerZkReceiptOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.DonationPermitHeader;
import org.whispersystems.textsecuregcm.configuration.OneTimeDonationConfiguration;
import org.whispersystems.textsecuregcm.grpc.OneTimeDonationUtil;
import org.whispersystems.textsecuregcm.grpc.SubscriptionsUtil;
import org.whispersystems.textsecuregcm.limits.RateLimitedByIp;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.storage.DonationPermitsManager;
import org.whispersystems.textsecuregcm.storage.IssuedReceiptsManager;
import org.whispersystems.textsecuregcm.storage.OneTimeDonationsManager;
import org.whispersystems.textsecuregcm.storage.WriteConflictException;
import org.whispersystems.textsecuregcm.subscriptions.BraintreeManager;
import org.whispersystems.textsecuregcm.subscriptions.ChargeFailure;
import org.whispersystems.textsecuregcm.subscriptions.CustomerAwareSubscriptionPaymentProcessor;
import org.whispersystems.textsecuregcm.subscriptions.PayPalDonationsTranslator;
import org.whispersystems.textsecuregcm.subscriptions.PaymentDetails;
import org.whispersystems.textsecuregcm.subscriptions.PaymentMethod;
import org.whispersystems.textsecuregcm.subscriptions.PaymentProvider;
import org.whispersystems.textsecuregcm.subscriptions.PaymentStatus;
import org.whispersystems.textsecuregcm.subscriptions.StripeManager;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionInvalidAmountException;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionProcessorException;
import org.whispersystems.textsecuregcm.util.ExactlySize;
import org.whispersystems.textsecuregcm.util.HeaderUtils;

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

  private final Clock clock;
  private final OneTimeDonationConfiguration oneTimeDonationConfiguration;
  private final StripeManager stripeManager;
  private final BraintreeManager braintreeManager;
  private final PayPalDonationsTranslator payPalDonationsTranslator;
  private final ServerZkReceiptOperations zkReceiptOperations;
  private final IssuedReceiptsManager issuedReceiptsManager;
  private final OneTimeDonationsManager oneTimeDonationsManager;
  private final DonationPermitsManager donationPermitsManager;

  public OneTimeDonationController(
      final Clock clock,
      final OneTimeDonationConfiguration oneTimeDonationConfiguration,
      final StripeManager stripeManager,
      final BraintreeManager braintreeManager,
      final PayPalDonationsTranslator payPalDonationsTranslator,
      final ServerZkReceiptOperations zkReceiptOperations,
      final IssuedReceiptsManager issuedReceiptsManager,
      final OneTimeDonationsManager oneTimeDonationsManager,
      final DonationPermitsManager donationPermitsManager) {
    this.clock = Objects.requireNonNull(clock);
    this.oneTimeDonationConfiguration = Objects.requireNonNull(oneTimeDonationConfiguration);
    this.stripeManager = Objects.requireNonNull(stripeManager);
    this.braintreeManager = Objects.requireNonNull(braintreeManager);
    this.payPalDonationsTranslator = Objects.requireNonNull(payPalDonationsTranslator);
    this.zkReceiptOperations = Objects.requireNonNull(zkReceiptOperations);
    this.issuedReceiptsManager = Objects.requireNonNull(issuedReceiptsManager);
    this.oneTimeDonationsManager = Objects.requireNonNull(oneTimeDonationsManager);
    this.donationPermitsManager = Objects.requireNonNull(donationPermitsManager);
  }

  public static class CreateBoostRequest {

    @Schema(maxLength = 3, minLength = 3)
    @NotEmpty
    @ExactlySize(3)
    public String currency;

    @Schema(minimum = "1", description = "The amount to pay in the [currency's minor unit](https://docs.stripe.com/currencies#minor-units)", requiredMode = Schema.RequiredMode.REQUIRED)
    @Min(1)
    public long amount;

    @Schema(description = "The level for the boost payment", defaultValue = "1", minimum = "1", requiredMode = Schema.RequiredMode.REQUIRED)
    @Min(1)
    public long level = 1;

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
  @ApiResponse(responseCode = "401", description = "Donation permit was invalid or already spent")
  @RateLimitedByIp(RateLimiters.For.ONE_TIME_DONATION)
  @ManagedAsync
  public CreateBoostResponse createBoostPaymentIntent(
      @Auth final Optional<AuthenticatedDevice> authenticatedAccount,

      @Parameter(description = "A base64-encoded donation permit retrieved from POST /v1/donation/permit")
      @HeaderParam(HeaderUtils.DONATION_PERMIT) final Optional<DonationPermitHeader> donationPermitHeader,

      @NotNull @Valid final CreateBoostRequest request,
      @HeaderParam(HttpHeaders.USER_AGENT) final String userAgent) throws SubscriptionInvalidAmountException {

    if (authenticatedAccount.isPresent()) {
      throw new ForbiddenException("must not use authenticated connection for one-time donation operations");
    }

    SubscriptionsUtil.recordDonationPermitPresent(donationPermitHeader.isPresent(), "boostCreate", userAgent);
    final boolean spendSuccessful = donationPermitHeader.map(
            permitHeader -> {
              try {
                return SubscriptionsUtil.verifyAndSpendDonationPermit(permitHeader.permit(), donationPermitsManager, clock);
              } catch (final VerificationFailedException e) {
                return false;
              }
            })
        .orElse(false);
    if (!spendSuccessful) {
      throw new WebApplicationException(Response.Status.UNAUTHORIZED);
    }

    validateRequestCurrencyAmount(request, BigDecimal.valueOf(request.amount), stripeManager);
    final PaymentIntent paymentIntent = stripeManager.createPaymentIntent(request.currency, request.amount,
        request.level,
        getClientPlatform(userAgent));
    return new CreateBoostResponse(paymentIntent.getClientSecret());
  }

  /**
   * Validates that the request level is valid, the currency is supported by the {@code manager} and
   * {@code request.paymentMethod}, and that the amount meets minimum and maximum constraints.
   *
   * @throws BadRequestException indicates validation failed. Inspect {@code response.error} for details
   */
  private void validateRequestCurrencyAmount(final CreateBoostRequest request, final BigDecimal amount,
      final CustomerAwareSubscriptionPaymentProcessor manager) {

    final Map<String, String> errorBody = switch (OneTimeDonationUtil.validateOneTimeDonationRequest(request.currency,
        amount, request.level, request.paymentMethod, oneTimeDonationConfiguration, manager)) {
      case OneTimeDonationUtil.OneTimeDonationRequestValidationResult.UnsupportedLevel _ ->
          Map.of("error", "invalid_level");
      case OneTimeDonationUtil.OneTimeDonationRequestValidationResult.UnsupportedCurrency _ ->
          Map.of("error", "unsupported_currency");
      case OneTimeDonationUtil.OneTimeDonationRequestValidationResult.AmountBelowMinimum(final BigDecimal min) ->
          Map.of("error", "amount_below_currency_minimum",
              "minimum", min.toString());
      case OneTimeDonationUtil.OneTimeDonationRequestValidationResult.AmountAboveSepaLimit(final BigDecimal max) ->
          Map.of("error", "amount_above_sepa_limit",
              "maximum", max.toString());
      case OneTimeDonationUtil.OneTimeDonationRequestValidationResult.Success _ -> Collections.emptyMap();
    };

    if (!errorBody.isEmpty()) {
      throw new BadRequestException(
          Response.status(Response.Status.BAD_REQUEST).entity(errorBody).build());
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

  public record CreatePayPalBoostResponse(String approvalUrl, String paymentId) {}

  @POST
  @Path("/paypal/create")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @ManagedAsync
  public CreatePayPalBoostResponse createPayPalBoost(
      @Auth final Optional<AuthenticatedDevice> authenticatedAccount,
      @NotNull @Valid final CreatePayPalBoostRequest request,
      @Context final ContainerRequestContext containerRequestContext) throws IOException {

    if (authenticatedAccount.isPresent()) {
      throw new ForbiddenException("must not use authenticated connection for one-time donation operations");
    }

    validateRequestCurrencyAmount(request, BigDecimal.valueOf(request.amount), braintreeManager);
    final List<Locale> acceptableLanguages =
        HeaderUtils.getAcceptableLanguagesForRequest(containerRequestContext);
    final OneTimeDonationUtil.LocalizedPayPalDonationLineItem localizedLineItem = OneTimeDonationUtil.localizePayPalDonationLineItem(
        payPalDonationsTranslator, acceptableLanguages);
    final BraintreeManager.PayPalOneTimePaymentApprovalDetails approvalDetails =
        braintreeManager.createOneTimePayment(request.currency.toUpperCase(Locale.ROOT), request.amount,
            localizedLineItem.locale().toLanguageTag(),
            request.returnUrl, request.cancelUrl, localizedLineItem.itemName());
    return new CreatePayPalBoostResponse(approvalDetails.approvalUrl(), approvalDetails.paymentId());
  }

  public static class ConfirmPayPalBoostRequest extends CreateBoostRequest {

    @NotEmpty
    public String payerId;
    @NotEmpty
    public String paymentId; // PAYID-…
    @NotEmpty
    public String paymentToken; // EC-…

    public ConfirmPayPalBoostRequest() {
      super.paymentMethod = PaymentMethod.PAYPAL;
    }
  }

  public record ConfirmPayPalBoostResponse(String paymentId) {}

  @POST
  @Path("/paypal/confirm")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @ManagedAsync
  public ConfirmPayPalBoostResponse confirmPayPalBoost(
      @Auth final Optional<AuthenticatedDevice> authenticatedAccount,
      @NotNull @Valid final ConfirmPayPalBoostRequest request,
      @HeaderParam(HttpHeaders.USER_AGENT) final String userAgent) throws SubscriptionProcessorException, IOException {

    if (authenticatedAccount.isPresent()) {
      throw new ForbiddenException("must not use authenticated connection for one-time donation operations");
    }

    validateRequestCurrencyAmount(request, BigDecimal.valueOf(request.amount), braintreeManager);
    final BraintreeManager.PayPalChargeSuccessDetails chargeSuccessDetails = braintreeManager.captureOneTimePayment(
        request.payerId, request.paymentId,
        request.paymentToken, request.currency, request.amount, request.level, getClientPlatform(userAgent));
    oneTimeDonationsManager.putPaidAt(chargeSuccessDetails.paymentId(), Instant.now());
    return new ConfirmPayPalBoostResponse(chargeSuccessDetails.paymentId());
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
  @ManagedAsync
  public Response createBoostReceiptCredentials(
      @Auth final Optional<AuthenticatedDevice> authenticatedAccount,
      @NotNull @Valid final CreateBoostReceiptCredentialsRequest request,
      @HeaderParam(HttpHeaders.USER_AGENT) final String userAgent) throws IOException {

    if (authenticatedAccount.isPresent()) {
      throw new ForbiddenException("must not use authenticated connection for one-time donation operations");
    }

    final Optional<PaymentDetails> maybePaymentDetails = (switch (request.processor) {
      case STRIPE -> stripeManager.getPaymentDetails(request.paymentIntentId);
      case BRAINTREE -> braintreeManager.getPaymentDetails(request.paymentIntentId);
      case GOOGLE_PLAY_BILLING -> throw new BadRequestException("cannot use play billing for one-time donations");
      case APPLE_APP_STORE -> throw new BadRequestException("cannot use app store purchases for one-time donations");
    });

    if (maybePaymentDetails.isEmpty()) {
      throw new WebApplicationException(Response.Status.NOT_FOUND);
    }
    final PaymentDetails paymentDetails = maybePaymentDetails.get();
    if (paymentDetails.status() == PaymentStatus.PROCESSING) {
      return Response.noContent().build();
    }
    if (paymentDetails.status() != PaymentStatus.SUCCEEDED) {
      throw new WebApplicationException(Response.status(Response.Status.PAYMENT_REQUIRED)
          .entity(new CreateBoostReceiptCredentialsErrorResponse(paymentDetails.chargeFailure())).build());
    }

    // The payment was successful, try to issue the receipt credential

    final OneTimeDonationUtil.DonationLevelDetails levelDetails;
    try {
      levelDetails = OneTimeDonationUtil.getLevelDetails(paymentDetails, oneTimeDonationConfiguration);
    } catch (OneTimeDonationUtil.InvalidLevelException _) {
      throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
    }

    final ReceiptCredentialRequest receiptCredentialRequest;
    try {
      receiptCredentialRequest = new ReceiptCredentialRequest(request.receiptCredentialRequest);
    } catch (final InvalidInputException e) {
      throw new BadRequestException("invalid receipt credential request", e);
    }
    try {
      issuedReceiptsManager.recordIssuance(paymentDetails.id(), request.processor,
          receiptCredentialRequest, clock.instant());
    } catch (WriteConflictException _) {
      throw new WebApplicationException(Response.Status.CONFLICT);
    }
    final Instant paidAt = oneTimeDonationsManager.getPaidAt(paymentDetails.id(), paymentDetails.created());
    final Instant expiration = paidAt
        .plus(levelDetails.levelExpiration())
        .truncatedTo(ChronoUnit.DAYS)
        .plus(1, ChronoUnit.DAYS);
    final ReceiptCredentialResponse receiptCredentialResponse;
    try {
      receiptCredentialResponse = zkReceiptOperations.issueReceiptCredential(
          receiptCredentialRequest, expiration.getEpochSecond(), levelDetails.level());
    } catch (final VerificationFailedException e) {
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
  }
}
