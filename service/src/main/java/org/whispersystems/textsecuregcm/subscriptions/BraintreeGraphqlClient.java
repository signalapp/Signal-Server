/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.subscriptions;

import com.apollographql.apollo3.api.ApolloResponse;
import com.apollographql.apollo3.api.Operation;
import com.apollographql.apollo3.api.Operations;
import com.apollographql.apollo3.api.Optional;
import com.apollographql.apollo3.api.json.BufferedSinkJsonWriter;
import com.braintree.graphql.client.type.ChargePaymentMethodInput;
import com.braintree.graphql.client.type.CreatePayPalBillingAgreementInput;
import com.braintree.graphql.client.type.CreatePayPalOneTimePaymentInput;
import com.braintree.graphql.client.type.CustomFieldInput;
import com.braintree.graphql.client.type.MonetaryAmountInput;
import com.braintree.graphql.client.type.PayPalBillingAgreementChargePattern;
import com.braintree.graphql.client.type.PayPalBillingAgreementExperienceProfileInput;
import com.braintree.graphql.client.type.PayPalBillingAgreementInput;
import com.braintree.graphql.client.type.PayPalExperienceProfileInput;
import com.braintree.graphql.client.type.PayPalIntent;
import com.braintree.graphql.client.type.PayPalLandingPageType;
import com.braintree.graphql.client.type.PayPalOneTimePaymentInput;
import com.braintree.graphql.client.type.PayPalProductAttributesInput;
import com.braintree.graphql.client.type.PayPalUserAction;
import com.braintree.graphql.client.type.TokenizePayPalBillingAgreementInput;
import com.braintree.graphql.client.type.TokenizePayPalOneTimePaymentInput;
import com.braintree.graphql.client.type.TransactionInput;
import com.braintree.graphql.client.type.VaultPaymentMethodInput;
import com.braintree.graphql.clientoperation.ChargePayPalOneTimePaymentMutation;
import com.braintree.graphql.clientoperation.CreatePayPalBillingAgreementMutation;
import com.braintree.graphql.clientoperation.CreatePayPalOneTimePaymentMutation;
import com.braintree.graphql.clientoperation.TokenizePayPalBillingAgreementMutation;
import com.braintree.graphql.clientoperation.TokenizePayPalOneTimePaymentMutation;
import com.braintree.graphql.clientoperation.VaultPaymentMethodMutation;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.ServiceUnavailableException;
import okio.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.http.FaultTolerantHttpClient;

class BraintreeGraphqlClient {

  // required header value, recommended to be the date the integration began
  // https://graphql.braintreepayments.com/guides/making_api_calls/#the-braintree-version-header
  private static final String BRAINTREE_VERSION = "2022-10-01";

  private static final Logger logger = LoggerFactory.getLogger(BraintreeGraphqlClient.class);

  private final FaultTolerantHttpClient httpClient;
  private final URI graphqlUri;
  private final String authorizationHeader;

  BraintreeGraphqlClient(final FaultTolerantHttpClient httpClient,
      final String graphqlUri,
      final String publicKey,
      final String privateKey) {
    this.httpClient = httpClient;
    try {
      this.graphqlUri = new URI(graphqlUri);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid URI", e);
    }
    // “public”/“private” key is a bit of a misnomer, but we follow the upstream nomenclature
    // they are used for Basic auth similar to “client key”/“client secret” credentials
    this.authorizationHeader = "Basic " + Base64.getEncoder().encodeToString((publicKey + ":" + privateKey).getBytes());
  }

  CompletableFuture<CreatePayPalOneTimePaymentMutation.CreatePayPalOneTimePayment> createPayPalOneTimePayment(
      final BigDecimal amount, final String currency, final String returnUrl,
      final String cancelUrl, final String locale) {

    final CreatePayPalOneTimePaymentInput input = buildCreatePayPalOneTimePaymentInput(amount, currency, returnUrl,
        cancelUrl, locale);
    final CreatePayPalOneTimePaymentMutation mutation = new CreatePayPalOneTimePaymentMutation(input);
    final HttpRequest request = buildRequest(mutation);

    return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
        .thenApply(httpResponse ->
        {
          // IntelliJ users: type parameters error “no instance of type variable exists so that Data conforms to Data”
          // is not accurate; this might be fixed in Kotlin 1.8: https://youtrack.jetbrains.com/issue/KTIJ-21905/
          final CreatePayPalOneTimePaymentMutation.Data data = assertSuccessAndExtractData(httpResponse, mutation);
          return data.createPayPalOneTimePayment;
        });
  }

  private static CreatePayPalOneTimePaymentInput buildCreatePayPalOneTimePaymentInput(BigDecimal amount,
      String currency, String returnUrl, String cancelUrl, String locale) {

    return new CreatePayPalOneTimePaymentInput(
        Optional.absent(),
        Optional.absent(), // merchant account ID will be specified when charging
        new MonetaryAmountInput(amount.toString(), currency), // this could potentially use a CustomScalarAdapter
        cancelUrl,
        Optional.absent(),
        PayPalIntent.SALE,
        Optional.absent(),
        Optional.present(false), // offerPayLater,
        Optional.absent(),
        Optional.present(
            new PayPalExperienceProfileInput(Optional.present("Signal"),
                Optional.present(false),
                Optional.present(PayPalLandingPageType.LOGIN),
                Optional.present(locale),
                Optional.absent(),
                Optional.present(PayPalUserAction.COMMIT))),
        Optional.absent(),
        Optional.absent(),
        returnUrl,
        Optional.absent(),
        Optional.absent()
    );
  }

  CompletableFuture<TokenizePayPalOneTimePaymentMutation.TokenizePayPalOneTimePayment> tokenizePayPalOneTimePayment(
      final String payerId, final String paymentId, final String paymentToken) {

    final TokenizePayPalOneTimePaymentInput input = new TokenizePayPalOneTimePaymentInput(
        Optional.absent(),
        Optional.absent(), // merchant account ID will be specified when charging
        new PayPalOneTimePaymentInput(payerId, paymentId, paymentToken)
    );

    final TokenizePayPalOneTimePaymentMutation mutation = new TokenizePayPalOneTimePaymentMutation(input);
    final HttpRequest request = buildRequest(mutation);

    return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
        .thenApply(httpResponse -> {
          // IntelliJ users: type parameters error “no instance of type variable exists so that Data conforms to Data”
          // is not accurate; this might be fixed in Kotlin 1.8: https://youtrack.jetbrains.com/issue/KTIJ-21905/
          final TokenizePayPalOneTimePaymentMutation.Data data = assertSuccessAndExtractData(httpResponse, mutation);
          return data.tokenizePayPalOneTimePayment;
        });
  }

  CompletableFuture<ChargePayPalOneTimePaymentMutation.ChargePaymentMethod> chargeOneTimePayment(
      final String paymentMethodId, final BigDecimal amount, final String merchantAccount, final long level) {

    final List<CustomFieldInput> customFields = List.of(
        new CustomFieldInput("level", Optional.present(Long.toString(level))));

    final ChargePaymentMethodInput input = buildChargePaymentMethodInput(paymentMethodId, amount, merchantAccount,
        customFields);
    final ChargePayPalOneTimePaymentMutation mutation = new ChargePayPalOneTimePaymentMutation(input);
    final HttpRequest request = buildRequest(mutation);

    return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
        .thenApply(httpResponse -> {
          // IntelliJ users: type parameters error “no instance of type variable exists so that Data conforms to Data”
          // is not accurate; this might be fixed in Kotlin 1.8: https://youtrack.jetbrains.com/issue/KTIJ-21905/
          final ChargePayPalOneTimePaymentMutation.Data data = assertSuccessAndExtractData(httpResponse,
              mutation);
          return data.chargePaymentMethod;
        });
  }

  private static ChargePaymentMethodInput buildChargePaymentMethodInput(String paymentMethodId, BigDecimal amount,
      String merchantAccount, List<CustomFieldInput> customFields) {

    return new ChargePaymentMethodInput(
        Optional.absent(),
        paymentMethodId,
        new TransactionInput(
            // documented as “amount: whole number, or exactly two or three decimal places”
            amount.toString(), // this could potentially use a CustomScalarAdapter
            Optional.present(merchantAccount),
            Optional.absent(),
            Optional.absent(),
            Optional.absent(),
            Optional.absent(),
            Optional.present(customFields),
            Optional.absent(),
            Optional.absent(),
            Optional.absent(),
            Optional.absent(),
            Optional.absent(),
            Optional.absent(),
            Optional.absent(),
            Optional.absent(),
            Optional.absent(),
            Optional.absent(),
            Optional.absent(),
            Optional.absent()
        )
    );
  }

  public CompletableFuture<CreatePayPalBillingAgreementMutation.CreatePayPalBillingAgreement> createPayPalBillingAgreement(
      final String returnUrl, final String cancelUrl, final String locale) {

    final CreatePayPalBillingAgreementInput input = buildCreatePayPalBillingAgreementInput(returnUrl, cancelUrl,
        locale);
    final CreatePayPalBillingAgreementMutation mutation = new CreatePayPalBillingAgreementMutation(input);
    final HttpRequest request = buildRequest(mutation);

    return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
        .thenApply(httpResponse -> {
          // IntelliJ users: type parameters error “no instance of type variable exists so that Data conforms to Data”
          // is not accurate; this might be fixed in Kotlin 1.8: https://youtrack.jetbrains.com/issue/KTIJ-21905/
          final CreatePayPalBillingAgreementMutation.Data data = assertSuccessAndExtractData(httpResponse, mutation);
          return data.createPayPalBillingAgreement;
        });
  }

  private static CreatePayPalBillingAgreementInput buildCreatePayPalBillingAgreementInput(String returnUrl,
      String cancelUrl, String locale) {

    return new CreatePayPalBillingAgreementInput(
        Optional.absent(),
        Optional.absent(),
        returnUrl,
        cancelUrl,
        Optional.absent(),
        Optional.absent(),
        Optional.present(false), // offerPayPalCredit
        Optional.absent(),
        Optional.present(
            new PayPalBillingAgreementExperienceProfileInput(Optional.present("Signal"),
                Optional.present(false), // collectShippingAddress
                Optional.present(PayPalLandingPageType.LOGIN),
                Optional.present(locale),
                Optional.absent())),
        Optional.absent(),
        Optional.present(new PayPalProductAttributesInput(
            Optional.present(PayPalBillingAgreementChargePattern.RECURRING_PREPAID)
        ))
    );
  }

  public CompletableFuture<TokenizePayPalBillingAgreementMutation.TokenizePayPalBillingAgreement> tokenizePayPalBillingAgreement(
      final String billingAgreementToken) {

    final TokenizePayPalBillingAgreementInput input = new TokenizePayPalBillingAgreementInput(
        Optional.absent(),
        new PayPalBillingAgreementInput(billingAgreementToken));
    final TokenizePayPalBillingAgreementMutation mutation = new TokenizePayPalBillingAgreementMutation(input);
    final HttpRequest request = buildRequest(mutation);

    return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
        .thenApply(httpResponse -> {
          // IntelliJ users: type parameters error “no instance of type variable exists so that Data conforms to Data”
          // is not accurate; this might be fixed in Kotlin 1.8: https://youtrack.jetbrains.com/issue/KTIJ-21905/
          final TokenizePayPalBillingAgreementMutation.Data data = assertSuccessAndExtractData(httpResponse, mutation);
          return data.tokenizePayPalBillingAgreement;
        });
  }

  public CompletableFuture<VaultPaymentMethodMutation.VaultPaymentMethod> vaultPaymentMethod(final String customerId,
      final String paymentMethodId) {

    final VaultPaymentMethodInput input = buildVaultPaymentMethodInput(customerId, paymentMethodId);
    final VaultPaymentMethodMutation mutation = new VaultPaymentMethodMutation(input);
    final HttpRequest request = buildRequest(mutation);

    return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
        .thenApply(httpResponse -> {
          // IntelliJ users: type parameters error “no instance of type variable exists so that Data conforms to Data”
          // is not accurate; this might be fixed in Kotlin 1.8: https://youtrack.jetbrains.com/issue/KTIJ-21905/
          final VaultPaymentMethodMutation.Data data = assertSuccessAndExtractData(httpResponse, mutation);
          return data.vaultPaymentMethod;
        });
  }

  private static VaultPaymentMethodInput buildVaultPaymentMethodInput(String customerId, String paymentMethodId) {
    return new VaultPaymentMethodInput(
        Optional.absent(),
        paymentMethodId,
        Optional.absent(),
        Optional.absent(),
        Optional.present(customerId),
        Optional.absent(),
        Optional.absent()
    );
  }

  /**
   * Verifies that the HTTP response has a {@code 200} status code and the GraphQL response has no errors, otherwise
   * throws a {@link ServiceUnavailableException}.
   */
  private <T extends Operation<U>, U extends Operation.Data> U assertSuccessAndExtractData(
      HttpResponse<String> httpResponse, T operation) {

    if (httpResponse.statusCode() != 200) {
      logger.warn("Received HTTP response status {} ({})", httpResponse.statusCode(),
          httpResponse.headers().firstValue("paypal-debug-id").orElse("<debug id absent>"));
      throw new ServiceUnavailableException();
    }

    ApolloResponse<U> response = Operations.parseJsonResponse(operation, httpResponse.body());

    if (response.hasErrors() || response.data == null) {
      //noinspection ConstantConditions
      response.errors.forEach(
          error -> {
            final Object legacyCode = java.util.Optional.ofNullable(error.getExtensions())
                .map(extensions -> extensions.get("legacyCode"))
                .orElse("<none>");
            logger.warn("Received GraphQL error for {}: \"{}\" (legacyCode: {})",
                response.operation.name(), error.getMessage(), legacyCode);
          });

      throw new ServiceUnavailableException();
    }

    return response.data;
  }

  private HttpRequest buildRequest(final Operation<?> operation) {

    final Buffer buffer = new Buffer();
    Operations.composeJsonRequest(operation, new BufferedSinkJsonWriter(buffer));

    return HttpRequest.newBuilder()
        .uri(graphqlUri)
        .method("POST", HttpRequest.BodyPublishers.ofString(buffer.readUtf8()))
        .header("Content-Type", "application/json")
        .header("Authorization", authorizationHeader)
        .header("Braintree-Version", BRAINTREE_VERSION)
        .build();
  }
}
