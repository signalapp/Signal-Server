package org.whispersystems.textsecuregcm.grpc;

import static org.whispersystems.textsecuregcm.grpc.SubscriptionsUtil.getClientPlatform;
import static org.whispersystems.textsecuregcm.grpc.SubscriptionsUtil.getPayPalLocale;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.util.Locale;
import java.util.Optional;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import org.signal.chat.errors.FailedPrecondition;
import org.signal.chat.errors.FailedUnidentifiedAuthorization;
import org.signal.chat.errors.FailedZkAuthentication;
import org.signal.chat.errors.NotFound;
import org.signal.chat.purchase.CreatePayPalPaymentMethodRequest;
import org.signal.chat.purchase.CreatePayPalPaymentMethodResponse;
import org.signal.chat.purchase.CreatePaymentMethodRequest;
import org.signal.chat.purchase.CreatePaymentMethodResponse;
import org.signal.chat.purchase.DeleteSubscriberRequest;
import org.signal.chat.purchase.DeleteSubscriberResponse;
import org.signal.chat.purchase.GetBankMandateRequest;
import org.signal.chat.purchase.GetBankMandateResponse;
import org.signal.chat.purchase.GetReceiptCredentialsRequest;
import org.signal.chat.purchase.GetReceiptCredentialsResponse;
import org.signal.chat.purchase.GetSubscriptionInformationRequest;
import org.signal.chat.purchase.GetSubscriptionInformationResponse;
import org.signal.chat.purchase.PaymentRequired;
import org.signal.chat.purchase.SetDefaultPaymentMethodRequest;
import org.signal.chat.purchase.SetDefaultPaymentMethodResponse;
import org.signal.chat.purchase.SetIapSubscriptionRequest;
import org.signal.chat.purchase.SetIapSubscriptionResponse;
import org.signal.chat.purchase.SetSubscriptionLevelRequest;
import org.signal.chat.purchase.SetSubscriptionLevelResponse;
import org.signal.chat.purchase.SimpleSubscriptionsGrpc;
import org.signal.chat.purchase.UpdateSubscriberRequest;
import org.signal.chat.purchase.UpdateSubscriberResponse;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.donation.DonationPermit;
import org.whispersystems.textsecuregcm.configuration.SubscriptionConfiguration;
import org.whispersystems.textsecuregcm.configuration.SubscriptionLevelConfiguration;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.storage.DonationPermitsManager;
import org.whispersystems.textsecuregcm.storage.SubscriberCredentials;
import org.whispersystems.textsecuregcm.storage.SubscriptionManager;
import org.whispersystems.textsecuregcm.storage.Subscriptions;
import org.whispersystems.textsecuregcm.subscriptions.AppleAppStoreManager;
import org.whispersystems.textsecuregcm.subscriptions.BankMandateTranslator;
import org.whispersystems.textsecuregcm.subscriptions.BankTransferType;
import org.whispersystems.textsecuregcm.subscriptions.BraintreeManager;
import org.whispersystems.textsecuregcm.subscriptions.CustomerAwareSubscriptionPaymentProcessor;
import org.whispersystems.textsecuregcm.subscriptions.GooglePlayBillingManager;
import org.whispersystems.textsecuregcm.subscriptions.ProcessorCustomer;
import org.whispersystems.textsecuregcm.subscriptions.StripeManager;
import org.whispersystems.textsecuregcm.subscriptions.SubscriberIdCreationNotPermittedException;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionChargeFailurePaymentRequiredException;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionForbiddenException;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionInformation;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionInvalidArgumentsException;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionInvalidIdempotencyKeyException;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionInvalidLevelException;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionNotFoundException;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionPaymentRequiredException;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionPaymentRequiresActionException;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionProcessorConflictException;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionProcessorException;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionReceiptAlreadyRedeemedException;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionReceiptRequestedForOpenPaymentException;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionStatus;

public class SubscriptionsGrpcService extends SimpleSubscriptionsGrpc.SubscriptionsImplBase {

  private final Clock clock;
  private final SubscriptionConfiguration subscriptionConfiguration;
  private final SubscriptionManager subscriptionManager;
  private final DonationPermitsManager donationPermitsManager;
  private final StripeManager stripeManager;
  private final BraintreeManager braintreeManager;
  private final GooglePlayBillingManager googlePlayBillingManager;
  private final AppleAppStoreManager appleAppStoreManager;
  private final BankMandateTranslator bankMandateTranslator;

  static final String RECEIPT_ISSUED_COUNTER_NAME = MetricsUtil.name(SubscriptionsGrpcService.class, "receiptIssued");
  static final String PROCESSOR_TAG_NAME = "processor";
  static final String TYPE_TAG_NAME = "type";
  private static final String SUBSCRIPTION_TYPE_TAG_NAME = "subscriptionType";

  public SubscriptionsGrpcService(final Clock clock,
      final SubscriptionConfiguration subscriptionConfiguration,
      final SubscriptionManager subscriptionManager,
      final DonationPermitsManager donationPermitsManager,
      final StripeManager stripeManager,
      final BraintreeManager braintreeManager,
      final GooglePlayBillingManager googlePlayBillingManager,
      final AppleAppStoreManager appleAppStoreManager,
      final BankMandateTranslator bankMandateTranslator) {
    this.clock = clock;
    this.subscriptionConfiguration = subscriptionConfiguration;
    this.subscriptionManager = subscriptionManager;
    this.donationPermitsManager = donationPermitsManager;
    this.stripeManager = stripeManager;
    this.braintreeManager = braintreeManager;
    this.googlePlayBillingManager = googlePlayBillingManager;
    this.appleAppStoreManager = appleAppStoreManager;
    this.bankMandateTranslator = bankMandateTranslator;
  }

  @Override
  public UpdateSubscriberResponse updateSubscriber(final UpdateSubscriberRequest request) {

    try {
      final SubscriberCredentials subscriberCredentials = SubscriberCredentials.process(
          request.getSubscriberId().toByteArray(), clock);

      final boolean creationPermitted;
      try {
        if (request.getDonationPermit().isEmpty()) {
          creationPermitted = false;
        } else {
          final DonationPermit permit = new DonationPermit(request.getDonationPermit().toByteArray());

          creationPermitted = SubscriptionsUtil.verifyAndSpendDonationPermit(permit, donationPermitsManager, clock);
        }
      } catch (final InvalidInputException _) {
        throw GrpcExceptions.invalidArguments("invalid donation permit");
      } catch (final VerificationFailedException _) {
        return UpdateSubscriberResponse.newBuilder()
            .setPermitRejected(FailedZkAuthentication.newBuilder()
                .setDescription("donation permit failed verification")
                .build())
            .build();
      }

      subscriptionManager.updateSubscriber(subscriberCredentials, creationPermitted);
      return UpdateSubscriberResponse.newBuilder().setSuccess(Empty.getDefaultInstance()).build();
    } catch (final SubscriptionForbiddenException e) {
      return UpdateSubscriberResponse.newBuilder().setSubscriberIdMismatch(
          FailedUnidentifiedAuthorization.newBuilder().setDescription(e.errorDetail().orElse("")).build()).build();
    } catch (SubscriberIdCreationNotPermittedException _) {
      if (request.getDonationPermit().isEmpty()) {
        throw GrpcExceptions.invalidArguments("donation permit is required to create a subscriber ID");
      }

      return UpdateSubscriberResponse.newBuilder()
          .setPermitRejected(FailedZkAuthentication.newBuilder()
              .setDescription("donation permit was not valid")
              .build())
          .build();
    }

  }

  @Override
  public DeleteSubscriberResponse deleteSubscriber(final DeleteSubscriberRequest request)
      throws RateLimitExceededException {
    try {
      final SubscriberCredentials subscriberCredentials = SubscriberCredentials.process(
          request.getSubscriberId().toByteArray(), clock);
      subscriptionManager.deleteSubscriber(subscriberCredentials);
      return DeleteSubscriberResponse.newBuilder().setSuccess(Empty.getDefaultInstance()).build();
    } catch (final SubscriptionNotFoundException e) {
      return DeleteSubscriberResponse.newBuilder().setSubscriberNotFound(NotFound.newBuilder().build()).build();
    } catch (final SubscriptionInvalidArgumentsException e) {
      return DeleteSubscriberResponse.newBuilder().setCannotCancelSubscription(
          FailedPrecondition.newBuilder().setDescription(e.errorDetail().orElse("")).build()).build();
    }
  }

  @Override
  public CreatePaymentMethodResponse createPaymentMethod(final CreatePaymentMethodRequest request) {
    final SubscriberCredentials subscriberCredentials = SubscriberCredentials.process(
        request.getSubscriberId().toByteArray(), clock);
    final CustomerAwareSubscriptionPaymentProcessor customerAwareSubscriptionPaymentProcessor = switch (request.getPaymentMethod()) {
      case PAYMENT_METHOD_CARD, PAYMENT_METHOD_SEPA_DEBIT, PAYMENT_METHOD_IDEAL -> stripeManager;
      default -> throw GrpcExceptions.fieldViolation("payment_method", "Unsupported payment method");
    };

    try {
      final DonationPermit permit = new DonationPermit(request.getDonationPermit().toByteArray());

      if (!SubscriptionsUtil.verifyAndSpendDonationPermit(permit, donationPermitsManager, clock)) {
        return CreatePaymentMethodResponse.newBuilder()
            .setPermitRejected(FailedZkAuthentication.getDefaultInstance())
            .build();
      }
    } catch (final InvalidInputException _) {
      throw GrpcExceptions.invalidArguments("invalid donation permit");
    } catch (final VerificationFailedException _) {
      return CreatePaymentMethodResponse.newBuilder()
          .setPermitRejected(FailedZkAuthentication.getDefaultInstance())
          .build();
    }

    try {
      final String token = subscriptionManager.addPaymentMethodToCustomer(subscriberCredentials,
          customerAwareSubscriptionPaymentProcessor,
          getClientPlatform(RequestAttributesUtil.getUserAgent().orElse(null)),
          CustomerAwareSubscriptionPaymentProcessor::createPaymentMethodSetupToken);
      return CreatePaymentMethodResponse.newBuilder().setResult(
          CreatePaymentMethodResponse.CreatePaymentMethodResult.newBuilder().setClientSecret(token)
              .setPaymentProvider(customerAwareSubscriptionPaymentProcessor.getProvider().toProto()).build()).build();
    } catch (final SubscriptionNotFoundException e) {
      return CreatePaymentMethodResponse.newBuilder().setSubscriberNotFound(NotFound.newBuilder()).build();
    } catch (final SubscriptionForbiddenException e) {
      return CreatePaymentMethodResponse.newBuilder().setSubscriberIdMismatch(
          FailedUnidentifiedAuthorization.newBuilder().setDescription(e.errorDetail().orElse("")).build()).build();
    } catch (final SubscriptionProcessorConflictException e) {
      return CreatePaymentMethodResponse.newBuilder().setSubscriptionProcessorConflict(
          FailedPrecondition.newBuilder().setDescription(e.errorDetail().orElse("")).build()).build();
    }
  }

  @Override
  public CreatePayPalPaymentMethodResponse createPayPalPaymentMethod(final CreatePayPalPaymentMethodRequest request)
      throws IOException {
    final SubscriberCredentials subscriberCredentials = SubscriberCredentials.process(
        request.getSubscriberId().toByteArray(), clock);
    final Locale locale = getPayPalLocale(RequestAttributesUtil.getAvailableAcceptedLocales());
    try {
      final BraintreeManager.PayPalBillingAgreementApprovalDetails details = subscriptionManager.addPaymentMethodToCustomer(
          subscriberCredentials, braintreeManager, getClientPlatform(RequestAttributesUtil.getUserAgent().orElse(null)),
          (mgr, _) -> mgr.createPayPalBillingAgreement(request.getReturnUrl(), request.getCancelUrl(),
              locale.toLanguageTag()));
      return CreatePayPalPaymentMethodResponse.newBuilder().setResult(
          CreatePayPalPaymentMethodResponse.CreatePayPalPaymentMethodResult.newBuilder()
              .setApprovalUrl(details.approvalUrl()).setToken(details.billingAgreementToken()).build()).build();
    } catch (final SubscriptionNotFoundException e) {
      return CreatePayPalPaymentMethodResponse.newBuilder().setSubscriberNotFound(NotFound.newBuilder().build())
          .build();
    } catch (final SubscriptionForbiddenException e) {
      return CreatePayPalPaymentMethodResponse.newBuilder().setSubscriberIdMismatch(
          FailedUnidentifiedAuthorization.newBuilder().setDescription(e.errorDetail().orElse("")).build()).build();
    } catch (final SubscriptionProcessorConflictException e) {
      return CreatePayPalPaymentMethodResponse.newBuilder().setSubscriptionProcessorConflict(
          FailedPrecondition.newBuilder().setDescription(e.errorDetail().orElse("")).build()).build();
    }
  }

  @Override
  public SetDefaultPaymentMethodResponse setDefaultPaymentMethod(final SetDefaultPaymentMethodRequest request)
      throws IOException {
    try {
      final SubscriberCredentials subscriberCredentials = SubscriberCredentials.process(
          request.getSubscriberId().toByteArray(), clock);
      final CustomerAwareSubscriptionPaymentProcessor manager;
      final String paymentMethodId = switch (request.getRequestCase()) {
        case STRIPE -> {
          manager = stripeManager;
          yield request.getStripe().getPaymentMethodToken();
        }
        case BRAINTREE -> {
          manager = braintreeManager;
          yield request.getBraintree().getPaymentMethodToken();
        }
        case SEPA -> {
          manager = stripeManager;
          yield stripeManager.getGeneratedSepaIdFromSetupIntent(request.getSepa().getSetupIntentId());
        }
        default -> throw GrpcExceptions.fieldViolation("request", "No payment method specified");
      };

      final Subscriptions.Record record = subscriptionManager.getSubscriber(subscriberCredentials);
      return record.getProcessorCustomer().map(
          processorCustomer -> setDefaultPaymentMethodForCustomer(manager, processorCustomer, paymentMethodId,
              record.subscriptionId)).orElseGet(() ->
          // a missing customer ID indicates the client made requests out of order,
          // and needs to call create_payment_method to create a customer for the given payment method
          SetDefaultPaymentMethodResponse.newBuilder().setPaymentMethodNotSetUp(FailedPrecondition.newBuilder().build())
              .build());
    } catch (final SubscriptionNotFoundException e) {
      return SetDefaultPaymentMethodResponse.newBuilder().setSubscriberNotFound(NotFound.newBuilder().build()).build();
    } catch (final SubscriptionForbiddenException e) {
      return SetDefaultPaymentMethodResponse.newBuilder().setSubscriberIdMismatch(
          FailedUnidentifiedAuthorization.newBuilder().setDescription(e.errorDetail().orElse("")).build()).build();
    } catch (final SubscriptionProcessorConflictException e) {
      return SetDefaultPaymentMethodResponse.newBuilder()
          .setSubscriptionProcessorConflict(
              FailedPrecondition.newBuilder().setDescription(e.errorDetail().orElse("")).build()).build();
    }
  }

  private static SetDefaultPaymentMethodResponse setDefaultPaymentMethodForCustomer(
      final CustomerAwareSubscriptionPaymentProcessor processor, final ProcessorCustomer processorCustomer,
      final String paymentMethodId, final String subscriptionId) {
    try {
      processor.setDefaultPaymentMethodForCustomer(processorCustomer.customerId(), paymentMethodId, subscriptionId);
      return SetDefaultPaymentMethodResponse.newBuilder().setSuccess(Empty.getDefaultInstance()).build();
    } catch (final SubscriptionInvalidArgumentsException e) {
      // Here, invalid arguments must mean that the client has made requests out of order, and needs to finish
      // setting up the paymentMethod first
      return SetDefaultPaymentMethodResponse.newBuilder()
          .setPaymentMethodNotSetUp(FailedPrecondition.newBuilder().setDescription(e.errorDetail().orElse("")).build())
          .build();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }

  }

  @Override
  public SetSubscriptionLevelResponse setSubscriptionLevel(final SetSubscriptionLevelRequest request) {
    final SubscriberCredentials subscriberCredentials = SubscriberCredentials.process(
        request.getSubscriberId().toByteArray(), clock);
    try {
      final Subscriptions.Record record = subscriptionManager.getSubscriber(subscriberCredentials);
      return record.getProcessorCustomer().map(
          processorCustomer -> setSubscriptionLevelForCustomer(processorCustomer, subscriberCredentials, record,
              request)).orElseGet(() -> SetSubscriptionLevelResponse.newBuilder()
          .setPaymentMethodNotSetUp(FailedPrecondition.newBuilder().build()).build());

    } catch (final SubscriptionNotFoundException e) {
      return SetSubscriptionLevelResponse.newBuilder().setSubscriberNotFound(NotFound.newBuilder().build()).build();
    } catch (final SubscriptionForbiddenException e) {
      return SetSubscriptionLevelResponse.newBuilder().setSubscriberIdMismatch(
          FailedUnidentifiedAuthorization.newBuilder().setDescription(e.errorDetail().orElse("")).build()).build();
    }
  }

  private SetSubscriptionLevelResponse setSubscriptionLevelForCustomer(final ProcessorCustomer processorCustomer,
      final SubscriberCredentials subscriberCredentials, final Subscriptions.Record subscriptionRecord,
      final SetSubscriptionLevelRequest request) {
    final SubscriptionLevelConfiguration config = subscriptionConfiguration.getSubscriptionLevel(request.getLevel());
    if (config == null) {
      return SetSubscriptionLevelResponse.newBuilder().setUnsupportedLevel(FailedPrecondition.newBuilder().build())
          .build();
    }
    final Optional<String> templateId = Optional.ofNullable(
            config.prices().get(request.getCurrency().toLowerCase(Locale.ROOT)))
        .map(priceConfiguration -> priceConfiguration.processorIds().get(processorCustomer.processor()));
    if (templateId.isEmpty()) {
      return SetSubscriptionLevelResponse.newBuilder().setUnsupportedCurrency(FailedPrecondition.newBuilder().build())
          .build();
    }

    final CustomerAwareSubscriptionPaymentProcessor manager;
    switch (processorCustomer.processor()) {
      case STRIPE -> manager = stripeManager;
      case BRAINTREE -> manager = braintreeManager;
      default -> {
        return SetSubscriptionLevelResponse.newBuilder().setUnsupportedOperation(FailedPrecondition.newBuilder()
            .setDescription(
                "Operation cannot be performed with the '" + processorCustomer.processor() + "' payment provider")
            .build()).build();
      }
    }

    try {
      subscriptionManager.updateSubscriptionLevelForCustomer(subscriberCredentials, subscriptionRecord, manager,
          request.getLevel(), request.getCurrency(), request.getIdempotencyKey(), templateId.get(),
          (l1, l2) -> SubscriptionsUtil.subscriptionsAreSameType(subscriptionConfiguration, l1, l2));
      return SetSubscriptionLevelResponse.newBuilder().setSuccess(
              SetSubscriptionLevelResponse.SetSubscriptionLevelResult.newBuilder().setLevel(request.getLevel()).build())
          .build();
    } catch (final SubscriptionInvalidIdempotencyKeyException e) {
      return SetSubscriptionLevelResponse.newBuilder()
          .setInvalidIdempotencyKey(FailedPrecondition.newBuilder().setDescription(e.errorDetail().orElse("")).build())
          .build();
    } catch (final SubscriptionProcessorException e) {
      return SetSubscriptionLevelResponse.newBuilder()
          .setChargeFailure(SubscriptionsUtil.toChargeFailure(e.getProcessor(), e.getChargeFailure())).build();
    } catch (final SubscriptionPaymentRequiresActionException e) {
      return SetSubscriptionLevelResponse.newBuilder().setPaymentRequiresAction(FailedPrecondition.newBuilder().build())
          .build();
    } catch (final SubscriptionInvalidLevelException e) {
      return SetSubscriptionLevelResponse.newBuilder()
          .setInvalidLevelTransition(FailedPrecondition.newBuilder().build()).build();
    } catch (final SubscriptionProcessorConflictException e) {
      return SetSubscriptionLevelResponse.newBuilder().setSubscriptionProcessorConflict(
          FailedPrecondition.newBuilder().setDescription(e.errorDetail().orElse("")).build()).build();
    }

  }

  @Override
  public SetIapSubscriptionResponse setIapSubscription(final SetIapSubscriptionRequest request)
      throws RateLimitExceededException {
    final SubscriberCredentials subscriberCredentials = SubscriberCredentials.process(
        request.getSubscriberId().toByteArray(), clock);
    try {
      final long level = switch (request.getRequestCase()) {
        case APP_STORE -> subscriptionManager.updateAppStoreTransactionId(subscriberCredentials, appleAppStoreManager,
            request.getAppStore().getOriginalTransactionId());
        case PLAY_BILLING ->
            subscriptionManager.updatePlayBillingPurchaseToken(subscriberCredentials, googlePlayBillingManager,
                request.getPlayBilling().getPurchaseToken());
        default -> throw GrpcExceptions.fieldViolation("request", "must set request type");
      };
      return SetIapSubscriptionResponse.newBuilder()
          .setSuccess(SetIapSubscriptionResponse.SetIapSubscriptionResult.newBuilder().setLevel(level).build()).build();
    } catch (final SubscriptionNotFoundException e) {
      return SetIapSubscriptionResponse.newBuilder().setSubscriberNotFound(NotFound.newBuilder().build()).build();
    } catch (final SubscriptionForbiddenException e) {
      return SetIapSubscriptionResponse.newBuilder().setSubscriberIdMismatch(
          FailedUnidentifiedAuthorization.newBuilder().setDescription(e.errorDetail().orElse("")).build()).build();
    } catch (final SubscriptionProcessorConflictException e) {
      return SetIapSubscriptionResponse.newBuilder().setSubscriptionProcessorConflict(
          FailedPrecondition.newBuilder().setDescription(e.errorDetail().orElse("")).build()).build();
    } catch (final SubscriptionPaymentRequiredException e) {
      return SetIapSubscriptionResponse.newBuilder().setPaymentRequired(FailedPrecondition.newBuilder().build())
          .build();
    } catch (final SubscriptionInvalidArgumentsException e) {
      return SetIapSubscriptionResponse.newBuilder()
          .setInvalidTransaction(FailedPrecondition.newBuilder().setDescription(e.errorDetail().orElse("")).build())
          .build();
    }
  }

  @Override
  public GetSubscriptionInformationResponse getSubscriptionInformation(final GetSubscriptionInformationRequest request)
      throws RateLimitExceededException {
    final SubscriberCredentials subscriberCredentials = SubscriberCredentials.process(
        request.getSubscriberId().toByteArray(), clock);
    try {
      return subscriptionManager.getSubscriptionInformation(subscriberCredentials)
          .map(SubscriptionsGrpcService::buildSubscriptionInformationResponse).orElseGet(
              () -> GetSubscriptionInformationResponse.newBuilder().setNoSubscription(Empty.getDefaultInstance())
                  .build());
    } catch (final SubscriptionNotFoundException e) {
      return GetSubscriptionInformationResponse.newBuilder().setSubscriberNotFound(NotFound.newBuilder().build())
          .build();
    } catch (final SubscriptionForbiddenException e) {
      return GetSubscriptionInformationResponse.newBuilder().setSubscriberIdMismatch(
          FailedUnidentifiedAuthorization.newBuilder().setDescription(e.errorDetail().orElse("")).build()).build();
    }
  }

  private static GetSubscriptionInformationResponse buildSubscriptionInformationResponse(
      final SubscriptionInformation info) {

    final GetSubscriptionInformationResponse.Subscription.Builder subscription = GetSubscriptionInformationResponse.Subscription.newBuilder()
        .setLevel(info.level()).setEndOfCurrentPeriod(info.endOfCurrentPeriod().getEpochSecond())
        .setActive(info.active()).setCancelAtPeriodEnd(info.cancelAtPeriodEnd()).setCurrency(info.price().currency())
        .setAmount(info.price().amount().longValue()).setStatus(toProtoSubscriptionStatus(info.status()))
        .setProcessor(info.paymentProvider().toProto()).setPaymentMethod(info.paymentMethod().toProtoPaymentMethod())
        .setPaymentProcessing(info.paymentProcessing());
    if (info.billingCycleAnchor() != null) {
      subscription.setBillingCycleAnchor(info.billingCycleAnchor().getEpochSecond());
    }
    if (info.chargeFailure() != null) {
      subscription.setChargeFailure(SubscriptionsUtil.toChargeFailure(info.paymentProvider(), info.chargeFailure()));
    }
    return GetSubscriptionInformationResponse.newBuilder().setSuccess(subscription.build()).build();

  }

  @Override
  public GetReceiptCredentialsResponse getReceiptCredentials(final GetReceiptCredentialsRequest request)
      throws RateLimitExceededException {
    final SubscriberCredentials subscriberCredentials = SubscriberCredentials.process(
        request.getSubscriberId().toByteArray(), clock);
    try {
      final SubscriptionManager.ReceiptResult result = subscriptionManager.createReceiptCredentials(
          subscriberCredentials, request.getReceiptCredentialRequest().toByteArray(),
          r -> SubscriptionsUtil.receiptExpirationWithGracePeriod(subscriptionConfiguration, r));
      Metrics.counter(RECEIPT_ISSUED_COUNTER_NAME,
              Tags.of(
                  Tag.of(PROCESSOR_TAG_NAME, result.paymentProvider().toString()),
                  Tag.of(TYPE_TAG_NAME, "subscription"),
                  Tag.of(SUBSCRIPTION_TYPE_TAG_NAME,
                      subscriptionConfiguration.getSubscriptionLevel(result.receiptItem().level()).type().name()
                          .toLowerCase(Locale.ROOT)),
                  UserAgentTagUtil.getPlatformTag(RequestAttributesUtil.getUserAgent().orElse(null))))
          .increment();
      return GetReceiptCredentialsResponse.newBuilder().setSuccess(
          GetReceiptCredentialsResponse.GetReceiptCredentialsResult.newBuilder()
              .setReceiptCredentialResponse(ByteString.copyFrom(result.receiptCredentialResponse().serialize()))
              .build()).build();
    } catch (final SubscriptionReceiptRequestedForOpenPaymentException e) {
      return GetReceiptCredentialsResponse.newBuilder().setNoPaidInvoice(FailedPrecondition.newBuilder().build())
          .build();
    } catch (final SubscriptionChargeFailurePaymentRequiredException e) {
      return GetReceiptCredentialsResponse.newBuilder().setPaymentRequired(
          PaymentRequired.newBuilder()
              .setChargeFailure(SubscriptionsUtil.toChargeFailure(e.getProcessor(), e.getChargeFailure())).build()).build();
    } catch (final SubscriptionPaymentRequiredException e) {
      return GetReceiptCredentialsResponse.newBuilder()
          .setPaymentRequired(PaymentRequired.newBuilder().build()).build();
    } catch (final SubscriptionInvalidArgumentsException e) {
      throw GrpcExceptions.invalidArguments(e.errorDetail().orElse(""));
    } catch (final SubscriptionReceiptAlreadyRedeemedException e) {
      return GetReceiptCredentialsResponse.newBuilder().setAlreadyRedeemed(FailedPrecondition.newBuilder().build())
          .build();
    } catch (final SubscriptionNotFoundException e) {
      return GetReceiptCredentialsResponse.newBuilder().setSubscriberNotFound(NotFound.newBuilder().build()).build();
    } catch (final SubscriptionForbiddenException e) {
      return GetReceiptCredentialsResponse.newBuilder().setSubscriberIdMismatch(
          FailedUnidentifiedAuthorization.newBuilder().setDescription(e.errorDetail().orElse("")).build()).build();
    }
  }

  private static org.signal.chat.purchase.SubscriptionStatus toProtoSubscriptionStatus(
      final SubscriptionStatus status) {
    return switch (status) {
      case ACTIVE -> org.signal.chat.purchase.SubscriptionStatus.SUBSCRIPTION_STATUS_ACTIVE;
      case INCOMPLETE -> org.signal.chat.purchase.SubscriptionStatus.SUBSCRIPTION_STATUS_INCOMPLETE;
      case PAST_DUE -> org.signal.chat.purchase.SubscriptionStatus.SUBSCRIPTION_STATUS_PAST_DUE;
      case CANCELED -> org.signal.chat.purchase.SubscriptionStatus.SUBSCRIPTION_STATUS_CANCELED;
      case UNPAID -> org.signal.chat.purchase.SubscriptionStatus.SUBSCRIPTION_STATUS_UNPAID;
      case UNKNOWN -> org.signal.chat.purchase.SubscriptionStatus.SUBSCRIPTION_STATUS_UNKNOWN;
    };
  }

  @Override
  public GetBankMandateResponse getBankMandate(final GetBankMandateRequest request) {
    final BankTransferType bankTransferType = switch (request.getBankTransferType()) {
      case BANK_TRANSFER_TYPE_SEPA_DEBIT -> BankTransferType.SEPA_DEBIT;
      default -> throw GrpcExceptions.fieldViolation("bank_transfer_type", "Unsupported bank transfer type");
    };
    final String mandate = bankMandateTranslator.translate(RequestAttributesUtil.getAvailableAcceptedLocales(),
        bankTransferType);
    return GetBankMandateResponse.newBuilder().setMandate(mandate).build();
  }

}
