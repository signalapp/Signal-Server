package org.whispersystems.textsecuregcm.grpc;

import static org.whispersystems.textsecuregcm.grpc.SubscriptionsUtil.getClientPlatform;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import java.math.BigDecimal;
import java.time.Clock;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.signal.chat.errors.FailedPrecondition;
import org.signal.chat.errors.FailedUnidentifiedAuthorization;
import org.signal.chat.errors.NotFound;
import org.signal.chat.subscriptions.CreatePayPalPaymentMethodRequest;
import org.signal.chat.subscriptions.CreatePayPalPaymentMethodResponse;
import org.signal.chat.subscriptions.CreatePaymentMethodRequest;
import org.signal.chat.subscriptions.CreatePaymentMethodResponse;
import org.signal.chat.subscriptions.DeleteSubscriberRequest;
import org.signal.chat.subscriptions.DeleteSubscriberResponse;
import org.signal.chat.subscriptions.GetBankMandateRequest;
import org.signal.chat.subscriptions.GetBankMandateResponse;
import org.signal.chat.subscriptions.GetConfigurationRequest;
import org.signal.chat.subscriptions.GetConfigurationResponse;
import org.signal.chat.subscriptions.GetReceiptCredentialsRequest;
import org.signal.chat.subscriptions.GetReceiptCredentialsResponse;
import org.signal.chat.subscriptions.GetSubscriptionInformationRequest;
import org.signal.chat.subscriptions.GetSubscriptionInformationResponse;
import org.signal.chat.subscriptions.PaymentMethod;
import org.signal.chat.subscriptions.SetDefaultPaymentMethodRequest;
import org.signal.chat.subscriptions.SetDefaultPaymentMethodResponse;
import org.signal.chat.subscriptions.SetIapSubscriptionRequest;
import org.signal.chat.subscriptions.SetIapSubscriptionResponse;
import org.signal.chat.subscriptions.SetSubscriptionLevelRequest;
import org.signal.chat.subscriptions.SetSubscriptionLevelResponse;
import org.signal.chat.subscriptions.SimpleSubscriptionsGrpc;
import org.signal.chat.subscriptions.UpdateSubscriberRequest;
import org.signal.chat.subscriptions.UpdateSubscriberResponse;
import org.whispersystems.textsecuregcm.badges.BadgeTranslator;
import org.whispersystems.textsecuregcm.configuration.OneTimeDonationConfiguration;
import org.whispersystems.textsecuregcm.configuration.SubscriptionConfiguration;
import org.whispersystems.textsecuregcm.configuration.SubscriptionLevelConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.entities.Badge;
import org.whispersystems.textsecuregcm.entities.PurchasableBadge;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.storage.SubscriberCredentials;
import org.whispersystems.textsecuregcm.storage.SubscriptionManager;
import org.whispersystems.textsecuregcm.storage.Subscriptions;
import org.whispersystems.textsecuregcm.subscriptions.AppleAppStoreManager;
import org.whispersystems.textsecuregcm.subscriptions.BankMandateTranslator;
import org.whispersystems.textsecuregcm.subscriptions.BankTransferType;
import org.whispersystems.textsecuregcm.subscriptions.BraintreeManager;
import org.whispersystems.textsecuregcm.subscriptions.ChargeFailure;
import org.whispersystems.textsecuregcm.subscriptions.CurrencyConfiguration;
import org.whispersystems.textsecuregcm.subscriptions.CustomerAwareSubscriptionPaymentProcessor;
import org.whispersystems.textsecuregcm.subscriptions.GooglePlayBillingManager;
import org.whispersystems.textsecuregcm.subscriptions.PaymentProvider;
import org.whispersystems.textsecuregcm.subscriptions.ProcessorCustomer;
import org.whispersystems.textsecuregcm.subscriptions.StripeManager;
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
  private final OneTimeDonationConfiguration oneTimeDonationConfiguration;
  private final SubscriptionManager subscriptionManager;
  private final StripeManager stripeManager;
  private final BraintreeManager braintreeManager;
  private final GooglePlayBillingManager googlePlayBillingManager;
  private final AppleAppStoreManager appleAppStoreManager;
  private final BadgeTranslator badgeTranslator;
  private final BankMandateTranslator bankMandateTranslator;
  private final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager;

  public SubscriptionsGrpcService(final Clock clock, final SubscriptionConfiguration subscriptionConfiguration,
      final OneTimeDonationConfiguration oneTimeDonationConfiguration, final SubscriptionManager subscriptionManager,
      final StripeManager stripeManager, final BraintreeManager braintreeManager,
      final GooglePlayBillingManager googlePlayBillingManager, final AppleAppStoreManager appleAppStoreManager,
      final BadgeTranslator badgeTranslator, final BankMandateTranslator bankMandateTranslator,
      final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager) {
    this.clock = clock;
    this.subscriptionConfiguration = subscriptionConfiguration;
    this.oneTimeDonationConfiguration = oneTimeDonationConfiguration;
    this.subscriptionManager = subscriptionManager;
    this.stripeManager = stripeManager;
    this.braintreeManager = braintreeManager;
    this.googlePlayBillingManager = googlePlayBillingManager;
    this.appleAppStoreManager = appleAppStoreManager;
    this.badgeTranslator = badgeTranslator;
    this.bankMandateTranslator = bankMandateTranslator;
    this.dynamicConfigurationManager = dynamicConfigurationManager;
  }

  @Override
  public UpdateSubscriberResponse updateSubscriber(final UpdateSubscriberRequest request) {

    try {
      final SubscriberCredentials subscriberCredentials = SubscriberCredentials.process(
          request.getSubscriberId().toByteArray(), clock);
      subscriptionManager.updateSubscriber(subscriberCredentials);
      return UpdateSubscriberResponse.newBuilder().setSuccess(Empty.getDefaultInstance()).build();
    } catch (final SubscriptionForbiddenException e) {
      return UpdateSubscriberResponse.newBuilder().setSubscriberIdMismatch(
          FailedUnidentifiedAuthorization.newBuilder().setDescription(e.errorDetail().orElse("")).build()).build();
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
    final PaymentMethod paymentMethod;
    final CustomerAwareSubscriptionPaymentProcessor customerAwareSubscriptionPaymentProcessor = switch (request.getPaymentMethod()) {
      case PAYMENT_METHOD_CARD, PAYMENT_METHOD_SEPA_DEBIT, PAYMENT_METHOD_IDEAL -> stripeManager;
      default -> throw GrpcExceptions.fieldViolation("payment_method", "Unsupported payment method");
    };

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
  public CreatePayPalPaymentMethodResponse createPayPalPaymentMethod(final CreatePayPalPaymentMethodRequest request) {
    final SubscriberCredentials subscriberCredentials = SubscriberCredentials.process(
        request.getSubscriberId().toByteArray(), clock);
    final Locale locale = RequestAttributesUtil.getAcceptableLanguages().stream().filter(r -> !"*".equals(r.getRange()))
        .findFirst().map(r -> Locale.forLanguageTag(r.getRange())).orElse(Locale.US);
    try {
      final BraintreeManager.PayPalBillingAgreementApprovalDetails details = subscriptionManager.addPaymentMethodToCustomer(
          subscriberCredentials, braintreeManager, getClientPlatform(RequestAttributesUtil.getUserAgent().orElse(null)),
          (mgr, customerId) -> mgr.createPayPalBillingAgreement(request.getReturnUrl(), request.getCancelUrl(),
              locale.toLanguageTag())).join();
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
  public SetDefaultPaymentMethodResponse setDefaultPaymentMethod(final SetDefaultPaymentMethodRequest request) {
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
        yield stripeManager.getGeneratedSepaIdFromSetupIntent(request.getSepa().getSetupIntentId()).join();
      }
      default -> throw GrpcExceptions.fieldViolation("request", "No payment method specified");
    };

    try {
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
          .setChargeFailure(toChargeFailure(e.getProcessor(), e.getChargeFailure())).build();
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
        .setProcessor(info.paymentProvider().toProto()).setPaymentMethod(toProtoPaymentMethod(info.paymentMethod()))
        .setPaymentProcessing(info.paymentProcessing());
    if (info.billingCycleAnchor() != null) {
      subscription.setBillingCycleAnchor(info.billingCycleAnchor().getEpochSecond());
    }
    if (info.chargeFailure() != null) {
      subscription.setChargeFailure(toChargeFailure(info.paymentProvider(), info.chargeFailure()));
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
      return GetReceiptCredentialsResponse.newBuilder().setSuccess(
          GetReceiptCredentialsResponse.GetReceiptCredentialsResult.newBuilder()
              .setReceiptCredentialResponse(ByteString.copyFrom(result.receiptCredentialResponse().serialize()))
              .build()).build();
    } catch (final SubscriptionReceiptRequestedForOpenPaymentException e) {
      return GetReceiptCredentialsResponse.newBuilder().setNoPaidInvoice(FailedPrecondition.newBuilder().build())
          .build();
    } catch (final SubscriptionChargeFailurePaymentRequiredException e) {
      return GetReceiptCredentialsResponse.newBuilder().setPaymentRequired(
          GetReceiptCredentialsResponse.PaymentRequired.newBuilder()
              .setChargeFailure(toChargeFailure(e.getProcessor(), e.getChargeFailure())).build()).build();
    } catch (final SubscriptionPaymentRequiredException e) {
      return GetReceiptCredentialsResponse.newBuilder()
          .setPaymentRequired(GetReceiptCredentialsResponse.PaymentRequired.newBuilder().build()).build();
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

  private static org.signal.chat.subscriptions.SubscriptionStatus toProtoSubscriptionStatus(
      final SubscriptionStatus status) {
    return switch (status) {
      case ACTIVE -> org.signal.chat.subscriptions.SubscriptionStatus.SUBSCRIPTION_STATUS_ACTIVE;
      case INCOMPLETE -> org.signal.chat.subscriptions.SubscriptionStatus.SUBSCRIPTION_STATUS_INCOMPLETE;
      case PAST_DUE -> org.signal.chat.subscriptions.SubscriptionStatus.SUBSCRIPTION_STATUS_PAST_DUE;
      case CANCELED -> org.signal.chat.subscriptions.SubscriptionStatus.SUBSCRIPTION_STATUS_CANCELED;
      case UNPAID -> org.signal.chat.subscriptions.SubscriptionStatus.SUBSCRIPTION_STATUS_UNPAID;
      case UNKNOWN -> org.signal.chat.subscriptions.SubscriptionStatus.SUBSCRIPTION_STATUS_UNKNOWN;
    };
  }

  private static PaymentMethod toProtoPaymentMethod(
      final org.whispersystems.textsecuregcm.subscriptions.PaymentMethod paymentMethod) {
    if (paymentMethod == null) {
      return PaymentMethod.PAYMENT_METHOD_UNKNOWN;
    }
    return switch (paymentMethod) {
      case CARD -> PaymentMethod.PAYMENT_METHOD_CARD;
      case SEPA_DEBIT -> PaymentMethod.PAYMENT_METHOD_SEPA_DEBIT;
      case IDEAL -> PaymentMethod.PAYMENT_METHOD_IDEAL;
      case PAYPAL -> PaymentMethod.PAYMENT_METHOD_PAYPAL;
      case GOOGLE_PLAY_BILLING -> PaymentMethod.PAYMENT_METHOD_GOOGLE_PLAY_BILLING;
      case APPLE_APP_STORE -> PaymentMethod.PAYMENT_METHOD_APPLE_APP_STORE;
      case UNKNOWN -> PaymentMethod.PAYMENT_METHOD_UNKNOWN;
    };
  }

  @Override
  public GetConfigurationResponse getConfiguration(final GetConfigurationRequest request) {
    final long maxBackupBytes = dynamicConfigurationManager.getConfiguration().getBackupConfiguration()
        .maxTotalMediaSize();

    final Map<Long, GetConfigurationResponse.BackupLevelConfiguration> backupLevels = subscriptionConfiguration.getBackupLevels()
        .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
            e -> GetConfigurationResponse.BackupLevelConfiguration.newBuilder().setStorageAllowanceBytes(maxBackupBytes)
                .setPlayProductId(e.getValue().playProductId()).setMediaTtlDays(e.getValue().mediaTtl().toDays())
                .build()));

    return GetConfigurationResponse.newBuilder().putAllCurrencies(buildCurrencyConfigurations())
        .putAllLevels(buildLevelConfigurations()).setBackup(
            GetConfigurationResponse.BackupConfiguration.newBuilder().putAllLevels(backupLevels)
                .setFreeTierMediaDays(subscriptionConfiguration.getbackupFreeTierMediaDuration().toDays()).build())
        .setSepaMaximumEuros(oneTimeDonationConfiguration.sepaMaximumEuros().toString()).build();
  }

  private Map<String, GetConfigurationResponse.CurrencyConfiguration> buildCurrencyConfigurations() {
    return SubscriptionsUtil.buildCurrencyConfiguration(List.of(stripeManager, braintreeManager),
            oneTimeDonationConfiguration, subscriptionConfiguration).entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> toProtoCurrencyConfiguration(e.getKey(), e.getValue())));
  }

  private Map<Long, GetConfigurationResponse.LevelConfiguration> buildLevelConfigurations() {
    return SubscriptionsUtil.buildDonationLevelsConfiguration(subscriptionConfiguration, oneTimeDonationConfiguration,
        badgeTranslator, RequestAttributesUtil.getAvailableAcceptedLocales()).entrySet().stream().collect(
        Collectors.toMap(Map.Entry::getKey, entry -> GetConfigurationResponse.LevelConfiguration.newBuilder()
            .setBadge(toProtoBadge(entry.getValue().badge())).build()));
  }

  private static GetConfigurationResponse.CurrencyConfiguration toProtoCurrencyConfiguration(final String currency,
      final CurrencyConfiguration config) {
    final GetConfigurationResponse.CurrencyConfiguration.Builder builder = GetConfigurationResponse.CurrencyConfiguration.newBuilder()
        .setMinimum(config.minimum().toString())
        .addAllSupportedPaymentMethods(
            config.supportedPaymentMethods().stream().map(SubscriptionsGrpcService::toProtoPaymentMethod).toList());
    config.oneTime().forEach((levelId, amounts) -> builder.putOneTime(levelId,
        GetConfigurationResponse.AmountList.newBuilder()
            .addAllAmounts(
                amounts.stream().map(BigDecimal::toString).toList())
            .build()));
    config.subscription()
        .forEach((levelId, amount) -> builder.putSubscription(levelId,
            amount.toString()));
    config.backupSubscription()
        .forEach((levelId, amount) -> builder.putBackupSubscription(levelId,
            amount.toString()));
    return builder.build();
  }

  private static GetConfigurationResponse.Badge toProtoBadge(final Badge badge) {
    final org.signal.chat.common.Badge commonBadge = org.signal.chat.common.Badge.newBuilder().setId(badge.getId())
        .setCategory(badge.getCategory()).setName(badge.getName()).setDescription(badge.getDescription())
        .addAllSprites6(badge.getSprites6()).setSvg(badge.getSvg()).addAllSvgs(badge.getSvgs().stream()
            .map(s -> org.signal.chat.common.BadgeSvg.newBuilder().setLight(s.getLight()).setDark(s.getDark()).build())
            .toList()).build();
    final GetConfigurationResponse.Badge.Builder builder = GetConfigurationResponse.Badge.newBuilder()
        .setBadge(commonBadge);
    if (badge instanceof final PurchasableBadge purchasableBadge) {
      builder.setDurationSeconds(purchasableBadge.getDuration().toSeconds());
    }
    return builder.build();
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

  private static org.signal.chat.subscriptions.ChargeFailure toChargeFailure(final PaymentProvider processor,
      final ChargeFailure chargeFailure) {
    final org.signal.chat.subscriptions.ChargeFailure.Builder builder = org.signal.chat.subscriptions.ChargeFailure.newBuilder()
        .setProcessor(processor.toProto()).setCode(chargeFailure.code()).setMessage(chargeFailure.message());
    if (chargeFailure.outcomeNetworkStatus() != null) {
      builder.setOutcomeNetworkStatus(chargeFailure.outcomeNetworkStatus());
    }
    if (chargeFailure.outcomeReason() != null) {
      builder.setOutcomeReason(chargeFailure.outcomeReason());
    }
    if (chargeFailure.outcomeType() != null) {
      builder.setOutcomeType(chargeFailure.outcomeType());
    }
    return builder.build();
  }

}
