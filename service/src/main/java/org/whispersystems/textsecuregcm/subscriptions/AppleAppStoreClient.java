/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.subscriptions;

import com.apple.itunes.storekit.client.APIError;
import com.apple.itunes.storekit.client.APIException;
import com.apple.itunes.storekit.client.AppStoreServerAPIClient;
import com.apple.itunes.storekit.model.Environment;
import com.apple.itunes.storekit.model.LastTransactionsItem;
import com.apple.itunes.storekit.model.Status;
import com.apple.itunes.storekit.model.StatusResponse;
import com.apple.itunes.storekit.verification.SignedDataVerifier;
import com.apple.itunes.storekit.verification.VerificationException;
import com.google.common.annotations.VisibleForTesting;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.micrometer.core.instrument.Metrics;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.http.HttpResponse;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import io.micrometer.core.instrument.Tags;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.util.ResilienceUtil;

/// Client for interacting with the storekit server APIs.
///
/// This handles fetching information about a subscription from a transaction id, and then can be used to verify
/// individual transactions from that subscription with [#verify]. Transactions generated with both production and
/// sandbox environments can be used.
public class AppleAppStoreClient {

  private static final Status[] EMPTY_STATUSES = new Status[0];

  private static final String GET_SUBSCRIPTION_ERROR_COUNTER_NAME =
      MetricsUtil.name(AppleAppStoreClient.class, "getSubscriptionsError");

  private final Environment defaultEnvironment;
  private final SignedDataVerifier productionSignedDataVerifier;
  private final AppStoreServerAPIClient productionApiClient;
  private final SignedDataVerifier sandboxSignedDataVerifier;
  private final AppStoreServerAPIClient sandboxApiClient;
  private final Retry retry;


  /// Construct an AppleAppStoreClient
  ///
  /// @param defaultEnvironment     The first environment to try. If it is the production environment and a
  ///                               transactionId does not exist there, we will fallback to the sandbox environment
  /// @param bundleId               The bundleId of the app
  /// @param appAppleId             The integer id of the app
  /// @param issuerId               The issuerId for the
  ///                               [keys](https://developer.apple.com/documentation/appstoreconnectapi/generating_tokens_for_api_requests)
  /// @param keyId                  The keyId for encodedKey
  /// @param encodedKey             A private key with the "In-App Purchase" key type
  /// @param base64AppleRootCerts   [Apple root certificates](https://www.apple.com/certificateauthority/) to verify
  ///                               signed API responses, encoded as base64 strings:
  /// @param retryConfigurationName The name of the retry configuration to use in the App Store client; if `null`, uses
  ///                               the global default configuration.
  public AppleAppStoreClient(
      final Environment defaultEnvironment,
      final String bundleId,
      final long appAppleId,
      final String issuerId,
      final String keyId,
      final String encodedKey,
      final List<String> base64AppleRootCerts,
      @Nullable final String retryConfigurationName) {
    this(defaultEnvironment,
        new SignedDataVerifier(decodeRootCerts(base64AppleRootCerts), bundleId, appAppleId, Environment.PRODUCTION,
            true),
        new AppStoreServerAPIClient(encodedKey, keyId, issuerId, bundleId, Environment.PRODUCTION),
        new SignedDataVerifier(decodeRootCerts(base64AppleRootCerts), bundleId, appAppleId, Environment.SANDBOX, true),
        new AppStoreServerAPIClient(encodedKey, keyId, issuerId, bundleId, Environment.SANDBOX),
        retryConfigurationName);
  }

  @VisibleForTesting
  AppleAppStoreClient(
      Environment defaultEnvironment,
      SignedDataVerifier productionSignedDataVerifier,
      AppStoreServerAPIClient productionApiClient,
      SignedDataVerifier sandboxSignedDataVerifier,
      AppStoreServerAPIClient sandboxApiClient,
      @Nullable final String retryConfigurationName) {
    this.defaultEnvironment = defaultEnvironment;
    this.sandboxSignedDataVerifier = sandboxSignedDataVerifier;
    this.sandboxApiClient = sandboxApiClient;
    this.productionSignedDataVerifier = productionSignedDataVerifier;
    this.productionApiClient = productionApiClient;
    this.retry = ResilienceUtil.getRetryRegistry().retry("appstore-retry", RetryConfig
        .<HttpResponse<?>>from(Optional.ofNullable(retryConfigurationName)
            .flatMap(name -> ResilienceUtil.getRetryRegistry().getConfiguration(name))
            .orElseGet(() -> ResilienceUtil.getRetryRegistry().getDefaultConfig()))
        .retryOnException(AppleAppStoreClient::shouldRetry).build());
  }


  /// Verify signature and decode transaction payloads
  public AppleAppStoreDecodedTransaction verify(final Environment environment, final LastTransactionsItem tx) {
    final SignedDataVerifier signedDataVerifier = switch (environment) {
      case PRODUCTION -> productionSignedDataVerifier;
      case SANDBOX -> sandboxSignedDataVerifier;
      default -> throw new IllegalStateException("Unexpected environment: " + environment);
    };
    try {
      return new AppleAppStoreDecodedTransaction(
          tx,
          signedDataVerifier.verifyAndDecodeTransaction(tx.getSignedTransactionInfo()),
          signedDataVerifier.verifyAndDecodeRenewalInfo(tx.getSignedRenewalInfo()));
    } catch (VerificationException e) {
      throw new UncheckedIOException(new IOException("Failed to verify payload from App Store Server", e));
    }
  }

  public StatusResponse getAllSubscriptions(final String originalTransactionId, final Tags errorTags)
      throws SubscriptionNotFoundException, SubscriptionInvalidArgumentsException, RateLimitExceededException {
    try {
      return retry.executeCallable(() -> {
        try {
          return getAllSubscriptionsHelper(defaultEnvironment, originalTransactionId);
        } catch (final APIException e) {
          Metrics.counter(GET_SUBSCRIPTION_ERROR_COUNTER_NAME, errorTags.and("reason", e.getApiError().name())).increment();
          throw switch (e.getApiError()) {
            case TRANSACTION_ID_NOT_FOUND, ORIGINAL_TRANSACTION_ID_NOT_FOUND -> new SubscriptionNotFoundException();
            case RATE_LIMIT_EXCEEDED -> new RateLimitExceededException(null);
            case INVALID_ORIGINAL_TRANSACTION_ID -> new SubscriptionInvalidArgumentsException(e.getApiErrorMessage());
            default -> throw e;
          };
        } catch (final IOException e) {
          Metrics.counter(GET_SUBSCRIPTION_ERROR_COUNTER_NAME, "reason", "io_error").increment();
          throw e;
        }
      });
    } catch (SubscriptionNotFoundException | SubscriptionInvalidArgumentsException | RateLimitExceededException e) {
      throw e;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (APIException e) {
      throw new UncheckedIOException(new IOException(e));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private StatusResponse getAllSubscriptionsHelper(final Environment env, final String originalTransactionId)
      throws APIException, IOException {
    final AppStoreServerAPIClient client = switch (env) {
      case SANDBOX -> sandboxApiClient;
      case PRODUCTION -> productionApiClient;
      default -> throw new IllegalArgumentException("Unknown environment: " + env);
    };
    try {
      return client.getAllSubscriptionStatuses(originalTransactionId, EMPTY_STATUSES);
    } catch (APIException e) {
      // First attempts to look up the transaction on the production environment, falling back to the sandbox env if
      // the transaction is not found.
      // See: https://developer.apple.com/documentation/AppStoreServerAPI#Test-using-the-sandbox-environment
      if (env == Environment.PRODUCTION && e.getApiError() == APIError.TRANSACTION_ID_NOT_FOUND) {
        return getAllSubscriptionsHelper(Environment.SANDBOX, originalTransactionId);
      }
      throw e;
    }
  }

  private static Set<InputStream> decodeRootCerts(final List<String> rootCerts) {
    return rootCerts.stream()
        .map(Base64.getDecoder()::decode)
        .map(ByteArrayInputStream::new)
        .collect(Collectors.toSet());
  }

  private static boolean shouldRetry(Throwable e) {
    return e instanceof APIException apiException && switch (apiException.getApiError()) {
      case ORIGINAL_TRANSACTION_ID_NOT_FOUND_RETRYABLE, GENERAL_INTERNAL_RETRYABLE, APP_NOT_FOUND_RETRYABLE -> true;
      default -> false;
    };
  }


}
