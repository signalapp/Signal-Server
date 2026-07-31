/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.subscriptions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.apple.itunes.storekit.client.APIError;
import com.apple.itunes.storekit.client.APIException;
import com.apple.itunes.storekit.client.AppStoreServerAPIClient;
import com.apple.itunes.storekit.model.Environment;
import com.apple.itunes.storekit.model.JWSRenewalInfoDecodedPayload;
import com.apple.itunes.storekit.model.JWSTransactionDecodedPayload;
import com.apple.itunes.storekit.model.LastTransactionsItem;
import com.apple.itunes.storekit.model.Status;
import com.apple.itunes.storekit.model.StatusResponse;
import com.apple.itunes.storekit.model.TransactionInfoResponse;
import com.apple.itunes.storekit.verification.SignedDataVerifier;
import com.apple.itunes.storekit.verification.VerificationException;
import io.micrometer.core.instrument.Tags;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;

class AppleAppStoreClientTest {

  private final static String ORIGINAL_TX_ID = "originalTxIdTest";
  private final static String SIGNED_RENEWAL_INFO = "signedRenewalInfoTest";
  private final static String SIGNED_TX_INFO = "signedRenewalInfoTest";
  private final static String PRODUCT_ID = "productIdTest";

  private final AppStoreServerAPIClient productionClient = mock(AppStoreServerAPIClient.class);
  private final AppStoreServerAPIClient sandboxClient = mock(AppStoreServerAPIClient.class);
  private final SignedDataVerifier productionSignedDataVerifier = mock(SignedDataVerifier.class);
  private final SignedDataVerifier sandboxSignedDataVerifier = mock(SignedDataVerifier.class);
  private AppleAppStoreClient apiWrapper;

  @BeforeEach
  public void setup() {
    reset(productionClient, productionSignedDataVerifier, sandboxClient, sandboxSignedDataVerifier);
    apiWrapper = new AppleAppStoreClient(Environment.PRODUCTION, productionSignedDataVerifier, productionClient,
        sandboxSignedDataVerifier, sandboxClient, null);
  }

  @Test
  public void getAllSubscriptions()
      throws APIException, IOException, SubscriptionInvalidArgumentsException, SubscriptionNotFoundException, RateLimitExceededException {
    when(productionClient.getAllSubscriptionStatuses(ORIGINAL_TX_ID, new Status[]{}))
        .thenReturn(new StatusResponse().environment(Environment.PRODUCTION));
    assertThat(apiWrapper.getAllSubscriptions(ORIGINAL_TX_ID, Tags.empty()).getEnvironment())
        .isEqualTo(Environment.PRODUCTION);
  }

  @Test
  public void getAllSubscriptionsFallback()
      throws APIException, IOException, SubscriptionInvalidArgumentsException, SubscriptionNotFoundException, RateLimitExceededException {
    when(productionClient.getAllSubscriptionStatuses(ORIGINAL_TX_ID, new Status[]{}))
        .thenThrow(new APIException(404, APIError.TRANSACTION_ID_NOT_FOUND, "test"));
    when(sandboxClient.getAllSubscriptionStatuses(ORIGINAL_TX_ID, new Status[]{}))
        .thenReturn(new StatusResponse().environment(Environment.SANDBOX));

    assertThat(apiWrapper.getAllSubscriptions(ORIGINAL_TX_ID, Tags.empty()).getEnvironment()).isEqualTo(Environment.SANDBOX);
  }

  public static Stream<Arguments> getAllSubscriptionsErrors() {
    return Stream.of(
        Arguments.of(new APIException(404, APIError.ORIGINAL_TRANSACTION_ID_NOT_FOUND, "test"), SubscriptionNotFoundException.class),
        Arguments.of(new APIException(404, APIError.ACCOUNT_NOT_FOUND, "test"), SubscriptionNotFoundException.class),
        Arguments.of(new APIException(429, APIError.RATE_LIMIT_EXCEEDED, "test"), RateLimitExceededException.class),
        Arguments.of(new APIException(429, APIError.INVALID_ORIGINAL_TRANSACTION_ID, "test"), SubscriptionInvalidArgumentsException.class));
  }

  @ParameterizedTest
  @MethodSource
  public void getAllSubscriptionsErrors(final APIException error, final Class<? extends Exception> expected)
      throws APIException, IOException {
    when(productionClient.getAllSubscriptionStatuses(ORIGINAL_TX_ID, new Status[]{})).thenThrow(error);
    assertThatExceptionOfType(expected)
        .isThrownBy(() -> apiWrapper.getAllSubscriptions(ORIGINAL_TX_ID, Tags.empty()));
  }

  @Test
  public void lookupTransaction()
      throws APIException, IOException, RateLimitExceededException, VerificationException {
    final JWSTransactionDecodedPayload expected = new JWSTransactionDecodedPayload();
    when(productionClient.getTransactionInfo(ORIGINAL_TX_ID))
        .thenReturn(new TransactionInfoResponse().signedTransactionInfo("signed"));
    when(productionSignedDataVerifier.verifyAndDecodeTransaction("signed")).thenReturn(expected);
    assertThat(apiWrapper.lookupTransaction(ORIGINAL_TX_ID, Tags.empty())).hasValue(expected);
  }

  @Test
  public void lookupTransactionFallback()
      throws APIException, IOException, RateLimitExceededException, VerificationException {
    final JWSTransactionDecodedPayload expected = new JWSTransactionDecodedPayload();
    when(productionClient.getTransactionInfo(ORIGINAL_TX_ID))
        .thenThrow(new APIException(404, APIError.TRANSACTION_ID_NOT_FOUND, "test"));

    when(sandboxClient.getTransactionInfo(ORIGINAL_TX_ID))
        .thenReturn(new TransactionInfoResponse().signedTransactionInfo("signed"));
    when(sandboxSignedDataVerifier.verifyAndDecodeTransaction("signed")).thenReturn(expected);
    assertThat(apiWrapper.lookupTransaction(ORIGINAL_TX_ID, Tags.empty())).hasValue(expected);

    verifyNoInteractions(productionSignedDataVerifier);
  }

  public static Stream<Arguments> lookupTransactionErrors() {
    return Stream.of(
        Arguments.of(new APIException(404, APIError.ORIGINAL_TRANSACTION_ID_NOT_FOUND, "test"), null),
        Arguments.of(new APIException(404, APIError.ACCOUNT_NOT_FOUND, "test"), null),
        Arguments.of(new APIException(400, APIError.INVALID_TRANSACTION_ID, "test"), null),
        Arguments.of(new APIException(409, APIError.INVALID_ORIGINAL_TRANSACTION_ID, "test"), null),
        Arguments.of(new APIException(429, APIError.RATE_LIMIT_EXCEEDED, "test"), RateLimitExceededException.class));
  }

  @ParameterizedTest
  @MethodSource
  public void lookupTransactionErrors(final APIException error, final Class<? extends Exception> expected)
      throws APIException, IOException, RateLimitExceededException {
    when(productionClient.getTransactionInfo(ORIGINAL_TX_ID)).thenThrow(error);
    if (expected == null) {
      assertThat(apiWrapper.lookupTransaction(ORIGINAL_TX_ID, Tags.empty())).isEmpty();
    } else {
      assertThatExceptionOfType(expected)
          .isThrownBy(() -> apiWrapper.lookupTransaction(ORIGINAL_TX_ID, Tags.empty()));
    }
  }

  @ParameterizedTest
  @EnumSource(value = APIError.class, mode = EnumSource.Mode.EXCLUDE, names = "TRANSACTION_ID_NOT_FOUND")
  public void noFallbackOnOtherErrors(APIError error) {
    final ConfigurableFinder throwingFinder = new ConfigurableFinder(always(new APIException(404, error, "test")));
    assertThatExceptionOfType(APIException.class).isThrownBy(() ->
        apiWrapper.lookupByTransactionId(ORIGINAL_TX_ID, Tags.empty(), throwingFinder));
    assertThat(throwingFinder.attempts).allMatch(Environment.PRODUCTION::equals);
  }

  @Test
  public void fallbackOnNoTransactionFound()
      throws APIException, IOException {
    final ConfigurableFinder finder =
        new ConfigurableFinder(times(1, new APIException(404, APIError.TRANSACTION_ID_NOT_FOUND, "test")));

    assertThat(apiWrapper.lookupByTransactionId(ORIGINAL_TX_ID, Tags.empty(), finder)).isEqualTo(Environment.SANDBOX);
    assertThat(finder.attempts).containsExactly(Environment.PRODUCTION, Environment.SANDBOX);
  }

  @Test
  public void retryEventuallyWorks()
      throws APIException, IOException {
    // Should retry up to 3 times
    final ConfigurableFinder finder = new ConfigurableFinder(
        times(2, new APIException(404, APIError.ORIGINAL_TRANSACTION_ID_NOT_FOUND_RETRYABLE.errorCode(), "test")));

    assertThat(apiWrapper.lookupByTransactionId(ORIGINAL_TX_ID, Tags.empty(), finder)).isEqualTo(Environment.PRODUCTION);
    assertThat(finder.attempts).containsExactly(Environment.PRODUCTION, Environment.PRODUCTION, Environment.PRODUCTION);
  }

  @Test
  public void retryEventuallyGivesUp() {
    // Should retry up to 3 times
    final ConfigurableFinder finder =
        new ConfigurableFinder(always(new APIException(404, APIError.ORIGINAL_TRANSACTION_ID_NOT_FOUND_RETRYABLE.errorCode(), "test")));
    assertThatExceptionOfType(APIException.class)
        .isThrownBy(() -> apiWrapper.lookupByTransactionId(ORIGINAL_TX_ID, Tags.empty(), finder));
    assertThat(finder.attempts).containsExactly(Environment.PRODUCTION, Environment.PRODUCTION, Environment.PRODUCTION);
  }

  @Test
  public void sandboxDoesRetries()
      throws APIException, IOException {
    final ConfigurableFinder finder = new ConfigurableFinder(
        times(1, new APIException(404, APIError.TRANSACTION_ID_NOT_FOUND, "test")),
        times(2, new APIException(404, APIError.ORIGINAL_TRANSACTION_ID_NOT_FOUND_RETRYABLE.errorCode(), "test")));

    assertThat(apiWrapper.lookupByTransactionId(ORIGINAL_TX_ID, Tags.empty(), finder)).isEqualTo(Environment.SANDBOX);
    assertThat(finder.attempts).containsExactly(Environment.PRODUCTION, Environment.SANDBOX, Environment.SANDBOX, Environment.SANDBOX);
  }

  @ParameterizedTest
  @EnumSource(value = Environment.class, mode = EnumSource.Mode.INCLUDE, names = {"SANDBOX", "PRODUCTION"})
  public void verifySignatureTest(Environment environment) throws VerificationException {
    final SignedDataVerifier expectedVerifier, unexpectedVerifier;
    if (environment.equals(Environment.SANDBOX)) {
      expectedVerifier = sandboxSignedDataVerifier;
      unexpectedVerifier = productionSignedDataVerifier;
    } else {
      expectedVerifier = productionSignedDataVerifier;
      unexpectedVerifier = sandboxSignedDataVerifier;
    }

    when(expectedVerifier.verifyAndDecodeTransaction(SIGNED_TX_INFO))
        .thenReturn(new JWSTransactionDecodedPayload().productId(PRODUCT_ID));
    when(expectedVerifier.verifyAndDecodeRenewalInfo(SIGNED_RENEWAL_INFO))
        .thenReturn(new JWSRenewalInfoDecodedPayload());

    apiWrapper.verifySubscription(environment, new LastTransactionsItem()
        .originalTransactionId(ORIGINAL_TX_ID)
        .status(Status.ACTIVE)
        .signedRenewalInfo(SIGNED_RENEWAL_INFO)
        .signedTransactionInfo(SIGNED_TX_INFO));

    verify(expectedVerifier).verifyAndDecodeTransaction(SIGNED_TX_INFO);
    verify(expectedVerifier).verifyAndDecodeRenewalInfo(SIGNED_RENEWAL_INFO);
    verifyNoInteractions(unexpectedVerifier);
  }

  /// Indicates how many times to throw an exception on calls to [ConfigurableFinder#lookup]
  private static class ExceptionConfig {
    int numTimes;
    final APIException exception;

    ExceptionConfig(final int numTimes, final APIException exception) {
      this.numTimes = numTimes;
      this.exception = exception;
    }
  }

  /// [ExceptionConfig] that indicates `exception` should be thrown on every call
  private static ExceptionConfig always(final APIException exception) {
    return new ExceptionConfig(-1, exception);
  }

  /// [ExceptionConfig] that indicates `exception` should be thrown `numTimes`
  private static ExceptionConfig times(int numTimes, final APIException exception) {
    return new ExceptionConfig(numTimes, exception);
  }

  /// TransactionFinder with configurable exception behavior
  private static class ConfigurableFinder implements AppleAppStoreClient.TransactionFinder<Environment> {

    private final ArrayDeque<ExceptionConfig> configs;
    private final List<Environment> attempts = new ArrayList<>();

    ConfigurableFinder(ExceptionConfig... configs) {
      this.configs = new ArrayDeque<>(List.of(configs));
    }

    @Override
    public Environment lookup(final Environment environment, final String transactionId) throws APIException {
      attempts.add(environment);

      if (configs.isEmpty()) {
        return environment;
      }
      final ExceptionConfig first = configs.getFirst();
      if (first.numTimes == 1) {
        configs.removeFirst();
      }
      first.numTimes -= 1;
      throw first.exception;
    }

  }

}
