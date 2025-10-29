/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.subscriptions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
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
import com.apple.itunes.storekit.verification.SignedDataVerifier;
import com.apple.itunes.storekit.verification.VerificationException;
import java.io.IOException;
import java.io.UncheckedIOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
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

  @ParameterizedTest
  @EnumSource(value = APIError.class, mode = EnumSource.Mode.EXCLUDE, names = "TRANSACTION_ID_NOT_FOUND")
  public void noFallbackOnOtherErrors(APIError error) throws APIException, IOException {
    when(productionClient.getAllSubscriptionStatuses(ORIGINAL_TX_ID, new Status[]{}))
        .thenThrow(new APIException(404, error, "test"));

    assertThatException().isThrownBy(() -> apiWrapper.getAllSubscriptions(ORIGINAL_TX_ID));
    verifyNoInteractions(sandboxClient);
  }

  @Test
  public void fallbackOnNoTransactionFound()
      throws APIException, IOException, SubscriptionInvalidArgumentsException, SubscriptionNotFoundException, RateLimitExceededException {
    when(productionClient.getAllSubscriptionStatuses(ORIGINAL_TX_ID, new Status[]{}))
        .thenThrow(new APIException(404, APIError.TRANSACTION_ID_NOT_FOUND, "test"));

    when(sandboxClient.getAllSubscriptionStatuses(ORIGINAL_TX_ID, new Status[]{}))
        .thenReturn(new StatusResponse().environment(Environment.SANDBOX));

    final StatusResponse allSubscriptions = apiWrapper.getAllSubscriptions(ORIGINAL_TX_ID);

    assertThat(allSubscriptions.getEnvironment()).isEqualTo(Environment.SANDBOX);
    verify(productionClient).getAllSubscriptionStatuses(ORIGINAL_TX_ID, new Status[]{});
    verify(sandboxClient).getAllSubscriptionStatuses(ORIGINAL_TX_ID, new Status[]{});
  }

  @Test
  public void retryEventuallyWorks()
      throws APIException, IOException, VerificationException, RateLimitExceededException, SubscriptionException {
    // Should retry up to 3 times
    when(productionClient.getAllSubscriptionStatuses(ORIGINAL_TX_ID, new Status[]{}))
        .thenThrow(new APIException(404, APIError.ORIGINAL_TRANSACTION_ID_NOT_FOUND_RETRYABLE.errorCode(), "test"))
        .thenThrow(new APIException(404, APIError.ORIGINAL_TRANSACTION_ID_NOT_FOUND_RETRYABLE.errorCode(), "test"))
        .thenReturn(new StatusResponse().environment(Environment.PRODUCTION));
    final StatusResponse statusResponse = apiWrapper.getAllSubscriptions(ORIGINAL_TX_ID);
    assertThat(statusResponse.getEnvironment()).isEqualTo(Environment.PRODUCTION);
    verifyNoInteractions(sandboxClient);
  }

  @Test
  public void retryEventuallyGivesUp() throws APIException, IOException, VerificationException {
    // Should retry up to 3 times
    when(productionClient.getAllSubscriptionStatuses(ORIGINAL_TX_ID, new Status[]{}))
        .thenThrow(new APIException(404, APIError.ORIGINAL_TRANSACTION_ID_NOT_FOUND_RETRYABLE.errorCode(), "test"));
    assertThatException()
        .isThrownBy(() -> apiWrapper.getAllSubscriptions(ORIGINAL_TX_ID))
        .isInstanceOf(UncheckedIOException.class)
        .withRootCauseInstanceOf(APIException.class);

    verify(productionClient, times(3)).getAllSubscriptionStatuses(ORIGINAL_TX_ID, new Status[]{});
    verifyNoInteractions(sandboxClient);
  }

  @Test
  public void sandboxDoesRetries()
      throws APIException, IOException, SubscriptionInvalidArgumentsException, SubscriptionNotFoundException, RateLimitExceededException {
    when(productionClient.getAllSubscriptionStatuses(ORIGINAL_TX_ID, new Status[]{}))
        .thenThrow(new APIException(404, APIError.TRANSACTION_ID_NOT_FOUND, "test"));

    when(sandboxClient.getAllSubscriptionStatuses(ORIGINAL_TX_ID, new Status[]{}))
        .thenThrow(new APIException(404, APIError.ORIGINAL_TRANSACTION_ID_NOT_FOUND_RETRYABLE.errorCode(), "test"))
        .thenThrow(new APIException(404, APIError.ORIGINAL_TRANSACTION_ID_NOT_FOUND_RETRYABLE.errorCode(), "test"))
        .thenReturn(new StatusResponse().environment(Environment.SANDBOX));

    final StatusResponse statusResponse = apiWrapper.getAllSubscriptions(ORIGINAL_TX_ID);
    assertThat(statusResponse.getEnvironment()).isEqualTo(Environment.SANDBOX);

    verify(productionClient, times(3))
        .getAllSubscriptionStatuses(ORIGINAL_TX_ID, new Status[]{});
    verify(sandboxClient, times(3))
        .getAllSubscriptionStatuses(ORIGINAL_TX_ID, new Status[]{});
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

    apiWrapper.verify(environment, new LastTransactionsItem()
        .originalTransactionId(ORIGINAL_TX_ID)
        .status(Status.ACTIVE)
        .signedRenewalInfo(SIGNED_RENEWAL_INFO)
        .signedTransactionInfo(SIGNED_TX_INFO));

    verify(expectedVerifier).verifyAndDecodeTransaction(SIGNED_TX_INFO);
    verify(expectedVerifier).verifyAndDecodeRenewalInfo(SIGNED_RENEWAL_INFO);
    verifyNoInteractions(unexpectedVerifier);
  }
}
