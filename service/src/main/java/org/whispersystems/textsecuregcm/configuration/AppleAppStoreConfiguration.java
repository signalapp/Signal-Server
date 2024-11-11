/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.apple.itunes.storekit.model.Environment;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretString;

/**
 * @param env                 The ios environment to use, typically SANDBOX or PRODUCTION
 * @param bundleId            The bundleId of the app
 * @param appAppleId          The integer id of the app
 * @param issuerId            The issuerId for the keys:
 *                            https://developer.apple.com/documentation/appstoreconnectapi/generating_tokens_for_api_requests
 * @param keyId               The keyId for encodedKey
 * @param encodedKey          A private key with the "In-App Purchase" key type
 * @param subscriptionGroupId The subscription group for in-app purchases
 * @param productIdToLevel    A map of productIds offered in the product catalog to their corresponding numeric
 *                            subscription levels
 * @param appleRootCerts      Apple root certificates to verify signed API responses, encoded as base64 strings:
 *                            https://www.apple.com/certificateauthority/
 */
public record AppleAppStoreConfiguration(
    @NotNull Environment env,
    @NotBlank String bundleId,
    @NotNull Long appAppleId,
    @NotBlank String issuerId,
    @NotBlank String keyId,
    @NotNull SecretString encodedKey,
    @NotBlank String subscriptionGroupId,
    @NotNull Map<String, Long> productIdToLevel,
    @NotNull List<@NotBlank String> appleRootCerts,
    @NotNull @Valid RetryConfiguration retry) {

  public AppleAppStoreConfiguration {
    if (retry == null) {
      retry = new RetryConfiguration();
    }
  }
}
