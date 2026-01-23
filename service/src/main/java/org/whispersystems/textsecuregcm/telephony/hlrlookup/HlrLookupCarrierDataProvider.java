/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.telephony.hlrlookup;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.i18n.phonenumbers.Phonenumber;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.Nullable;
import io.micrometer.core.instrument.Metrics;
import org.apache.commons.lang3.StringUtils;
import org.whispersystems.textsecuregcm.http.FaultTolerantHttpClient;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.telephony.CarrierData;
import org.whispersystems.textsecuregcm.telephony.CarrierDataException;
import org.whispersystems.textsecuregcm.telephony.CarrierDataProvider;
import org.whispersystems.textsecuregcm.util.SystemMapper;

/// A carrier data provider that uses [HLR Lookup](https://www.hlrlookup.com/) as its data source.
public class HlrLookupCarrierDataProvider implements CarrierDataProvider {

  private final String apiKey;
  private final String apiSecret;

  private final FaultTolerantHttpClient httpClient;
  private final URI lookupUri;

  private static final URI HLR_LOOKUP_URI = URI.create("https://api.hlrlookup.com/apiv2/hlr");

  private static final ObjectMapper OBJECT_MAPPER = SystemMapper.jsonMapper();

  private static final String REQUEST_COUNTER_NAME = MetricsUtil.name(HlrLookupCarrierDataProvider.class, "request");

  public HlrLookupCarrierDataProvider(final String apiKey,
      final String apiSecret,
      final Executor httpRequestExecutor,
      @Nullable final String circuitBreakerConfigurationName,
      @Nullable final String retryConfigurationName,
      final ScheduledExecutorService retryExecutor) {

    this(apiKey,
        apiSecret,
        FaultTolerantHttpClient.newBuilder("hlr-lookup", httpRequestExecutor)
            .withCircuitBreaker(circuitBreakerConfigurationName)
            .withRetry(retryConfigurationName, retryExecutor)
            .build(),
        HLR_LOOKUP_URI);
  }

  @VisibleForTesting
  HlrLookupCarrierDataProvider(final String apiKey,
      final String apiSecret,
      final FaultTolerantHttpClient httpClient,
      final URI lookupUri) {

    this.apiKey = apiKey;
    this.apiSecret = apiSecret;
    this.httpClient = httpClient;
    this.lookupUri = lookupUri;
  }

  @Override
  public Optional<CarrierData> lookupCarrierData(final Phonenumber.PhoneNumber phoneNumber,
      final Duration maxCachedAge) throws IOException, CarrierDataException {

    final HlrLookupResponse response;

    try {
      final String requestJson = OBJECT_MAPPER.writeValueAsString(
          new HlrLookupRequest(apiKey, apiSecret,
              List.of(TelephoneNumberRequest.forPhoneNumber(phoneNumber, maxCachedAge))));

      final HttpRequest request = HttpRequest
          .newBuilder(lookupUri)
          .POST(HttpRequest.BodyPublishers.ofString(requestJson))
          .header("Content-Type", "application/json")
          .build();

      final HttpResponse<String> httpResponse = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      if (httpResponse.statusCode() != 200) {
        // We may or may not have helpful data in the response body
        try {
          final HlrLookupResponse hlrLookupResponse = parseResponse(httpResponse.body());

          if (StringUtils.isNotBlank(hlrLookupResponse.error()) || StringUtils.isNotBlank(
              hlrLookupResponse.message())) {
            throw new CarrierDataException(
                "Received a non-success status code (%d): error = %s; message = %s".formatted(
                    httpResponse.statusCode(), hlrLookupResponse.error(), hlrLookupResponse.message()));
          }
        } catch (final JsonProcessingException _) {
          // We couldn't parse the body, so just move on with the default error message
        }

        throw new CarrierDataException("Received a non-success status code (%d)".formatted(httpResponse.statusCode()));
      }

      response = parseResponse(httpResponse.body());

      Metrics.counter(REQUEST_COUNTER_NAME, "status", String.valueOf(httpResponse.statusCode()))
          .increment();
    } catch (final IOException | CarrierDataException e) {
      Metrics.counter(REQUEST_COUNTER_NAME, "exception", e.getClass().getSimpleName())
          .increment();

      throw e;
    }

    if (response.results() == null || response.results().isEmpty()) {
      throw new CarrierDataException("No error reported, but no results provided");
    }

    final HlrLookupResult result = response.results().getFirst();

    if (!result.error().equals("NONE")) {
      throw new CarrierDataException("Received a per-number error: " + result.error());
    }

    return getNetworkDetails(result)
        .map(networkDetails -> new CarrierData(
            networkDetails.name(),
            lineType(result.telephoneNumberType()),
            mccFromMccMnc(networkDetails.mccmnc()),
            mncFromMccMnc(networkDetails.mccmnc())));
  }

  @VisibleForTesting
  static Optional<String> mccFromMccMnc(@Nullable final String mccMnc) {
    // MCCs are always 3 digits
    return Optional.ofNullable(StringUtils.stripToNull(mccMnc))
        .map(trimmedMccMnc -> trimmedMccMnc.substring(0, 3));
  }

  @VisibleForTesting
  static Optional<String> mncFromMccMnc(@Nullable final String mccMnc) {
    // MNCs may be 2 or 3 digits, but always come after a 3-digit MCC
    return Optional.ofNullable(StringUtils.stripToNull(mccMnc))
        .map(trimmedMccMnc -> trimmedMccMnc.substring(3));
  }

  @VisibleForTesting
  static CarrierData.LineType lineType(@Nullable final String telephoneNumberType) {
    if (telephoneNumberType == null) {
      return CarrierData.LineType.UNKNOWN;
    }

    return switch (telephoneNumberType) {
      case "MOBILE" -> CarrierData.LineType.MOBILE;
      case "LANDLINE", "MOBILE_OR_LANDLINE" -> CarrierData.LineType.LANDLINE;
      case "VOIP" -> CarrierData.LineType.NON_FIXED_VOIP;
      case "UNKNOWN" -> CarrierData.LineType.UNKNOWN;
      default -> CarrierData.LineType.OTHER;
    };
  }

  @VisibleForTesting
  static Optional<NetworkDetails> getNetworkDetails(final HlrLookupResult hlrLookupResult) {
    if (hlrLookupResult.currentNetwork().equals("AVAILABLE")) {
      return Optional.of(hlrLookupResult.currentNetworkDetails());
    } else if (hlrLookupResult.originalNetwork().equals("AVAILABLE")) {
      return Optional.of(hlrLookupResult.originalNetworkDetails());
    }

    return Optional.empty();
  }

  @VisibleForTesting
  static HlrLookupResponse parseResponse(final String responseJson) throws JsonProcessingException {
    return OBJECT_MAPPER.readValue(responseJson, HlrLookupResponse.class);
  }
}
