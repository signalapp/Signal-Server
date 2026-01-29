/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.telephony.hlrlookup;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.http.FaultTolerantHttpClient;
import org.whispersystems.textsecuregcm.telephony.CarrierData;
import org.whispersystems.textsecuregcm.telephony.CarrierDataException;

class HlrLookupCarrierDataProviderTest {

  private HlrLookupCarrierDataProvider hlrLookupCarrierDataProvider;

  @RegisterExtension
  private static final WireMockExtension WIRE_MOCK_EXTENSION = WireMockExtension.newInstance()
      .options(wireMockConfig().dynamicPort().dynamicHttpsPort())
      .build();

  private static final String HLR_LOOKUP_PATH = "/hlr";

  @BeforeEach
  void setUp() {
    final FaultTolerantHttpClient faultTolerantHttpClient = FaultTolerantHttpClient.newBuilder("hlrLookupTest", Runnable::run)
        .build();

    hlrLookupCarrierDataProvider = new HlrLookupCarrierDataProvider("test", "test", faultTolerantHttpClient, URI.create("http://localhost:" + WIRE_MOCK_EXTENSION.getPort() + HLR_LOOKUP_PATH));
  }

  @Test
  void lookupCarrierData() throws IOException, CarrierDataException {
    final String responseJson = """
        {
            "results": [
                {
                    "error": "NONE",
                    "uuid": "f066f711-4043-4d54-847d-c273e6491881",
                    "request_parameters": {
                        "telephone_number": "+44(7790) 60 60 23",
                        "save_to_cache": "YES",
                        "input_format": "",
                        "output_format": "",
                        "cache_days_global": 0,
                        "cache_days_private": 0,
                        "get_ported_date": "NO",
                        "get_landline_status": "NO",
                        "usa_status": "NO"
                    },
                    "credits_spent": 1,
                    "detected_telephone_number": "447790606023",
                    "formatted_telephone_number": "",
                    "live_status": "LIVE",
                    "original_network": "AVAILABLE",
                    "original_network_details": {
                        "name": "EE Limited (Orange)",
                        "mccmnc": "23433",
                        "country_name": "United Kingdom",
                        "country_iso3": "GBR",
                        "area": "United Kingdom",
                        "country_prefix": "44"
                    },
                    "current_network": "AVAILABLE",
                    "current_network_details": {
                        "name": "Virgin Mobile",
                        "mccmnc": "23438",
                        "country_name": "United Kingdom",
                        "country_iso3": "GBR",
                        "country_prefix": "44"
                    },
                    "is_ported": "YES",
                    "timestamp": "2022-09-08T10:56:03Z",
                    "telephone_number_type": "MOBILE",
                    "sms_email": "",
                    "mms_email": ""
                }
            ]
        }
        """;

    WIRE_MOCK_EXTENSION.stubFor(post(urlEqualTo(HLR_LOOKUP_PATH))
        .willReturn(aResponse()
            .withHeader("Content-Type", "application/json")
            .withBody(responseJson)));

    final Optional<CarrierData> maybeCarrierData =
        hlrLookupCarrierDataProvider.lookupCarrierData(PhoneNumberUtil.getInstance().getExampleNumber("US"), Duration.ZERO);

    assertEquals(Optional.of(new CarrierData("Virgin Mobile", CarrierData.LineType.MOBILE, Optional.of("234"), Optional.of("38"), Optional.of(true))),
        maybeCarrierData);
  }

  @Test
  void lookupCarrierDataNonSuccessStatus() {
    WIRE_MOCK_EXTENSION.stubFor(post(urlEqualTo(HLR_LOOKUP_PATH))
        .willReturn(aResponse()
            .withStatus(500)));

    assertThrows(CarrierDataException.class, () ->
        hlrLookupCarrierDataProvider.lookupCarrierData(PhoneNumberUtil.getInstance().getExampleNumber("US"), Duration.ZERO));
  }

  @Test
  void lookupCarrierDataErrorMessage() {
    final String responseJson = """
        { "error": "UNAUTHORIZED", "message": "Invalid api_key or api_secret" }
        """;

    WIRE_MOCK_EXTENSION.stubFor(post(urlEqualTo(HLR_LOOKUP_PATH))
        .willReturn(aResponse()
            .withHeader("Content-Type", "application/json")
            .withBody(responseJson)));

    assertThrows(CarrierDataException.class, () ->
        hlrLookupCarrierDataProvider.lookupCarrierData(PhoneNumberUtil.getInstance().getExampleNumber("US"), Duration.ZERO));
  }

  @Test
  void lookupCarrierDataEmptyBody() {
    WIRE_MOCK_EXTENSION.stubFor(post(urlEqualTo(HLR_LOOKUP_PATH))
        .willReturn(aResponse()
            .withHeader("Content-Type", "application/json")
            .withBody("{}")));

    assertThrows(CarrierDataException.class, () ->
        hlrLookupCarrierDataProvider.lookupCarrierData(PhoneNumberUtil.getInstance().getExampleNumber("US"), Duration.ZERO));
  }

  @Test
  void lookupCarrierDataPerNumberError() {
    final String responseJson = """
        {
            "body": {
                "results": [
                    {
                        "error": "INTERNAL_ERROR"
                    }
                ]
            }
        }
        """;

    WIRE_MOCK_EXTENSION.stubFor(post(urlEqualTo(HLR_LOOKUP_PATH))
        .willReturn(aResponse()
            .withHeader("Content-Type", "application/json")
            .withBody(responseJson)));

    assertThrows(CarrierDataException.class, () ->
        hlrLookupCarrierDataProvider.lookupCarrierData(PhoneNumberUtil.getInstance().getExampleNumber("US"), Duration.ZERO));
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  @ParameterizedTest
  @MethodSource
  void mccFromMccMnc(@Nullable final String mccMnc, final Optional<String> expectedMcc) {
    assertEquals(expectedMcc, HlrLookupCarrierDataProvider.mccFromMccMnc(mccMnc));
  }

  private static List<Arguments> mccFromMccMnc() {
    return List.of(
        Arguments.argumentSet("Null mccMnc string", null, Optional.empty()),
        Arguments.argumentSet("Empty mccMnc string", "", Optional.empty()),
        Arguments.argumentSet("Blank mccMnc string", " ", Optional.empty()),
        Arguments.argumentSet("Two-digit MNC", "12345", Optional.of("123")),
        Arguments.argumentSet("Three-digit MNC", "123456", Optional.of("123"))
    );
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  @ParameterizedTest
  @MethodSource
  void mncFromMccMnc(@Nullable final String mccMnc, final Optional<String> expectedMnc) {
    assertEquals(expectedMnc, HlrLookupCarrierDataProvider.mncFromMccMnc(mccMnc));
  }

  private static List<Arguments> mncFromMccMnc() {
    return List.of(
        Arguments.argumentSet("Null mccMnc string", null, Optional.empty()),
        Arguments.argumentSet("Empty mccMnc string", "", Optional.empty()),
        Arguments.argumentSet("Blank mccMnc string", " ", Optional.empty()),
        Arguments.argumentSet("Two-digit MNC", "12345", Optional.of("45")),
        Arguments.argumentSet("Three-digit MNC", "123456", Optional.of("456"))
    );
  }

  @ParameterizedTest
  @MethodSource
  void lineType(@Nullable final String lineType, final CarrierData.LineType expectedLineType) {
    assertEquals(expectedLineType, HlrLookupCarrierDataProvider.lineType(lineType));
  }

  private static List<Arguments> lineType() {
    return List.of(
        Arguments.argumentSet("Null line type", null, CarrierData.LineType.UNKNOWN)
    );
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  @ParameterizedTest
  @MethodSource
  void isPorted(final String isPortedString, final Optional<Boolean> expectedIsPortedValue) {
    final HlrLookupResult hlrLookupResult =
        new HlrLookupResult("NONE", 1.0f, "NOT_AVAILABLE", null, "NOT_AVAILABLE", null, "MOBILE", isPortedString);

    assertEquals(expectedIsPortedValue, HlrLookupCarrierDataProvider.isPorted(hlrLookupResult.isPorted()));
  }

  private static List<Arguments> isPorted() {
    return List.of(
        Arguments.argumentSet("Null isPorted string", null, Optional.empty()),
        Arguments.argumentSet("Is ported", "YES", Optional.of(true)),
        Arguments.argumentSet("Is not ported", "NO", Optional.of(false)),
        Arguments.argumentSet("Unrecognized isPorted string", "UNKNOWN", Optional.empty())
    );
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  @ParameterizedTest
  @MethodSource
  void getNetworkDetails(final HlrLookupResult hlrLookupResult, final Optional<NetworkDetails> expectedNetworkDetails) {
    assertEquals(expectedNetworkDetails, HlrLookupCarrierDataProvider.getNetworkDetails(hlrLookupResult));
  }

  private static List<Arguments> getNetworkDetails() {
    final NetworkDetails originalNetwork = new NetworkDetails(
        "Original network",
        "123456",
        "United States of America",
        "USA",
        "United States of America",
        "1");

    final NetworkDetails currentNetwork = new NetworkDetails(
        "Current network",
        "654321",
        "United States of America",
        "USA",
        "United States of America",
        "1");

    return List.of(
        Arguments.argumentSet("Original and current network",
            resultWithNetworkDetails(originalNetwork, currentNetwork),
            Optional.of(currentNetwork)),

        Arguments.argumentSet("Original network only",
            resultWithNetworkDetails(originalNetwork, null),
            Optional.of(originalNetwork)),

        Arguments.argumentSet("Current network only",
            resultWithNetworkDetails(null, currentNetwork),
            Optional.of(currentNetwork)),

        Arguments.argumentSet("No network details",
            resultWithNetworkDetails(null, null),
            Optional.empty())
    );
  }

  private static HlrLookupResult resultWithNetworkDetails(@Nullable final NetworkDetails originalNetwork,
      @Nullable final NetworkDetails currentNetwork) {

    return new HlrLookupResult(null,
        1.0f,
        originalNetwork == null ? "NOT_AVAILABLE" : "AVAILABLE",
        originalNetwork,
        currentNetwork == null ? "NOT_AVAILABLE" : "AVAILABLE",
        currentNetwork,
        "MOBILE",
        "NO");
  }

  @Test
  void parseResponse() throws JsonProcessingException {
    final String json = """
        {
            "results": [
                {
                    "error": "NONE",
                    "uuid": "f066f711-4043-4d54-847d-c273e6491881",
                    "request_parameters": {
                        "telephone_number": "+44(7790) 60 60 23",
                        "save_to_cache": "YES",
                        "input_format": "",
                        "output_format": "",
                        "cache_days_global": 0,
                        "cache_days_private": 0,
                        "get_ported_date": "NO",
                        "get_landline_status": "NO",
                        "usa_status": "NO"
                    },
                    "credits_spent": 1,
                    "detected_telephone_number": "447790606023",
                    "formatted_telephone_number": "",
                    "live_status": "LIVE",
                    "original_network": "AVAILABLE",
                    "original_network_details": {
                        "name": "EE Limited (Orange)",
                        "mccmnc": "23433",
                        "country_name": "United Kingdom",
                        "country_iso3": "GBR",
                        "area": "United Kingdom",
                        "country_prefix": "44"
                    },
                    "current_network": "AVAILABLE",
                    "current_network_details": {
                        "name": "Virgin Mobile",
                        "mccmnc": "23438",
                        "country_name": "United Kingdom",
                        "country_iso3": "GBR",
                        "country_prefix": "44"
                    },
                    "is_ported": "YES",
                    "timestamp": "2022-09-08T10:56:03Z",
                    "telephone_number_type": "MOBILE",
                    "sms_email": "",
                    "mms_email": ""
                }
            ]
        }
        """;

    final HlrLookupResponse response = HlrLookupCarrierDataProvider.parseResponse(json);
    assertNull(response.error());
    assertNull(response.message());
    assertNotNull(response.results());
    assertEquals(1, response.results().size());

    final HlrLookupResult result = response.results().getFirst();
    assertEquals("NONE", result.error());
    assertEquals("MOBILE", result.telephoneNumberType());

    assertEquals("AVAILABLE", result.originalNetwork());
    assertEquals("EE Limited (Orange)", result.originalNetworkDetails().name());
    assertEquals("23433", result.originalNetworkDetails().mccmnc());
    assertEquals("United Kingdom", result.originalNetworkDetails().countryName());
    assertEquals("GBR", result.originalNetworkDetails().countryIso3());
    assertEquals("United Kingdom", result.originalNetworkDetails().area());
    assertEquals("44", result.originalNetworkDetails().countryPrefix());

    assertEquals("AVAILABLE", result.currentNetwork());
    assertEquals("Virgin Mobile", result.currentNetworkDetails().name());
    assertEquals("23438", result.currentNetworkDetails().mccmnc());
    assertEquals("United Kingdom", result.currentNetworkDetails().countryName());
    assertEquals("GBR", result.currentNetworkDetails().countryIso3());
    assertNull(result.currentNetworkDetails().area());
    assertEquals("44", result.currentNetworkDetails().countryPrefix());
  }

  @Test
  void parseResponseError() throws JsonProcessingException {
    final String json = """
        { "error": "UNAUTHORIZED", "message": "Invalid api_key or api_secret" }
        """;

    final HlrLookupResponse response = HlrLookupCarrierDataProvider.parseResponse(json);
    assertEquals("UNAUTHORIZED", response.error());
    assertEquals("Invalid api_key or api_secret", response.message());
    assertNull(response.results());
  }
}
