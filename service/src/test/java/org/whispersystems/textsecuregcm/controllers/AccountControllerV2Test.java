/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.i18n.phonenumbers.PhoneNumberUtil;
import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.http.HttpStatus;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.InvalidRegistrationSessionException;
import org.whispersystems.textsecuregcm.auth.RecoveryPasswordVerificationFailedException;
import org.whispersystems.textsecuregcm.auth.UnverifiedRegistrationSessionException;
import org.whispersystems.textsecuregcm.entities.AccountDataReportResponse;
import org.whispersystems.textsecuregcm.entities.AccountIdentityResponse;
import org.whispersystems.textsecuregcm.entities.ChangeNumberRequest;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.entities.PhoneNumberDiscoverabilityRequest;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.mappers.ImpossiblePhoneNumberExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.NonNormalizedPhoneNumberExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.RateLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.push.MessageTooLargeException;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.ChangeNumberManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.tests.util.AccountsTestHelper;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.tests.util.KeysHelper;
import org.whispersystems.textsecuregcm.util.SystemMapper;

@ExtendWith(DropwizardExtensionsSupport.class)
class AccountControllerV2Test {

  private static final ECKeyPair IDENTITY_KEY_PAIR = ECKeyPair.generate();
  private static final IdentityKey IDENTITY_KEY = new IdentityKey(IDENTITY_KEY_PAIR.getPublicKey());

  private static final String NEW_NUMBER = PhoneNumberUtil.getInstance().format(
      PhoneNumberUtil.getInstance().getExampleNumber("US"),
      PhoneNumberUtil.PhoneNumberFormat.E164);

  private final AccountsManager accountsManager = mock(AccountsManager.class);
  private final ChangeNumberManager changeNumberManager = mock(ChangeNumberManager.class);

  private final ResourceExtension resources = ResourceExtension.builder()
      .addProperty(ServerProperties.UNWRAP_COMPLETION_STAGE_IN_WRITER_ENABLE, Boolean.TRUE)
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(new AuthValueFactoryProvider.Binder<>(AuthenticatedDevice.class))
      .addProvider(new RateLimitExceededExceptionMapper())
      .addProvider(new ImpossiblePhoneNumberExceptionMapper())
      .addProvider(new NonNormalizedPhoneNumberExceptionMapper())
      .setMapper(SystemMapper.jsonMapper())
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(new AccountControllerV2(accountsManager, changeNumberManager))
      .build();

  @Nested
  class ChangeNumber {

    @BeforeEach
    void setUp() throws Exception {
      reset(changeNumberManager);

      when(accountsManager.getByAccountIdentifier(AuthHelper.VALID_UUID)).thenReturn(Optional.of(AuthHelper.VALID_ACCOUNT));

      when(changeNumberManager.changeNumber(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any())).thenAnswer(
          (Answer<Account>) invocation -> {
            final UUID uuid = invocation.getArgument(0);
            final Account account = accountsManager.getByAccountIdentifier(uuid).orElseThrow();

            final String number = invocation.getArgument(4);
            final IdentityKey pniIdentityKey = invocation.getArgument(5);

            final List<Device> devices = account.getDevices();

            final Account updatedAccount = mock(Account.class);
            when(updatedAccount.getUuid()).thenReturn(uuid);
            when(updatedAccount.getNumber()).thenReturn(number);
            when(updatedAccount.getIdentityKey(IdentityType.PNI)).thenReturn(pniIdentityKey);
            if (number.equals(account.getNumber())) {
              when(updatedAccount.getPhoneNumberIdentifier()).thenReturn(AuthHelper.VALID_PNI);
            } else {
              when(updatedAccount.getPhoneNumberIdentifier()).thenReturn(UUID.randomUUID());
            }
            when(updatedAccount.getDevices()).thenReturn(devices);

            for (byte i = 1; i <= 3; i++) {
              final Optional<Device> d = account.getDevice(i);
              when(updatedAccount.getDevice(i)).thenReturn(d);
            }

            return updatedAccount;
          });
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void changeNumberSuccess(final boolean useSessionVerification) throws Exception {
      @Nullable final String sessionId = useSessionVerification ? encodeSessionId("session") : null;
      @Nullable final byte[] recoveryPassword = useSessionVerification ? null : "recovery-password".getBytes(StandardCharsets.UTF_8);

      final AccountIdentityResponse accountIdentityResponse =
          resources.getJerseyTest()
              .target("/v2/accounts/number")
              .request()
              .header(HttpHeaders.AUTHORIZATION,
                  AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
              .put(Entity.entity(
                  new ChangeNumberRequest(sessionId, recoveryPassword, NEW_NUMBER, "123", IDENTITY_KEY,
                      Collections.emptyList(),
                      Map.of(Device.PRIMARY_ID, KeysHelper.signedECPreKey(1, IDENTITY_KEY_PAIR)),
                      Map.of(Device.PRIMARY_ID, KeysHelper.signedKEMPreKey(2, IDENTITY_KEY_PAIR)),
                      Map.of(Device.PRIMARY_ID, 17)),
                  MediaType.APPLICATION_JSON_TYPE), AccountIdentityResponse.class);

      verify(changeNumberManager).changeNumber(eq(AuthHelper.VALID_UUID), any(), any(), any(), eq(NEW_NUMBER), any(),
          any(), any(), any(), any(), any(), any(), any());

      assertEquals(AuthHelper.VALID_UUID, accountIdentityResponse.uuid());
      assertEquals(NEW_NUMBER, accountIdentityResponse.number());
      assertNotEquals(AuthHelper.VALID_PNI, accountIdentityResponse.pni());
    }

    @Test
    void changeNumberNonNormalizedNumber() {
      try (Response response = resources.getJerseyTest()
          .target("/v2/accounts/number")
          .request()
          .header(HttpHeaders.AUTHORIZATION,
              AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
          .put(Entity.entity(
              // +4407700900111 is a valid number but not normalized - it has an optional '0' after the country code
              new ChangeNumberRequest(encodeSessionId("session"), null, "+4407700900111", null,
                  new IdentityKey(ECKeyPair.generate().getPublicKey()),
                  Collections.emptyList(),
                  Collections.emptyMap(), null, Collections.emptyMap()),
              MediaType.APPLICATION_JSON_TYPE))) {
        assertEquals(422, response.getStatus());
      }
    }

    @Test
    void unprocessableRequestJson() {
      final Invocation.Builder request = resources.getJerseyTest()
          .target("/v2/accounts/number")
          .request()
          .header(HttpHeaders.AUTHORIZATION,
              AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD));
      try (Response response = request.put(Entity.json(unprocessableJson()))) {
        assertEquals(400, response.getStatus());
      }
    }

    @Test
    void missingBasicAuthorization() {
      final Invocation.Builder request = resources.getJerseyTest()
          .target("/v2/accounts/number")
          .request();
      try (Response response = request.put(Entity.json(requestJson("sessionId", NEW_NUMBER)))) {
        assertEquals(401, response.getStatus());
      }
    }

    @Test
    void invalidBasicAuthorization() {
      final Invocation.Builder request = resources.getJerseyTest()
          .target("/v2/accounts/number")
          .request()
          .header(HttpHeaders.AUTHORIZATION, "Basic but-invalid");
      try (Response response = request.put(Entity.json(invalidRequestJson()))) {
        assertEquals(401, response.getStatus());
      }
    }

    @Test
    void invalidRequestBody() {
      final Invocation.Builder request = resources.getJerseyTest()
          .target("/v2/accounts/number")
          .request()
          .header(HttpHeaders.AUTHORIZATION,
              AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD));
      try (Response response = request.put(Entity.json(invalidRequestJson()))) {
        assertEquals(422, response.getStatus());
      }
    }

    @ParameterizedTest
    @MethodSource
    void invalidRegistrationId(final Integer pniRegistrationId, final int expectedStatusCode) {
      final ChangeNumberRequest changeNumberRequest = new ChangeNumberRequest(encodeSessionId("session"), null, NEW_NUMBER, "123", IDENTITY_KEY,
          Collections.emptyList(),
          Map.of(Device.PRIMARY_ID, KeysHelper.signedECPreKey(1, IDENTITY_KEY_PAIR)),
          Map.of(Device.PRIMARY_ID, KeysHelper.signedKEMPreKey(2, IDENTITY_KEY_PAIR)),
          Map.of(Device.PRIMARY_ID, pniRegistrationId));

      try (final Response response = resources.getJerseyTest()
          .target("/v2/accounts/number")
          .request()
          .header(HttpHeaders.AUTHORIZATION,
              AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
          .put(Entity.entity(changeNumberRequest, MediaType.APPLICATION_JSON_TYPE))) {

        assertEquals(expectedStatusCode, response.getStatus());
      }
    }

    private static Stream<Arguments> invalidRegistrationId() {
      return Stream.of(
          Arguments.of(0x3FFF, 200),
          Arguments.of(0, 422),
          Arguments.of(-1, 422),
          Arguments.of(0x3FFF + 1, 422),
          Arguments.of(Integer.MAX_VALUE, 422)
      );
    }

    @Test
    void rateLimitedNumber() throws Exception {
      doThrow(new RateLimitExceededException(null))
          .when(changeNumberManager).changeNumber(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

      final Invocation.Builder request = resources.getJerseyTest()
          .target("/v2/accounts/number")
          .request()
          .header(HttpHeaders.AUTHORIZATION,
              AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD));

      try (Response response = request.put(Entity.json(requestJson("sessionId", NEW_NUMBER)))) {
        assertEquals(429, response.getStatus());
      }
    }

    @ParameterizedTest
    @MethodSource
    void phoneVerificationException(final Exception exception, final int expectedStatus)
        throws Exception {
      doThrow(exception)
          .when(changeNumberManager).changeNumber(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

      final Invocation.Builder request = resources.getJerseyTest()
          .target("/v2/accounts/number")
          .request()
          .header(HttpHeaders.AUTHORIZATION,
              AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD));

      try (Response response = request.put(Entity.json(requestJson("sessionId", NEW_NUMBER)))) {
        assertEquals(expectedStatus, response.getStatus());
      }
    }

    private static List<Arguments> phoneVerificationException() {
      return List.of(
          Arguments.argumentSet("Bad request", new InvalidRegistrationSessionException("invalid registration session"), HttpStatus.SC_BAD_REQUEST),
          Arguments.argumentSet("Not authorized", new UnverifiedRegistrationSessionException(), HttpStatus.SC_UNAUTHORIZED),
          Arguments.argumentSet("Forbidden", new RecoveryPasswordVerificationFailedException(), HttpStatus.SC_FORBIDDEN),
          Arguments.argumentSet("Unexpected exception", new IOException("unavailable"), HttpStatus.SC_SERVICE_UNAVAILABLE)
      );
    }

    @Test
    void deviceMessageTooLarge() throws Exception {
      doThrow(MessageTooLargeException.class)
          .when(changeNumberManager).changeNumber(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

      try (final Response response = resources.getJerseyTest()
              .target("/v2/accounts/number")
              .request()
              .header(HttpHeaders.AUTHORIZATION,
                  AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
              .put(Entity.entity(
                  new ChangeNumberRequest(encodeSessionId("session"), null, NEW_NUMBER, "123", IDENTITY_KEY,
                      Collections.emptyList(),
                      Map.of(Device.PRIMARY_ID, KeysHelper.signedECPreKey(1, IDENTITY_KEY_PAIR)),
                      Map.of(Device.PRIMARY_ID, KeysHelper.signedKEMPreKey(2, IDENTITY_KEY_PAIR)),
                      Map.of(Device.PRIMARY_ID, 17)),
                  MediaType.APPLICATION_JSON_TYPE))) {

        assertEquals(413, response.getStatus());
      }
    }

    @Test
    void messageDeliveryNotAllowed() throws Exception {
      doThrow(MessageDeliveryNotAllowedException.class)
          .when(changeNumberManager).changeNumber(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

      try (final Response response = resources.getJerseyTest()
          .target("/v2/accounts/number")
          .request()
          .header(HttpHeaders.AUTHORIZATION,
              AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
          .put(Entity.entity(
              new ChangeNumberRequest(encodeSessionId("session"), null, NEW_NUMBER, "123", IDENTITY_KEY,
                  Collections.emptyList(),
                  Map.of(Device.PRIMARY_ID, KeysHelper.signedECPreKey(1, IDENTITY_KEY_PAIR)),
                  Map.of(Device.PRIMARY_ID, KeysHelper.signedKEMPreKey(2, IDENTITY_KEY_PAIR)),
                  Map.of(Device.PRIMARY_ID, 17)),
              MediaType.APPLICATION_JSON_TYPE))) {

        assertEquals(503, response.getStatus());
      }
    }

    /**
     * Valid request JSON with the give session ID and recovery password
     */
    private static String requestJson(final String sessionId,
        final byte[] recoveryPassword,
        final String newNumber,
        final Integer pniRegistrationId) {

      final ECSignedPreKey pniSignedPreKey = KeysHelper.signedECPreKey(1, IDENTITY_KEY_PAIR);
      final KEMSignedPreKey pniLastResortPreKey = KeysHelper.signedKEMPreKey(2, IDENTITY_KEY_PAIR);

      return String.format("""
          {
            "sessionId": "%s",
            "recoveryPassword": "%s",
            "number": "%s",
            "reglock": "1234",
            "pniIdentityKey": "%s",
            "deviceMessages": [],
            "devicePniSignedPrekeys": {"1": {"keyId": %d, "publicKey": "%s", "signature": "%s"}},
            "devicePniPqLastResortPrekeys": {"1": {"keyId": %d, "publicKey": "%s", "signature": "%s"}},
            "pniRegistrationIds": {"1": %d}
          }
          """, encodeSessionId(sessionId),
          encodeRecoveryPassword(recoveryPassword),
          newNumber,
          Base64.getEncoder().encodeToString(IDENTITY_KEY.serialize()),
          pniSignedPreKey.keyId(), Base64.getEncoder().encodeToString(pniSignedPreKey.serializedPublicKey()), Base64.getEncoder().encodeToString(pniSignedPreKey.signature()),
          pniLastResortPreKey.keyId(), Base64.getEncoder().encodeToString(pniLastResortPreKey.serializedPublicKey()), Base64.getEncoder().encodeToString(pniLastResortPreKey.signature()),
          pniRegistrationId);
    }

    /**
     * Valid request JSON with the give session ID
     */
    private static String requestJson(final String sessionId, final String newNumber) {
      return requestJson(sessionId, new byte[0], newNumber, 123);
    }

    /**
     * Request JSON in the shape of {@link org.whispersystems.textsecuregcm.entities.ChangeNumberRequest}, but that
     * fails validation
     */
    private static String invalidRequestJson() {
      return """
          {
            "sessionId": null
          }
          """;
    }

    /**
     * Request JSON that cannot be marshalled into
     * {@link org.whispersystems.textsecuregcm.entities.ChangeNumberRequest}
     */
    private static String unprocessableJson() {
      return """
          {
            "sessionId": []
          }
          """;
    }

    private static String encodeSessionId(final String sessionId) {
      return Base64.getUrlEncoder().encodeToString(sessionId.getBytes(StandardCharsets.UTF_8));
    }

    private static String encodeRecoveryPassword(final byte[] recoveryPassword) {
      return Base64.getEncoder().encodeToString(recoveryPassword);
    }
  }

  @Nested
  class PhoneNumberDiscoverability {

    @BeforeEach
    void setup() {
      AccountsHelper.setupMockUpdate(accountsManager);
      when(accountsManager.getByAccountIdentifier(AuthHelper.VALID_UUID)).thenReturn(Optional.of(AuthHelper.VALID_ACCOUNT));
    }

    @Test
    void testSetPhoneNumberDiscoverability() {
      Response response = resources.getJerseyTest()
          .target("/v2/accounts/phone_number_discoverability")
          .request()
          .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
          .put(Entity.json(new PhoneNumberDiscoverabilityRequest(true)));

      assertThat(response.getStatus()).isEqualTo(204);

      ArgumentCaptor<Boolean> discoverabilityCapture = ArgumentCaptor.forClass(Boolean.class);
      verify(AuthHelper.VALID_ACCOUNT).setDiscoverableByPhoneNumber(discoverabilityCapture.capture());
      assertThat(discoverabilityCapture.getValue()).isTrue();
    }

    @Test
    void testSetNullPhoneNumberDiscoverability() {
      Response response = resources.getJerseyTest()
          .target("/v2/accounts/phone_number_discoverability")
          .request()
          .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
          .put(Entity.json(
              """
                  {
                    "discoverableByPhoneNumber": null
                  }
                  """));

      assertThat(response.getStatus()).isEqualTo(422);
      verify(AuthHelper.VALID_ACCOUNT, never()).setDiscoverableByPhoneNumber(anyBoolean());
    }

    @ParameterizedTest
    @ArgumentsSource(AccountsTestHelper.AccountsDataReportArgumentProvider.class)
    void testGetAccountDataReport(final Account account, final String expectedTextAfterHeader) throws Exception {
      when(AuthHelper.ACCOUNTS_MANAGER.getByAccountIdentifier(account.getUuid())).thenReturn(Optional.of(account));
      when(accountsManager.getByAccountIdentifier(account.getUuid())).thenReturn(Optional.of(account));

      final Response response = resources.getJerseyTest()
          .target("/v2/accounts/data_report")
          .request()
          .header("Authorization", AuthHelper.getAuthHeader(account.getUuid(), "password"))
          .get();

      assertEquals(200, response.getStatus());

      final String stringResponse = response.readEntity(String.class);

      final AccountDataReportResponse structuredResponse = SystemMapper.jsonMapper()
          .readValue(stringResponse, AccountDataReportResponse.class);

      assertEquals(account.getNumber(), structuredResponse.data().account().phoneNumber());
      assertEquals(account.isDiscoverableByPhoneNumber(),
          structuredResponse.data().account().findAccountByPhoneNumber());
      assertEquals(account.isUnrestrictedUnidentifiedAccess(),
          structuredResponse.data().account().allowSealedSenderFromAnyone());

      final Set<Byte> deviceIds = account.getDevices().stream().map(Device::getId).collect(Collectors.toSet());

      // all devices should be present
      structuredResponse.data().devices().forEach(deviceDataReport -> {
        assertTrue(deviceIds.remove(deviceDataReport.id()));
        assertEquals(account.getDevice(deviceDataReport.id()).orElseThrow().getUserAgent(),
            deviceDataReport.userAgent());
      });
      assertTrue(deviceIds.isEmpty());

      final String actualText = (String) SystemMapper.jsonMapper().readValue(stringResponse, Map.class).get("text");
      AccountsTestHelper.verifyAccountDataReportText(actualText, expectedTextAfterHeader);
    }
  }
}
