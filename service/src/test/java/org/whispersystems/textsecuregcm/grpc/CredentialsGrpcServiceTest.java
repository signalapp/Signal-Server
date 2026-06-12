/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.whispersystems.textsecuregcm.grpc.GrpcTestUtils.assertRateLimitExceeded;
import static org.whispersystems.textsecuregcm.grpc.GrpcTestUtils.assertStatusException;

import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.signal.chat.credentials.CredentialsGrpc;
import org.signal.chat.credentials.ExternalServiceType;
import org.signal.chat.credentials.GetCreateCallLinkCredentialsRequest;
import org.signal.chat.credentials.GetCreateCallLinkCredentialsResponse;
import org.signal.chat.credentials.GetDeliveryCertificateRequest;
import org.signal.chat.credentials.GetDeliveryCertificateResponse;
import org.signal.chat.credentials.GetExternalServiceCredentialsRequest;
import org.signal.chat.credentials.GetExternalServiceCredentialsResponse;
import org.signal.chat.credentials.GetGroupCredentialsRequest;
import org.signal.chat.credentials.GetGroupCredentialsResponse;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.InvalidKeyException;
import org.signal.libsignal.protocol.ServiceId;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.signal.libsignal.protocol.ecc.ECPublicKey;
import org.signal.libsignal.zkgroup.GenericServerSecretParams;
import org.signal.libsignal.zkgroup.ServerSecretParams;
import org.signal.libsignal.zkgroup.auth.AuthCredentialWithPniResponse;
import org.signal.libsignal.zkgroup.auth.ClientZkAuthOperations;
import org.signal.libsignal.zkgroup.auth.ServerZkAuthOperations;
import org.signal.libsignal.zkgroup.calllinks.CallLinkAuthCredentialResponse;
import org.signal.libsignal.zkgroup.calllinks.CreateCallLinkCredentialRequestContext;
import org.whispersystems.textsecuregcm.auth.CertificateGenerator;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.controllers.CertificateController;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.util.MockUtils;
import org.whispersystems.textsecuregcm.util.TestClock;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import reactor.core.publisher.Mono;

public class CredentialsGrpcServiceTest
    extends SimpleBaseGrpcTest<CredentialsGrpcService, CredentialsGrpc.CredentialsBlockingStub> {

  @Mock
  private AccountsManager accountsManager;

  @Mock
  private RateLimiters rateLimiters;

  @Mock
  private Account authenticatedAccount;

  private static final ExternalServiceCredentialsGenerator DIRECTORY_CREDENTIALS_GENERATOR = Mockito.spy(ExternalServiceCredentialsGenerator
      .builder(TestRandomUtil.nextBytes(32))
      .withUserDerivationKey(TestRandomUtil.nextBytes(32))
      .prependUsername(false)
      .truncateSignature(false)
      .build());

  private static final ExternalServiceCredentialsGenerator PAYMENTS_CREDENTIALS_GENERATOR = Mockito.spy(ExternalServiceCredentialsGenerator
      .builder(TestRandomUtil.nextBytes(32))
      .prependUsername(true)
      .build());

  private static final UUID AUTHENTICATED_PNI = UUID.randomUUID();
  private static final String PHONE_NUMBER = PhoneNumberUtil.getInstance().format(
      PhoneNumberUtil.getInstance().getExampleNumber("US"), PhoneNumberUtil.PhoneNumberFormat.E164);

  private static ECKeyPair IDENTITY_KEY_PAIR;

  private static ECKeyPair SERVER_KEY_PAIR;
  private static MessageProtos.ServerCertificate SIGNED_SERVER_CERTIFICATE;

  private static ServerSecretParams SERVER_SECRET_PARAMS;
  private static GenericServerSecretParams GENERIC_SERVER_SECRET_PARAMS;
  private static ServerZkAuthOperations SERVER_ZK_AUTH_OPERATIONS;

  private static final Clock CLOCK = TestClock.pinned(Instant.now());

  @BeforeAll
  static void setUpBeforeAll() {
    IDENTITY_KEY_PAIR = ECKeyPair.generate();

    final ECKeyPair caKeyPair = ECKeyPair.generate();
    SERVER_KEY_PAIR = ECKeyPair.generate();

    final MessageProtos.ServerCertificate.Certificate serverCertificate =
        MessageProtos.ServerCertificate.Certificate.newBuilder()
            .setId(ThreadLocalRandom.current().nextInt())
            .setKey(ByteString.copyFrom(SERVER_KEY_PAIR.getPublicKey().serialize()))
            .build();

    final byte[] signature = caKeyPair.getPrivateKey().calculateSignature(serverCertificate.toByteArray());

    SIGNED_SERVER_CERTIFICATE = MessageProtos.ServerCertificate.newBuilder()
        .setCertificate(ByteString.copyFrom(serverCertificate.toByteArray()))
        .setSignature(ByteString.copyFrom(signature))
        .build();

    SERVER_SECRET_PARAMS = ServerSecretParams.generate();
    GENERIC_SERVER_SECRET_PARAMS = GenericServerSecretParams.generate();
    SERVER_ZK_AUTH_OPERATIONS = new ServerZkAuthOperations(SERVER_SECRET_PARAMS);
  }

  @BeforeEach
  void setUp() {
    when(authenticatedAccount.getUuid()).thenReturn(AUTHENTICATED_ACI);
    when(authenticatedAccount.getNumber()).thenReturn(PHONE_NUMBER);
    when(authenticatedAccount.getIdentifier(IdentityType.ACI)).thenReturn(AUTHENTICATED_ACI);
    when(authenticatedAccount.getIdentifier(IdentityType.PNI)).thenReturn(AUTHENTICATED_PNI);
    when(authenticatedAccount.getIdentityKey(IdentityType.ACI))
        .thenReturn(new IdentityKey(IDENTITY_KEY_PAIR.getPublicKey()));

    when(accountsManager.getByAccountIdentifier(AUTHENTICATED_ACI)).thenReturn(Optional.of(authenticatedAccount));
  }

  @Override
  protected CredentialsGrpcService createServiceBeforeEachTest() {
    final CertificateGenerator certificateGenerator;
    try {
      certificateGenerator = new CertificateGenerator(SIGNED_SERVER_CERTIFICATE.toByteArray(),
          SERVER_KEY_PAIR.getPrivateKey(),
          1,
          false);
    } catch (final InvalidProtocolBufferException e) {
      throw new UncheckedIOException(e);
    }

    return new CredentialsGrpcService(accountsManager,
        certificateGenerator,
        SERVER_ZK_AUTH_OPERATIONS,
        GENERIC_SERVER_SECRET_PARAMS,
        rateLimiters,
        CLOCK,
        Map.of(
            ExternalServiceType.EXTERNAL_SERVICE_TYPE_DIRECTORY, DIRECTORY_CREDENTIALS_GENERATOR,
            ExternalServiceType.EXTERNAL_SERVICE_TYPE_PAYMENTS, PAYMENTS_CREDENTIALS_GENERATOR
        ));
  }

  static Stream<Arguments> testSuccess() {
    return Stream.of(
        Arguments.of(ExternalServiceType.EXTERNAL_SERVICE_TYPE_DIRECTORY, DIRECTORY_CREDENTIALS_GENERATOR),
        Arguments.of(ExternalServiceType.EXTERNAL_SERVICE_TYPE_PAYMENTS, PAYMENTS_CREDENTIALS_GENERATOR)
    );
  }

  @ParameterizedTest
  @MethodSource
  public void testSuccess(
      final ExternalServiceType externalServiceType,
      final ExternalServiceCredentialsGenerator credentialsGenerator) {
    final RateLimiter limiter = mock(RateLimiter.class);
    doReturn(limiter).when(rateLimiters).forDescriptor(eq(RateLimiters.For.EXTERNAL_SERVICE_CREDENTIALS));
    doReturn(Mono.fromFuture(CompletableFuture.completedFuture(null))).when(limiter).validateReactive(eq(AUTHENTICATED_ACI));
    final GetExternalServiceCredentialsResponse artResponse = authenticatedServiceStub().getExternalServiceCredentials(
        GetExternalServiceCredentialsRequest.newBuilder()
            .setExternalService(externalServiceType)
            .build());
    final Optional<Long> artValidation = credentialsGenerator.validateAndGetTimestamp(
        new ExternalServiceCredentials(artResponse.getUsername(), artResponse.getPassword()));
    assertTrue(artValidation.isPresent());
  }

  @ParameterizedTest
  @ValueSource(ints = { -1, 0, 1000 })
  public void testUnrecognizedService(final int externalServiceTypeValue) throws Exception {
    assertStatusException(Status.INVALID_ARGUMENT, () -> authenticatedServiceStub().getExternalServiceCredentials(
        GetExternalServiceCredentialsRequest.newBuilder()
            .setExternalServiceValue(externalServiceTypeValue)
            .build()));
  }

  @Test
  public void testInvalidRequest() throws Exception {
    assertStatusException(Status.INVALID_ARGUMENT, () -> authenticatedServiceStub().getExternalServiceCredentials(
        GetExternalServiceCredentialsRequest.newBuilder()
            .build()));
  }

  @Test
  public void testRateLimitExceeded() {
    final Duration retryAfter = MockUtils.updateRateLimiterResponseToFail(
        rateLimiters, RateLimiters.For.EXTERNAL_SERVICE_CREDENTIALS, AUTHENTICATED_ACI, Duration.ofSeconds(100));
    Mockito.reset(DIRECTORY_CREDENTIALS_GENERATOR);
    assertRateLimitExceeded(
        retryAfter,
        () -> authenticatedServiceStub().getExternalServiceCredentials(
            GetExternalServiceCredentialsRequest.newBuilder()
                .setExternalService(ExternalServiceType.EXTERNAL_SERVICE_TYPE_DIRECTORY)
                .build()),
        DIRECTORY_CREDENTIALS_GENERATOR
    );
  }

  /**
   * `ExternalServiceDefinitions` enum is supposed to have entries for all values in `ExternalServiceType`,
   * except for the `EXTERNAL_SERVICE_TYPE_UNSPECIFIED` and `UNRECOGNIZED`.
   * This test makes sure that is the case.
   */
  @ParameterizedTest
  @EnumSource(mode = EnumSource.Mode.EXCLUDE, names = { "UNRECOGNIZED", "EXTERNAL_SERVICE_TYPE_UNSPECIFIED" })
  public void testHaveExternalServiceDefinitionForServiceTypes(final ExternalServiceType externalServiceType) throws Exception {
    assertTrue(
        Arrays.stream(ExternalServiceDefinitions.values()).anyMatch(v -> v.externalService() == externalServiceType),
        "`ExternalServiceDefinitions` enum entry is missing for the `%s` value of `ExternalServiceType`".formatted(externalServiceType)
    );
  }

  @Test
  void getDeliveryCertificate() throws InvalidProtocolBufferException, InvalidKeyException {
    final GetDeliveryCertificateResponse response =
        authenticatedServiceStub().getDeliveryCertificate(GetDeliveryCertificateRequest.getDefaultInstance());

    checkDeliveryCertificate(response.getCertificateWithE164().toByteArray(), true);
    checkDeliveryCertificate(response.getCertificateWithoutE164().toByteArray(), false);
  }

  private static void checkDeliveryCertificate(final byte[] deliveryCertificate, final boolean expectE164)
      throws InvalidProtocolBufferException, InvalidKeyException {
    final MessageProtos.SenderCertificate senderCertificateHolder =
        MessageProtos.SenderCertificate.parseFrom(deliveryCertificate);

    final MessageProtos.SenderCertificate.Certificate senderCertificate =
        MessageProtos.SenderCertificate.Certificate.parseFrom(senderCertificateHolder.getCertificate());

    final MessageProtos.ServerCertificate.Certificate serverCertificate =
        MessageProtos.ServerCertificate.Certificate.parseFrom(SIGNED_SERVER_CERTIFICATE.getCertificate());

    assertEquals(serverCertificate.getId(), senderCertificate.getSignerId());

    final ECPublicKey serverPublicKey = new ECPublicKey(serverCertificate.getKey().toByteArray());

    assertTrue(serverPublicKey.verifySignature(senderCertificateHolder.getCertificate().toByteArray(),
        senderCertificateHolder.getSignature().toByteArray()));

    assertEquals(expectE164, senderCertificate.hasSenderE164());

    if (expectE164) {
      assertEquals(PHONE_NUMBER, senderCertificate.getSenderE164());
    }

    assertEquals(AUTHENTICATED_DEVICE_ID, senderCertificate.getSenderDevice());
    assertTrue(senderCertificate.hasSenderUuid());
    assertEquals(UUIDUtil.toByteString(AUTHENTICATED_ACI), senderCertificate.getSenderUuid());
    assertArrayEquals(senderCertificate.getIdentityKey().toByteArray(),
        new IdentityKey(IDENTITY_KEY_PAIR.getPublicKey()).serialize());
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 3, 4, 5, 6, 7})
  void getGroupCredentials(final int redemptionEndOffsetDays) {
    final Instant startOfDay = CLOCK.instant().truncatedTo(ChronoUnit.DAYS);

    final GetGroupCredentialsResponse response =
        authenticatedServiceStub().getGroupCredentials(GetGroupCredentialsRequest.newBuilder()
            .setRedemptionStartSeconds(startOfDay.getEpochSecond())
            .setRedemptionEndSeconds(startOfDay.plus(Duration.ofDays(redemptionEndOffsetDays)).getEpochSecond())
            .build());

    assertEquals(AUTHENTICATED_PNI, UUIDUtil.fromByteString(response.getPni()));
    assertEquals(redemptionEndOffsetDays + 1, response.getGroupCredentialsCount());
    assertEquals(redemptionEndOffsetDays + 1, response.getCallLinkAuthCredentialsCount());

    final ClientZkAuthOperations clientZkAuthOperations =
        new ClientZkAuthOperations(SERVER_SECRET_PARAMS.getPublicParams());

    for (int i = 0; i < redemptionEndOffsetDays; i++) {
      final Instant redemptionTime = startOfDay.plus(Duration.ofDays(i));
      assertEquals(redemptionTime.getEpochSecond(), response.getGroupCredentials(i).getRedemptionTimeSeconds());
      assertEquals(redemptionTime.getEpochSecond(), response.getCallLinkAuthCredentials(i).getRedemptionTimeSeconds());

      final int index = i;

      assertDoesNotThrow(() -> {
        clientZkAuthOperations.receiveAuthCredentialWithPniAsServiceId(
            new ServiceId.Aci(AUTHENTICATED_ACI),
            new ServiceId.Pni(AUTHENTICATED_PNI),
            redemptionTime.getEpochSecond(),
            new AuthCredentialWithPniResponse(response.getGroupCredentials(index).getCredential().toByteArray()));
      });

      assertDoesNotThrow(() -> {
        new CallLinkAuthCredentialResponse(response.getCallLinkAuthCredentials(index).getCredential().toByteArray())
            .receive(new ServiceId.Aci(AUTHENTICATED_ACI), redemptionTime, GENERIC_SERVER_SECRET_PARAMS.getPublicParams());
      });
    }
  }

  @ParameterizedTest
  @MethodSource
  void getGroupCredentialsIllegalRedemptionTimes(final Instant redemptionStart, final Instant redemptionEnd) {
    //noinspection ThrowableNotThrown
    GrpcTestUtils.assertStatusException(Status.INVALID_ARGUMENT, () -> {
      //noinspection ResultOfMethodCallIgnored
      authenticatedServiceStub().getGroupCredentials(GetGroupCredentialsRequest.newBuilder()
          .setRedemptionStartSeconds(redemptionStart.getEpochSecond())
          .setRedemptionEndSeconds(redemptionEnd.getEpochSecond())
          .build());
    });
  }

  private static Collection<Arguments> getGroupCredentialsIllegalRedemptionTimes() {
    final Instant startOfDay = CLOCK.instant().truncatedTo(ChronoUnit.DAYS);
    
    return List.of(
        Arguments.argumentSet("Start is after end", startOfDay.plus(Duration.ofDays(1)), startOfDay),
        Arguments.argumentSet("Start is in the past", startOfDay.minus(Duration.ofDays(2)), startOfDay),
        Arguments.argumentSet("End is too far in the future", startOfDay,
            startOfDay.plus(CertificateController.MAX_REDEMPTION_DURATION).plus(Duration.ofDays(1))),
        Arguments.argumentSet("Start is not at a day boundary", startOfDay.plusSeconds(17),
            startOfDay.plus(Duration.ofDays(1))),
        Arguments.argumentSet("End is not at a day boundary", startOfDay, startOfDay.plusSeconds(17))
    );
  }

  @Test
  void getCreateCallLinkCredentials() {
    when(rateLimiters.getCreateCallLinkLimiter()).thenReturn(mock(RateLimiter.class));

    final byte[] roomId = TestRandomUtil.nextBytes(32);

    final GetCreateCallLinkCredentialsResponse response =
        authenticatedServiceStub().getCreateCallLinkCredentials(GetCreateCallLinkCredentialsRequest.newBuilder()
            .setCredentialRequest(
                ByteString.copyFrom(CreateCallLinkCredentialRequestContext.forRoom(roomId).getRequest().serialize()))
            .build());

    assertFalse(response.getCredential().isEmpty());
    assertEquals(CLOCK.instant().truncatedTo(ChronoUnit.DAYS).getEpochSecond(), response.getRedemptionTimeSeconds());
  }

  @Test
  void getCreateCallLinkCredentialsRateLimited() throws RateLimitExceededException {
    final Duration retryAfter = Duration.ofSeconds(71);

    final RateLimiter rateLimiter = mock(RateLimiter.class);
    doThrow(new RateLimitExceededException(retryAfter)).when(rateLimiter).validate(any(UUID.class));

    when(rateLimiters.getCreateCallLinkLimiter()).thenReturn(rateLimiter);

    //noinspection ResultOfMethodCallIgnored
    GrpcTestUtils.assertRateLimitExceeded(retryAfter,
        () -> authenticatedServiceStub().getCreateCallLinkCredentials(GetCreateCallLinkCredentialsRequest.newBuilder()
            .setCredentialRequest(ByteString.copyFrom(TestRandomUtil.nextBytes(32)))
            .build()));
  }

  @Test
  void getCreateCallLinkCredentialsInvalidRequest() {
    when(rateLimiters.getCreateCallLinkLimiter()).thenReturn(mock(RateLimiter.class));

    //noinspection ThrowableNotThrown,ResultOfMethodCallIgnored
    GrpcTestUtils.assertStatusException(Status.INVALID_ARGUMENT,
        () -> authenticatedServiceStub().getCreateCallLinkCredentials(GetCreateCallLinkCredentialsRequest.newBuilder()
            .setCredentialRequest(ByteString.copyFrom(TestRandomUtil.nextBytes(16)))
            .build()));
  }
}
