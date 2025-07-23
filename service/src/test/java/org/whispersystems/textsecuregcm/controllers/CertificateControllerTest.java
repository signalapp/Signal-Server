/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.signal.libsignal.protocol.InvalidKeyException;
import org.signal.libsignal.protocol.ServiceId;
import org.signal.libsignal.protocol.ecc.ECPrivateKey;
import org.signal.libsignal.protocol.ecc.ECPublicKey;
import org.signal.libsignal.zkgroup.GenericServerSecretParams;
import org.signal.libsignal.zkgroup.ServerSecretParams;
import org.signal.libsignal.zkgroup.auth.AuthCredentialWithPniResponse;
import org.signal.libsignal.zkgroup.auth.ClientZkAuthOperations;
import org.signal.libsignal.zkgroup.auth.ServerZkAuthOperations;
import org.signal.libsignal.zkgroup.calllinks.CallLinkAuthCredentialResponse;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.CertificateGenerator;
import org.whispersystems.textsecuregcm.entities.DeliveryCertificate;
import org.whispersystems.textsecuregcm.entities.GroupCredentials;
import org.whispersystems.textsecuregcm.entities.MessageProtos.SenderCertificate;
import org.whispersystems.textsecuregcm.entities.MessageProtos.ServerCertificate;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.HeaderUtils;
import org.whispersystems.textsecuregcm.util.SystemMapper;

@ExtendWith(DropwizardExtensionsSupport.class)
class CertificateControllerTest {

  private static final ECPublicKey caPublicKey;

  static {
    try {
      caPublicKey = new ECPublicKey(Base64.getDecoder().decode("BWh+UOhT1hD8bkb+MFRvb6tVqhoG8YYGCzOd7mgjo8cV"));
    } catch (InvalidKeyException e) {
      throw new AssertionError("Statically-defined key was invalid", e);
    }
  }

  @SuppressWarnings("unused")
  private static final String caPrivateKey = "EO3Mnf0kfVlVnwSaqPoQnAxhnnGL1JTdXqktCKEe9Eo=";

  private static final String signingCertificate = "CiUIDBIhBbTz4h1My+tt+vw+TVscgUe/DeHS0W02tPWAWbTO2xc3EkD+go4bJnU0AcnFfbOLKoiBfCzouZtDYMOVi69rE7r4U9cXREEqOkUmU2WJBjykAxWPCcSTmVTYHDw7hkSp/puG";
  private static final String signingKey = "ABOxG29xrfq4E7IrW11Eg7+HBbtba9iiS0500YoBjn4=";

  private static final ServerSecretParams serverSecretParams = ServerSecretParams.generate();

  private static final GenericServerSecretParams genericServerSecretParams = GenericServerSecretParams.generate();
  private static final CertificateGenerator certificateGenerator;
  private static final ServerZkAuthOperations serverZkAuthOperations;
  private static final Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());

  private static final AccountsManager accountsManager = mock(AccountsManager.class);

  static {
    try {
      certificateGenerator = new CertificateGenerator(Base64.getDecoder().decode(signingCertificate),
          new ECPrivateKey(Base64.getDecoder().decode(signingKey)), 1);
      serverZkAuthOperations = new ServerZkAuthOperations(serverSecretParams);
    } catch (IOException | InvalidKeyException e) {
      throw new AssertionError(e);
    }
  }


  private static final ResourceExtension resources = ResourceExtension.builder()
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(new AuthValueFactoryProvider.Binder<>(AuthenticatedDevice.class))
      .setMapper(SystemMapper.jsonMapper())
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(new CertificateController(accountsManager, certificateGenerator, serverZkAuthOperations, genericServerSecretParams, clock))
      .build();

  @BeforeEach
  void setUp() {
    when(accountsManager.getByAccountIdentifier(AuthHelper.VALID_UUID)).thenReturn(Optional.of(AuthHelper.VALID_ACCOUNT));
  }

  @Test
  void testValidCertificate() throws Exception {
    DeliveryCertificate certificateObject = resources.getJerseyTest()
        .target("/v1/certificate/delivery")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(DeliveryCertificate.class);

    SenderCertificate certificateHolder = SenderCertificate.parseFrom(certificateObject.getCertificate());
    SenderCertificate.Certificate certificate = SenderCertificate.Certificate.parseFrom(
        certificateHolder.getCertificate());

    ServerCertificate serverCertificateHolder = certificate.getSigner();
    ServerCertificate.Certificate serverCertificate = ServerCertificate.Certificate.parseFrom(
        serverCertificateHolder.getCertificate());
    ECPublicKey serverPublicKey = new ECPublicKey(serverCertificate.getKey().toByteArray());

    assertTrue(serverPublicKey.verifySignature(
        certificateHolder.getCertificate().toByteArray(), certificateHolder.getSignature().toByteArray()));
    assertTrue(caPublicKey.verifySignature(serverCertificateHolder.getCertificate().toByteArray(),
        serverCertificateHolder.getSignature().toByteArray()));

    assertEquals(certificate.getSender(), AuthHelper.VALID_NUMBER);
    assertEquals(certificate.getSenderDevice(), 1L);
    assertTrue(certificate.hasSenderUuid());
    assertEquals(AuthHelper.VALID_UUID.toString(), certificate.getSenderUuid());
    assertArrayEquals(certificate.getIdentityKey().toByteArray(), AuthHelper.VALID_IDENTITY.serialize());
  }

  @Test
  void testValidCertificateWithUuid() throws Exception {
    DeliveryCertificate certificateObject = resources.getJerseyTest()
        .target("/v1/certificate/delivery")
        .queryParam("includeUuid", "true")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(DeliveryCertificate.class);

    SenderCertificate certificateHolder = SenderCertificate.parseFrom(certificateObject.getCertificate());
    SenderCertificate.Certificate certificate = SenderCertificate.Certificate.parseFrom(
        certificateHolder.getCertificate());

    ServerCertificate serverCertificateHolder = certificate.getSigner();
    ServerCertificate.Certificate serverCertificate = ServerCertificate.Certificate.parseFrom(
        serverCertificateHolder.getCertificate());
    ECPublicKey serverPublicKey = new ECPublicKey(serverCertificate.getKey().toByteArray());

    assertTrue(serverPublicKey.verifySignature(certificateHolder.getCertificate().toByteArray(),
        certificateHolder.getSignature().toByteArray()));
    assertTrue(caPublicKey.verifySignature(serverCertificateHolder.getCertificate().toByteArray(),
        serverCertificateHolder.getSignature().toByteArray()));

    assertEquals(certificate.getSender(), AuthHelper.VALID_NUMBER);
    assertEquals(certificate.getSenderDevice(), 1L);
    assertEquals(certificate.getSenderUuid(), AuthHelper.VALID_UUID.toString());
    assertArrayEquals(certificate.getIdentityKey().toByteArray(), AuthHelper.VALID_IDENTITY.serialize());
  }

  @Test
  void testValidCertificateWithUuidNoE164() throws Exception {
    DeliveryCertificate certificateObject = resources.getJerseyTest()
        .target("/v1/certificate/delivery")
        .queryParam("includeUuid", "true")
        .queryParam("includeE164", "false")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(DeliveryCertificate.class);

    SenderCertificate certificateHolder = SenderCertificate.parseFrom(certificateObject.getCertificate());
    SenderCertificate.Certificate certificate = SenderCertificate.Certificate.parseFrom(
        certificateHolder.getCertificate());

    ServerCertificate serverCertificateHolder = certificate.getSigner();
    ServerCertificate.Certificate serverCertificate = ServerCertificate.Certificate.parseFrom(
        serverCertificateHolder.getCertificate());
    ECPublicKey serverPublicKey = new ECPublicKey(serverCertificate.getKey().toByteArray());

    assertTrue(serverPublicKey.verifySignature(certificateHolder.getCertificate().toByteArray(),
        certificateHolder.getSignature().toByteArray()));
    assertTrue(caPublicKey.verifySignature(serverCertificateHolder.getCertificate().toByteArray(),
        serverCertificateHolder.getSignature().toByteArray()));

    assertTrue(StringUtils.isBlank(certificate.getSender()));
    assertEquals(certificate.getSenderDevice(), 1L);
    assertEquals(certificate.getSenderUuid(), AuthHelper.VALID_UUID.toString());
    assertArrayEquals(certificate.getIdentityKey().toByteArray(), AuthHelper.VALID_IDENTITY.serialize());
  }

  @Test
  void testBadAuthentication() {
    Response response = resources.getJerseyTest()
        .target("/v1/certificate/delivery")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.INVALID_PASSWORD))
        .get();

    assertEquals(response.getStatus(), 401);
  }


  @Test
  void testNoAuthentication() {
    Response response = resources.getJerseyTest()
        .target("/v1/certificate/delivery")
        .request()
        .get();

    assertEquals(response.getStatus(), 401);
  }


  @Test
  void testUnidentifiedAuthentication() {
    Response response = resources.getJerseyTest()
        .target("/v1/certificate/delivery")
        .request()
        .header(HeaderUtils.UNIDENTIFIED_ACCESS_KEY, AuthHelper.getUnidentifiedAccessHeader("1234".getBytes()))
        .get();

    assertEquals(response.getStatus(), 401);
  }

  @Test
  void testGetSingleGroupCredentialWithPniAsServiceId() {
    final Instant startOfDay = clock.instant().truncatedTo(ChronoUnit.DAYS);

    final GroupCredentials credentials = resources.getJerseyTest()
        .target("/v1/certificate/auth/group")
        .queryParam("redemptionStartSeconds", startOfDay.getEpochSecond())
        .queryParam("redemptionEndSeconds", startOfDay.getEpochSecond())
        .queryParam("pniAsServiceId", true)
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(GroupCredentials.class);

    assertEquals(1, credentials.credentials().size());
    assertEquals(1, credentials.callLinkAuthCredentials().size());

    assertEquals(AuthHelper.VALID_PNI, credentials.pni());
    assertEquals(startOfDay.getEpochSecond(), credentials.credentials().get(0).redemptionTime());
    assertEquals(startOfDay.getEpochSecond(), credentials.callLinkAuthCredentials().get(0).redemptionTime());

    final ClientZkAuthOperations clientZkAuthOperations =
        new ClientZkAuthOperations(serverSecretParams.getPublicParams());

    assertDoesNotThrow(() -> {
      clientZkAuthOperations.receiveAuthCredentialWithPniAsServiceId(
          new ServiceId.Aci(AuthHelper.VALID_UUID),
          new ServiceId.Pni(AuthHelper.VALID_PNI),
          (int) startOfDay.getEpochSecond(),
          new AuthCredentialWithPniResponse(credentials.credentials().get(0).credential()));
    });

    assertDoesNotThrow(() -> {
      new CallLinkAuthCredentialResponse(credentials.callLinkAuthCredentials().get(0).credential())
          .receive(new ServiceId.Aci(AuthHelper.VALID_UUID), startOfDay, genericServerSecretParams.getPublicParams());
    });
  }

  @Test
  void testGetSingleGroupCredentialZkc() {
    final Instant startOfDay = clock.instant().truncatedTo(ChronoUnit.DAYS);

    final GroupCredentials credentials = resources.getJerseyTest()
        .target("/v1/certificate/auth/group")
        .queryParam("redemptionStartSeconds", startOfDay.getEpochSecond())
        .queryParam("redemptionEndSeconds", startOfDay.getEpochSecond())
        .queryParam("zkcCredential", true)
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(GroupCredentials.class);

    assertEquals(1, credentials.credentials().size());
    assertEquals(1, credentials.callLinkAuthCredentials().size());

    assertEquals(AuthHelper.VALID_PNI, credentials.pni());
    assertEquals(startOfDay.getEpochSecond(), credentials.credentials().get(0).redemptionTime());
    assertEquals(startOfDay.getEpochSecond(), credentials.callLinkAuthCredentials().get(0).redemptionTime());

    final ClientZkAuthOperations clientZkAuthOperations =
        new ClientZkAuthOperations(serverSecretParams.getPublicParams());

    assertDoesNotThrow(() -> {
      clientZkAuthOperations.receiveAuthCredentialWithPniAsServiceId(
          new ServiceId.Aci(AuthHelper.VALID_UUID),
          new ServiceId.Pni(AuthHelper.VALID_PNI),
          (int) startOfDay.getEpochSecond(),
          new AuthCredentialWithPniResponse(credentials.credentials().get(0).credential()));
    });

    assertDoesNotThrow(() -> {
      new CallLinkAuthCredentialResponse(credentials.callLinkAuthCredentials().get(0).credential())
          .receive(new ServiceId.Aci(AuthHelper.VALID_UUID), startOfDay, genericServerSecretParams.getPublicParams());
    });
  }

  @Test
  void testGetWeekLongGroupCredentials() {
    final Instant startOfDay = clock.instant().truncatedTo(ChronoUnit.DAYS);

    final GroupCredentials credentials = resources.getJerseyTest()
        .target("/v1/certificate/auth/group")
        .queryParam("redemptionStartSeconds", startOfDay.getEpochSecond())
        .queryParam("redemptionEndSeconds", startOfDay.plus(Duration.ofDays(7)).getEpochSecond())
        .queryParam("pniAsServiceId", true)
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(GroupCredentials.class);

    assertEquals(AuthHelper.VALID_PNI, credentials.pni());
    assertEquals(8, credentials.credentials().size());
    assertEquals(8, credentials.callLinkAuthCredentials().size());

    final ClientZkAuthOperations clientZkAuthOperations =
        new ClientZkAuthOperations(serverSecretParams.getPublicParams());

    for (int i = 0; i < 8; i++) {
      final Instant redemptionTime = startOfDay.plus(Duration.ofDays(i));
      assertEquals(redemptionTime.getEpochSecond(), credentials.credentials().get(i).redemptionTime());
      assertEquals(redemptionTime.getEpochSecond(), credentials.callLinkAuthCredentials().get(i).redemptionTime());

      final int index = i;

      assertDoesNotThrow(() -> {
        clientZkAuthOperations.receiveAuthCredentialWithPniAsServiceId(
            new ServiceId.Aci(AuthHelper.VALID_UUID),
            new ServiceId.Pni(AuthHelper.VALID_PNI),
            redemptionTime.getEpochSecond(),
            new AuthCredentialWithPniResponse(credentials.credentials().get(index).credential()));
      });

      assertDoesNotThrow(() -> {
        new CallLinkAuthCredentialResponse(credentials.callLinkAuthCredentials().get(index).credential())
            .receive(new ServiceId.Aci(AuthHelper.VALID_UUID), redemptionTime, genericServerSecretParams.getPublicParams());
      });
    }
  }

  @ParameterizedTest
  @MethodSource
  void testBadRedemptionTimes(final Instant redemptionStart, final Instant redemptionEnd) {
    final Response response = resources.getJerseyTest()
        .target("/v1/certificate/auth/group")
        .queryParam("redemptionStartSeconds", redemptionStart.getEpochSecond())
        .queryParam("redemptionEndSeconds", redemptionEnd.getEpochSecond())
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get();

    assertEquals(400, response.getStatus());
  }

  private static Stream<Arguments> testBadRedemptionTimes() {
    return Stream.of(
        // Start is after end
        Arguments.of(clock.instant().plus(Duration.ofDays(1)), clock.instant()),

        // Start is in the past
        Arguments.of(clock.instant().minus(Duration.ofDays(1)), clock.instant()),

        // End is too far in the future
        Arguments.of(clock.instant(),
            clock.instant().plus(CertificateController.MAX_REDEMPTION_DURATION).plus(Duration.ofDays(1))),

        // Start is not at a day boundary
        Arguments.of(clock.instant().plusSeconds(17), clock.instant().plus(Duration.ofDays(1))),

        // End is not at a day boundary
        Arguments.of(clock.instant(), clock.instant().plusSeconds(17))
    );
  }
}
