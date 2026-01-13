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
import java.util.Collection;
import java.util.List;
import java.util.Optional;
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
import org.whispersystems.textsecuregcm.util.UUIDUtil;

@ExtendWith(DropwizardExtensionsSupport.class)
class CertificateControllerTest {

  private static final ECPublicKey CA_PUBLIC_KEY;
  private static final byte[] SIGNING_CERTIFICATE_DATA;
  private static final CertificateGenerator CERTIFICATE_GENERATOR;
  private static final ServerCertificate.Certificate SIGNING_CERTIFICATE;

  private static final ServerSecretParams SERVER_SECRET_PARAMS = ServerSecretParams.generate();

  private static final GenericServerSecretParams genericServerSecretParams = GenericServerSecretParams.generate();

  private static final ServerZkAuthOperations SERVER_ZK_AUTH_OPERATIONS;
  private static final Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());

  private static final AccountsManager ACCOUNTS_MANAGER = mock(AccountsManager.class);

  static {
    try {
      CA_PUBLIC_KEY = new ECPrivateKey(Base64.getDecoder().decode("EO3Mnf0kfVlVnwSaqPoQnAxhnnGL1JTdXqktCKEe9Eo="))
          .publicKey();
      SIGNING_CERTIFICATE_DATA = Base64.getDecoder().decode(
          "CiUIDBIhBbTz4h1My+tt+vw+TVscgUe/DeHS0W02tPWAWbTO2xc3EkD+go4bJnU0AcnFfbOLKoiBfCzouZtDYMOVi69rE7r4U9cXREEqOkUmU2WJBjykAxWPCcSTmVTYHDw7hkSp/puG");
      final ECPrivateKey signingKey = new ECPrivateKey(Base64.getDecoder().decode("ABOxG29xrfq4E7IrW11Eg7+HBbtba9iiS0500YoBjn4="));

      CERTIFICATE_GENERATOR = new CertificateGenerator(SIGNING_CERTIFICATE_DATA, signingKey, 1, false);
      SIGNING_CERTIFICATE = ServerCertificate.Certificate.parseFrom(
          ServerCertificate.parseFrom(SIGNING_CERTIFICATE_DATA).getCertificate());
      SERVER_ZK_AUTH_OPERATIONS = new ServerZkAuthOperations(SERVER_SECRET_PARAMS);
    } catch (IOException | InvalidKeyException e) {
      throw new AssertionError(e);
    }
  }

  private static final ResourceExtension resources = ResourceExtension.builder()
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(new AuthValueFactoryProvider.Binder<>(AuthenticatedDevice.class))
      .setMapper(SystemMapper.jsonMapper())
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(new CertificateController(ACCOUNTS_MANAGER, CERTIFICATE_GENERATOR, SERVER_ZK_AUTH_OPERATIONS, genericServerSecretParams, clock))
      .build();

  @BeforeEach
  void setUp() {
    when(ACCOUNTS_MANAGER.getByAccountIdentifier(AuthHelper.VALID_UUID)).thenReturn(Optional.of(AuthHelper.VALID_ACCOUNT));
  }

  @Test
  void testSigningCertificate() throws Exception {
    final ServerCertificate fullCertificate = ServerCertificate.parseFrom(SIGNING_CERTIFICATE_DATA);
    assertTrue(CA_PUBLIC_KEY.verifySignature(fullCertificate.getCertificate().toByteArray(),
        fullCertificate.getSignature().toByteArray()));
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

    assertEquals(SIGNING_CERTIFICATE.getId(), certificate.getSignerId());
    ECPublicKey serverPublicKey = new ECPublicKey(SIGNING_CERTIFICATE.getKey().toByteArray());

    assertTrue(serverPublicKey.verifySignature(
        certificateHolder.getCertificate().toByteArray(), certificateHolder.getSignature().toByteArray()));

    assertEquals(AuthHelper.VALID_NUMBER, certificate.getSenderE164());
    assertEquals(1L, certificate.getSenderDevice());
    assertTrue(certificate.hasSenderUuid());
    assertEquals(UUIDUtil.toByteString(AuthHelper.VALID_UUID), certificate.getSenderUuid());
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

    assertEquals(SIGNING_CERTIFICATE.getId(), certificate.getSignerId());
    ECPublicKey serverPublicKey = new ECPublicKey(SIGNING_CERTIFICATE.getKey().toByteArray());

    assertTrue(serverPublicKey.verifySignature(certificateHolder.getCertificate().toByteArray(),
        certificateHolder.getSignature().toByteArray()));

    assertEquals(AuthHelper.VALID_NUMBER, certificate.getSenderE164());
    assertEquals(1L, certificate.getSenderDevice());
    assertEquals(certificate.getSenderUuid(), UUIDUtil.toByteString(AuthHelper.VALID_UUID));
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

    assertEquals(SIGNING_CERTIFICATE.getId(), certificate.getSignerId());
    ECPublicKey serverPublicKey = new ECPublicKey(SIGNING_CERTIFICATE.getKey().toByteArray());

    assertTrue(serverPublicKey.verifySignature(certificateHolder.getCertificate().toByteArray(),
        certificateHolder.getSignature().toByteArray()));

    assertTrue(StringUtils.isBlank(certificate.getSenderE164()));
    assertEquals(1L, certificate.getSenderDevice());
    assertEquals(certificate.getSenderUuid(), UUIDUtil.toByteString(AuthHelper.VALID_UUID));
    assertArrayEquals(certificate.getIdentityKey().toByteArray(), AuthHelper.VALID_IDENTITY.serialize());
  }

  @Test
  void testBadAuthentication() {
    Response response = resources.getJerseyTest()
        .target("/v1/certificate/delivery")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.INVALID_PASSWORD))
        .get();

    assertEquals(401, response.getStatus());
  }


  @Test
  void testNoAuthentication() {
    Response response = resources.getJerseyTest()
        .target("/v1/certificate/delivery")
        .request()
        .get();

    assertEquals(401, response.getStatus());
  }


  @Test
  void testUnidentifiedAuthentication() {
    Response response = resources.getJerseyTest()
        .target("/v1/certificate/delivery")
        .request()
        .header(HeaderUtils.UNIDENTIFIED_ACCESS_KEY, AuthHelper.getUnidentifiedAccessHeader("1234".getBytes()))
        .get();

    assertEquals(401, response.getStatus());
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
    assertEquals(startOfDay.getEpochSecond(), credentials.credentials().getFirst().redemptionTime());
    assertEquals(startOfDay.getEpochSecond(), credentials.callLinkAuthCredentials().getFirst().redemptionTime());

    final ClientZkAuthOperations clientZkAuthOperations =
        new ClientZkAuthOperations(SERVER_SECRET_PARAMS.getPublicParams());

    assertDoesNotThrow(() -> {
      clientZkAuthOperations.receiveAuthCredentialWithPniAsServiceId(
          new ServiceId.Aci(AuthHelper.VALID_UUID),
          new ServiceId.Pni(AuthHelper.VALID_PNI),
          (int) startOfDay.getEpochSecond(),
          new AuthCredentialWithPniResponse(credentials.credentials().getFirst().credential()));
    });

    assertDoesNotThrow(() -> {
      new CallLinkAuthCredentialResponse(credentials.callLinkAuthCredentials().getFirst().credential())
          .receive(new ServiceId.Aci(AuthHelper.VALID_UUID), startOfDay, genericServerSecretParams.getPublicParams());
    });
  }

  @Test
  void testGetSingleGroupCredential() {
    final Instant startOfDay = clock.instant().truncatedTo(ChronoUnit.DAYS);

    final GroupCredentials credentials = resources.getJerseyTest()
        .target("/v1/certificate/auth/group")
        .queryParam("redemptionStartSeconds", startOfDay.getEpochSecond())
        .queryParam("redemptionEndSeconds", startOfDay.getEpochSecond())
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(GroupCredentials.class);

    assertEquals(1, credentials.credentials().size());
    assertEquals(1, credentials.callLinkAuthCredentials().size());

    assertEquals(AuthHelper.VALID_PNI, credentials.pni());
    assertEquals(startOfDay.getEpochSecond(), credentials.credentials().getFirst().redemptionTime());
    assertEquals(startOfDay.getEpochSecond(), credentials.callLinkAuthCredentials().getFirst().redemptionTime());

    final ClientZkAuthOperations clientZkAuthOperations =
        new ClientZkAuthOperations(SERVER_SECRET_PARAMS.getPublicParams());

    assertDoesNotThrow(() -> {
      clientZkAuthOperations.receiveAuthCredentialWithPniAsServiceId(
          new ServiceId.Aci(AuthHelper.VALID_UUID),
          new ServiceId.Pni(AuthHelper.VALID_PNI),
          (int) startOfDay.getEpochSecond(),
          new AuthCredentialWithPniResponse(credentials.credentials().getFirst().credential()));
    });

    assertDoesNotThrow(() -> {
      new CallLinkAuthCredentialResponse(credentials.callLinkAuthCredentials().getFirst().credential())
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
        new ClientZkAuthOperations(SERVER_SECRET_PARAMS.getPublicParams());

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

  private static Collection<Arguments> testBadRedemptionTimes() {
    return List.of(
        Arguments.argumentSet("Start is after end", clock.instant().plus(Duration.ofDays(1)), clock.instant()),
        Arguments.argumentSet("Start is in the past", clock.instant().minus(Duration.ofDays(1)), clock.instant()),
        Arguments.argumentSet("End is too far in the future", clock.instant(),
            clock.instant().plus(CertificateController.MAX_REDEMPTION_DURATION).plus(Duration.ofDays(1))),
        Arguments.argumentSet("Start is not at a day boundary", clock.instant().plusSeconds(17),
            clock.instant().plus(Duration.ofDays(1))),
        Arguments.argumentSet("End is not at a day boundary", clock.instant(), clock.instant().plusSeconds(17))
    );
  }
}
