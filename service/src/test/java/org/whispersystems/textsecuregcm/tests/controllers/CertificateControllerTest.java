/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.controllers;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableSet;
import io.dropwizard.auth.PolymorphicAuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.stream.Stream;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.signal.libsignal.protocol.InvalidKeyException;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.zkgroup.ServerSecretParams;
import org.signal.libsignal.zkgroup.auth.AuthCredentialWithPniResponse;
import org.signal.libsignal.zkgroup.auth.ClientZkAuthOperations;
import org.signal.libsignal.zkgroup.auth.ServerZkAuthOperations;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.CertificateGenerator;
import org.whispersystems.textsecuregcm.auth.DisabledPermittedAuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.OptionalAccess;
import org.whispersystems.textsecuregcm.controllers.CertificateController;
import org.whispersystems.textsecuregcm.entities.DeliveryCertificate;
import org.whispersystems.textsecuregcm.entities.GroupCredentials;
import org.whispersystems.textsecuregcm.entities.MessageProtos.SenderCertificate;
import org.whispersystems.textsecuregcm.entities.MessageProtos.ServerCertificate;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.SystemMapper;

@ExtendWith(DropwizardExtensionsSupport.class)
class CertificateControllerTest {

  private static final String caPublicKey = "BWh+UOhT1hD8bkb+MFRvb6tVqhoG8YYGCzOd7mgjo8cV";

  @SuppressWarnings("unused")
  private static final String caPrivateKey = "EO3Mnf0kfVlVnwSaqPoQnAxhnnGL1JTdXqktCKEe9Eo=";

  private static final String signingCertificate = "CiUIDBIhBbTz4h1My+tt+vw+TVscgUe/DeHS0W02tPWAWbTO2xc3EkD+go4bJnU0AcnFfbOLKoiBfCzouZtDYMOVi69rE7r4U9cXREEqOkUmU2WJBjykAxWPCcSTmVTYHDw7hkSp/puG";
  private static final String signingKey = "ABOxG29xrfq4E7IrW11Eg7+HBbtba9iiS0500YoBjn4=";

  private static final ServerSecretParams serverSecretParams = ServerSecretParams.generate();
  private static final CertificateGenerator certificateGenerator;
  private static final ServerZkAuthOperations serverZkAuthOperations;
  private static final Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());

  static {
    try {
      certificateGenerator = new CertificateGenerator(Base64.getDecoder().decode(signingCertificate),
          Curve.decodePrivatePoint(Base64.getDecoder().decode(signingKey)), 1);
      serverZkAuthOperations = new ServerZkAuthOperations(serverSecretParams);
    } catch (IOException | InvalidKeyException e) {
      throw new AssertionError(e);
    }
  }


  private static final ResourceExtension resources = ResourceExtension.builder()
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(new PolymorphicAuthValueFactoryProvider.Binder<>(
          ImmutableSet.of(AuthenticatedAccount.class, DisabledPermittedAuthenticatedAccount.class)))
      .setMapper(SystemMapper.jsonMapper())
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(new CertificateController(certificateGenerator, serverZkAuthOperations, clock))
      .build();

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

    assertTrue(Curve.verifySignature(Curve.decodePoint(serverCertificate.getKey().toByteArray(), 0),
        certificateHolder.getCertificate().toByteArray(), certificateHolder.getSignature().toByteArray()));
    assertTrue(Curve.verifySignature(Curve.decodePoint(Base64.getDecoder().decode(caPublicKey), 0),
        serverCertificateHolder.getCertificate().toByteArray(), serverCertificateHolder.getSignature().toByteArray()));

    assertEquals(certificate.getSender(), AuthHelper.VALID_NUMBER);
    assertEquals(certificate.getSenderDevice(), 1L);
    assertTrue(certificate.hasSenderUuid());
    assertEquals(AuthHelper.VALID_UUID.toString(), certificate.getSenderUuid());
    assertArrayEquals(certificate.getIdentityKey().toByteArray(),
        Base64.getDecoder().decode(AuthHelper.VALID_IDENTITY));
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

    assertTrue(Curve.verifySignature(Curve.decodePoint(serverCertificate.getKey().toByteArray(), 0),
        certificateHolder.getCertificate().toByteArray(), certificateHolder.getSignature().toByteArray()));
    assertTrue(Curve.verifySignature(Curve.decodePoint(Base64.getDecoder().decode(caPublicKey), 0),
        serverCertificateHolder.getCertificate().toByteArray(), serverCertificateHolder.getSignature().toByteArray()));

    assertEquals(certificate.getSender(), AuthHelper.VALID_NUMBER);
    assertEquals(certificate.getSenderDevice(), 1L);
    assertEquals(certificate.getSenderUuid(), AuthHelper.VALID_UUID.toString());
    assertArrayEquals(certificate.getIdentityKey().toByteArray(),
        Base64.getDecoder().decode(AuthHelper.VALID_IDENTITY));
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

    assertTrue(Curve.verifySignature(Curve.decodePoint(serverCertificate.getKey().toByteArray(), 0),
        certificateHolder.getCertificate().toByteArray(), certificateHolder.getSignature().toByteArray()));
    assertTrue(Curve.verifySignature(Curve.decodePoint(Base64.getDecoder().decode(caPublicKey), 0),
        serverCertificateHolder.getCertificate().toByteArray(), serverCertificateHolder.getSignature().toByteArray()));

    assertTrue(StringUtils.isBlank(certificate.getSender()));
    assertEquals(certificate.getSenderDevice(), 1L);
    assertEquals(certificate.getSenderUuid(), AuthHelper.VALID_UUID.toString());
    assertArrayEquals(certificate.getIdentityKey().toByteArray(),
        Base64.getDecoder().decode(AuthHelper.VALID_IDENTITY));
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
        .header(OptionalAccess.UNIDENTIFIED, AuthHelper.getUnidentifiedAccessHeader("1234".getBytes()))
        .get();

    assertEquals(response.getStatus(), 401);
  }

  @Test
  void testDisabledAuthentication() {
    Response response = resources.getJerseyTest()
        .target("/v1/certificate/delivery")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.DISABLED_UUID, AuthHelper.DISABLED_PASSWORD))
        .get();

    assertEquals(response.getStatus(), 401);
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
    assertEquals(AuthHelper.VALID_PNI, credentials.pni());
    assertEquals(startOfDay.getEpochSecond(), credentials.credentials().get(0).redemptionTime());

    final ClientZkAuthOperations clientZkAuthOperations =
        new ClientZkAuthOperations(serverSecretParams.getPublicParams());

    assertDoesNotThrow(() -> {
      clientZkAuthOperations.receiveAuthCredentialWithPni(
          AuthHelper.VALID_UUID,
          AuthHelper.VALID_PNI,
          (int) startOfDay.getEpochSecond(),
          new AuthCredentialWithPniResponse(credentials.credentials().get(0).credential()));
    });
  }

  @Test
  void testGetWeekLongGroupCredentials() {
    final Instant startOfDay = clock.instant().truncatedTo(ChronoUnit.DAYS);

    final GroupCredentials credentials = resources.getJerseyTest()
        .target("/v1/certificate/auth/group")
        .queryParam("redemptionStartSeconds", startOfDay.getEpochSecond())
        .queryParam("redemptionEndSeconds", startOfDay.plus(Duration.ofDays(7)).getEpochSecond())
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(GroupCredentials.class);

    assertEquals(AuthHelper.VALID_PNI, credentials.pni());
    assertEquals(8, credentials.credentials().size());

    final ClientZkAuthOperations clientZkAuthOperations =
        new ClientZkAuthOperations(serverSecretParams.getPublicParams());

    for (int i = 0; i < 8; i++) {
      final Instant redemptionTime = startOfDay.plus(Duration.ofDays(i));
      assertEquals(redemptionTime.getEpochSecond(), credentials.credentials().get(i).redemptionTime());

      final int index = i;

      assertDoesNotThrow(() -> {
        clientZkAuthOperations.receiveAuthCredentialWithPni(
            AuthHelper.VALID_UUID,
            AuthHelper.VALID_PNI,
            redemptionTime.getEpochSecond(),
            new AuthCredentialWithPniResponse(credentials.credentials().get(index).credential()));
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
