/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableSet;
import io.dropwizard.auth.PolymorphicAuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.stream.Stream;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.signal.libsignal.zkgroup.ServerSecretParams;
import org.signal.libsignal.zkgroup.auth.AuthCredentialWithPniResponse;
import org.signal.libsignal.zkgroup.auth.ClientZkAuthOperations;
import org.signal.libsignal.zkgroup.auth.ServerZkAuthOperations;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.DisabledPermittedAuthenticatedAccount;
import org.whispersystems.textsecuregcm.entities.GroupCredentials;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import javax.ws.rs.core.Response;

@ExtendWith(DropwizardExtensionsSupport.class)
class GroupControllerTest {

  private static final ServerSecretParams SERVER_SECRET_PARAMS = ServerSecretParams.generate();
  private static final ServerZkAuthOperations SERVER_ZK_AUTH_OPERATIONS = new ServerZkAuthOperations(SERVER_SECRET_PARAMS);

  private static final Clock CLOCK = Clock.fixed(Instant.now(), ZoneId.systemDefault());

  private static final ResourceExtension RESOURCE_EXTENSION = ResourceExtension.builder()
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(new PolymorphicAuthValueFactoryProvider.Binder<>(
          ImmutableSet.of(AuthenticatedAccount.class, DisabledPermittedAuthenticatedAccount.class)))
      .setMapper(SystemMapper.getMapper())
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(new GroupController(SERVER_ZK_AUTH_OPERATIONS, CLOCK))
      .build();

  @Test
  void testGetSingleGroupCredential() {
    final Instant startOfDay = CLOCK.instant().truncatedTo(ChronoUnit.DAYS);

    final GroupCredentials credentials = RESOURCE_EXTENSION.getJerseyTest()
        .target("/v1/group/auth")
        .queryParam("redemptionStartSeconds", startOfDay.getEpochSecond())
        .queryParam("redemptionEndSeconds", startOfDay.getEpochSecond())
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(GroupCredentials.class);

    assertEquals(1, credentials.getCredentials().size());
    assertEquals(startOfDay.getEpochSecond(), credentials.getCredentials().get(0).getRedemptionTime());

    final ClientZkAuthOperations clientZkAuthOperations =
        new ClientZkAuthOperations(SERVER_SECRET_PARAMS.getPublicParams());

    assertDoesNotThrow(() -> {
      clientZkAuthOperations.receiveAuthCredentialWithPni(AuthHelper.VALID_UUID,
          AuthHelper.VALID_PNI,
          (int) startOfDay.getEpochSecond(),
          new AuthCredentialWithPniResponse(credentials.getCredentials().get(0).getCredential()));
    });
  }

  @Test
  void testGetWeekLongGroupCredentials() {
    final Instant startOfDay = CLOCK.instant().truncatedTo(ChronoUnit.DAYS);

    final GroupCredentials credentials = RESOURCE_EXTENSION.getJerseyTest()
        .target("/v1/group/auth")
        .queryParam("redemptionStartSeconds", startOfDay.getEpochSecond())
        .queryParam("redemptionEndSeconds", startOfDay.plus(Duration.ofDays(7)).getEpochSecond())
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(GroupCredentials.class);

    assertEquals(8, credentials.getCredentials().size());

    final ClientZkAuthOperations clientZkAuthOperations =
        new ClientZkAuthOperations(SERVER_SECRET_PARAMS.getPublicParams());

    for (int i = 0; i < 8; i++) {
      final Instant redemptionTime = startOfDay.plus(Duration.ofDays(i));
      assertEquals(redemptionTime.getEpochSecond(), credentials.getCredentials().get(i).getRedemptionTime());

      final int index = i;

      assertDoesNotThrow(() -> {
        clientZkAuthOperations.receiveAuthCredentialWithPni(AuthHelper.VALID_UUID,
            AuthHelper.VALID_PNI,
            redemptionTime.getEpochSecond(),
            new AuthCredentialWithPniResponse(credentials.getCredentials().get(index).getCredential()));
      });
    }
  }

  @ParameterizedTest
  @MethodSource
  void testBadRedemptionTimes(final Instant redemptionStart, final Instant redemptionEnd) {
    final Response response = RESOURCE_EXTENSION.getJerseyTest()
        .target("/v1/group/auth")
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
        Arguments.of(CLOCK.instant().plus(Duration.ofDays(1)), CLOCK.instant()),

        // Start is in the past
        Arguments.of(CLOCK.instant().minus(Duration.ofDays(1)), CLOCK.instant()),

        // End is too far in the future
        Arguments.of(CLOCK.instant(), CLOCK.instant().plus(GroupController.MAX_REDEMPTION_DURATION).plus(Duration.ofDays(1))),

        // Start is not at a day boundary
        Arguments.of(CLOCK.instant().plusSeconds(17), CLOCK.instant().plus(Duration.ofDays(1))),

        // End is not at a day boundary
        Arguments.of(CLOCK.instant(), CLOCK.instant().plusSeconds(17))
    );
  }
}
