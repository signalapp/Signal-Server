/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;


import static org.assertj.core.api.Assertions.assertThat;
import static org.whispersystems.textsecuregcm.util.MockUtils.randomSecretBytes;

import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.configuration.SecureValueRecoveryConfiguration;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.MutableClock;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import java.time.Instant;
import java.util.HexFormat;

@ExtendWith(DropwizardExtensionsSupport.class)
public class SecureValueRecoveryBControllerTest {

  private static final SecureValueRecoveryConfiguration CFG = new SecureValueRecoveryConfiguration(
      "",
      randomSecretBytes(32),
      randomSecretBytes(32),
      null,
      null,
      null
  );

  private static final MutableClock CLOCK = new MutableClock();

  private static final ExternalServiceCredentialsGenerator CREDENTIAL_GENERATOR =
      SecureValueRecoveryBController.credentialsGenerator(CFG, CLOCK);

  private static final SecureValueRecoveryBController CONTROLLER =
      new SecureValueRecoveryBController(CREDENTIAL_GENERATOR);

  private static final ResourceExtension RESOURCES = ResourceExtension.builder()
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(new AuthValueFactoryProvider.Binder<>(AuthenticatedDevice.class))
      .setMapper(SystemMapper.jsonMapper())
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(CONTROLLER)
      .build();

  @Test
  public void testGetCredentials() {
    CLOCK.setTimeInstant(Instant.ofEpochSecond(123));
    final ExternalServiceCredentials creds = RESOURCES.getJerseyTest()
        .target("/v1/svrb/auth")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(ExternalServiceCredentials.class);

    assertThat(HexFormat.of().parseHex(creds.username())).hasSize(16);
    System.out.println(creds.password());
    final String[] split = creds.password().split(":", 2);
    assertThat(Long.parseLong(split[0])).isEqualTo(123);
  }
}
