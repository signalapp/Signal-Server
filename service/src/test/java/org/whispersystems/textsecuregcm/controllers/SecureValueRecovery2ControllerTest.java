/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;


import static org.mockito.Mockito.mock;
import static org.whispersystems.textsecuregcm.util.MockUtils.randomSecretBytes;

import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import jakarta.ws.rs.core.Response;
import java.util.Map;
import java.util.stream.Collectors;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.extension.ExtendWith;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.configuration.SecureValueRecovery2Configuration;
import org.whispersystems.textsecuregcm.entities.AuthCheckResponseV2;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.MutableClock;
import org.whispersystems.textsecuregcm.util.SystemMapper;

@ExtendWith(DropwizardExtensionsSupport.class)
public class SecureValueRecovery2ControllerTest extends SecureValueRecoveryControllerBaseTest {

  private static final SecureValueRecovery2Configuration CFG = new SecureValueRecovery2Configuration(
      "",
      randomSecretBytes(32),
      randomSecretBytes(32),
      null,
      null,
      null
  );

  private static final MutableClock CLOCK = new MutableClock();

  private static final ExternalServiceCredentialsGenerator CREDENTIAL_GENERATOR =
      SecureValueRecovery2Controller.credentialsGenerator(CFG, CLOCK);

  private static final AccountsManager ACCOUNTS_MANAGER = mock(AccountsManager.class);
  private static final SecureValueRecovery2Controller CONTROLLER =
      new SecureValueRecovery2Controller(CREDENTIAL_GENERATOR, ACCOUNTS_MANAGER);

  private static final ResourceExtension RESOURCES = ResourceExtension.builder()
      .addProvider(AuthHelper.getAuthFilter())
      .setMapper(SystemMapper.jsonMapper())
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(CONTROLLER)
      .build();

  protected SecureValueRecovery2ControllerTest() {
    super("/v2", ACCOUNTS_MANAGER, CLOCK, RESOURCES, CREDENTIAL_GENERATOR);
  }

  @Override
  Map<String, CheckStatus> parseCheckResponse(final Response response) {
    final AuthCheckResponseV2 authCheckResponseV2 = response.readEntity(AuthCheckResponseV2.class);
    return authCheckResponseV2.matches().entrySet().stream().collect(Collectors.toMap(
        Map.Entry::getKey, e -> switch (e.getValue()) {
          case MATCH -> CheckStatus.MATCH;
          case INVALID -> CheckStatus.INVALID;
          case NO_MATCH -> CheckStatus.NO_MATCH;
        }
    ));
  }
}
